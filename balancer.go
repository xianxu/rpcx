package rpcx

import (
	"github.com/xianxu/gostrich"

	"fmt"
	"io"
	"math"
	"math/rand"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

/*
   Generic load balancer logic in a distributed system. It provides:
     - Load balancing among multiple hosts/connections evenly (number of qps).
     - Aware of difference in machine power. Query fast machines more.
     - Probing when a service's dead (Supervisor).
     - Recreate service when dead for a while (Supervisor)

   TODO:
     - Service discovery (ServerSet)
     - Dynamic adjust of number of connections (do we need?)
 */

var (
	TimeoutErr              = Error("request timeout")
	CancelledErr            = Error("request cancelled")
	NilUnderlyingServiceErr = Error("underlying service is nil or empty")

	// Setting ProberReq to this value, the prober will use last request that triggers a service
	// being marked as dead as the prober req. This works fine for idempotent requests. Otherwise
	// doesn't work and prober req would be in other form, e.g. a different request hitting same
	// service.
	ProberReqLastFail ProberReqLastFailType

	logger                  = gostrich.NamedLogger { "[Rpcx]" }
)

const (
	proberFreqSec    int32 = 5
	replacerFreqSec  int32 = 30
	maxSelectorRetry int   = 20

	// Note, the following can be tweaked for fault tolerant behavior. They can be made as per
	// service configuration. However, I feel a good choice of values can provide sufficient values,
	// and freeing clients to figure out good values of those arcane numbers.
	deadThreshold  float64 = 4   // factor over normal to be considered dead
	errorFactor    float64 = 30  // treat errors as X times of normal latency
	latencyBuffer  float64 = 1.5 // factor of how bad latency compared to context before react

	// max error latency
	maxErrorMicros int64   = 60000000

	// size of history to keep
	supervisorHistorySize int = 30
	// how long latency history do we care, 5 second seems reasonable
	reactionPeriod int = 5
	// how many items needs to be collected before we consider marking service dead
	supervisorHistoryPadding int64 = 5
)

type Error string

func (t Error) Error() string {
	return string(t)
}

/*
An abstract service that transforms req to rsp. Typically a service is a rpc, but not
necessarily so. Response object's created by caller, the Service usually inspect its type to
determine how to fill it when rpc completes.

The cancel argument's used for caller to signal cancellation. This is mostly for advisory purpose,
implementation may or may not honor it.

Service also implement the io.Closer interface for simple life cycle management.
 */
type Service interface {
	// release any resources associated with this service.
	io.Closer
	// handle req, populate rsp, return err and optionally honor cancellation. cancel can be nil.
	// req and rsp can't be nil.
	Serve(req interface{}, rsp interface{}, cancel *bool) error
}

/*
 * Maker of a service, this is used to recreate a service in case of persisted errors. Maker would
 * typically wrap state such as which host to connect to etc.
 */
type ServiceMaker interface {
	Name() string
	Make() (s Service, e error)
}

// General interface used to do basic reporting of service status. the parameters in order are:
// req, rep, error and latency.
type ServiceReporter interface {
	Report(interface{}, interface{}, error, int64)
}

// whether to use last req that errors out as probing req.
type ProberReqLastFailType int

// Wrapping on top of a Service, keeping track service history for load balancing purpose.
// There are two strategy dealing with faulty services, either we can keep probing it, or
// we can create a new service to replace the faulty one. Supervisor's intended to work in
// a Cluster, rather than be used alone. TODO: hide this?
type Supervisor struct {
	// lock used to guard change of underlying service.
	svcLock sync.RWMutex

	// underlying service
	service Service

	// Human readable name for logging purpose
	name string

	// latency stats
	// Keeps track of host latency, in micro seconds
	latencies gostrich.IntSampler

	// Average latency, cached value from latencies sampler
	latencyAvg int64

	// A function that returns what's the average latency across some computation context, such
	// as within a cluster. This gives context of how slow/bad this Service is performance, with
	// respect to its peers. Supervisor's coded to react when self latency hitting 2x and 4x
	// of latency context.
	latencyContext func() float64

	// Note the following two fields are int32 so that we can compare/set atomically
	// Mutable field of whether there's a prober running
	proberRunning int32

	// Mutable field of whether we are replacing service
	replacerRunning int32

	// Strategy of dealing with faults, when a service is marked dead. Those two strategies can
	// be combined.
	//   - To start a prober to probe, this is needed because upstream will not send more traffic
	//     when a service's dead.
	//   - To replace faulty service with a new service with a ServiceMaker.
	//
	// To use prober, set proberReq to non-null. If it's anything that's a ProberReqLastFailType,
	// last req's used as probe req, otherwise proberReq is assumed to be the object to use. To
	// replace faulty service, supply a ServiceMaker.
	//
	// When proberReq is not nil, probing will be started. When serviceMaker is nil, underlying
	// service will not be replaced.
	proberReq interface{}

	serviceMaker ServiceMaker

	// where to report service status, gostrich thing
	reporter ServiceReporter

	// estimate of current qps
	qps *gostrich.QpsTracker
}

// A Balancer is Supervisor that tracks last 30 service call status. It recovers mostly by keep
// probing. In other cases, ServiceMaker may be invoked to recreate all underlying services.

// Note: The name + service is somewhat duplicate of serviceMaker, it's there so that serviceMaker
//       is not mandatory.
func NewSupervisor(
	name string,
	service Service,
	latencyContext func() float64,
	reporter ServiceReporter,
	proberReq interface{},
	serviceMaker ServiceMaker) *Supervisor {
	return &Supervisor{
		sync.RWMutex{},
		service,
		name,
		gostrich.NewIntSampler(supervisorHistorySize),
		0, //TODO: good default?
		latencyContext,
		0,
		0,
		proberReq,
		serviceMaker,
		reporter,
		gostrich.NewQpsTracker(time.Second),
	}
}

func MicroTilNow(then time.Time) int64 {
	return time.Now().Sub(then).Nanoseconds() / 1000
}

func (s *Supervisor) isDead() bool {
	// wait till we have enough "bad" samples before reporting dead. Also, if there's no context
	// service won't be dead thus bypassing all service recreation logic. If Supervisor's used
	// directly outside of a Cluster, supply a function that reports "normal" latency as context.
	if s.latencies.Count() <= supervisorHistoryPadding || s.latencyContext == nil {
		return false
	}
	avg := atomic.LoadInt64(&(s.latencyAvg))
	overallAvg := s.latencyContext()
	return float64(avg) >= deadThreshold*overallAvg
}

func (s *Supervisor) Close() (err error) {
	logger.LogDbg("Supervisor " + s.name + " is closed")
	s.svcLock.Lock()
	defer s.svcLock.Unlock()

	err = s.service.Close()
	s.service = nil
	return
}

/*
 * Serve request. The basic logic's to call underlying service, keep track of latency and optionally
 * trigger prober/replacer.
 */
func (s *Supervisor) Serve(req interface{}, rsp interface{}, cancel *bool) (err error) {
	then := time.Now()
	logger.LogDbg("Supervisor " + s.name + " serving request")
	s.svcLock.RLock()
	if s.service == nil {
		logger.LogInfo("There's no underlying service for " + s.name)
		err = NilUnderlyingServiceErr
	} else {
		logger.LogDbg("Supervisor " + s.name + " delegating to underlying service")
		err = s.service.Serve(req, rsp, cancel)
	}

	latency := MicroTilNow(then)
	logger.LogDbg(fmt.Sprintf("Supervisor %v finishes in %v micro sec", s.name, latency))

	// collect stats before adjusting latency
	if s.reporter != nil {
		s.reporter.Report(req, rsp, err, latency)
	}

	if err != nil {
		logger.LogDbg("Supervisor " + s.name + " received error from underlying service: " + err.Error())
		// treat errors as errorFactor times average context latency, ceiling at 60 seconds
		latency = int64(math.Min(s.latencyContext()*errorFactor, float64(maxErrorMicros)))
	}

	s.qps.Record()
	recordIt := func() {
		s.latencies.Observe(latency)
		sampled := s.latencies.Sampled()
		avg := average(sampled)
		logger.LogDbg(fmt.Sprintf("Supervisor %v is logging stats, current avg latency is %v", s.name, avg))
		// set average for faster access later on.
		atomic.StoreInt64(&(s.latencyAvg), int64(avg))
	}
	if s.latencies.Count() < int64(s.latencies.Length()) {
		logger.LogDbg(fmt.Sprintf("Supervisor %v, chance of recording latency is 100%%"))
		// fill all sample slots first, the case for startup
		recordIt()
	} else {
		// logic: we have keep 30 "supervisorHistorySize" samples and want to span that across
		// 5 sec (reactionPeriod), if we are at 6 qps, we do full sample; if we are at 60 qps,
		// we sample every 10%
		c := chance(s.qps.Ticks())
		logger.LogDbg(fmt.Sprintf("Supervisor %v, chance of recording latency is %v%%", s.name, c * 100))
		gostrich.DoWithChance(c, recordIt)
	}

	s.svcLock.RUnlock()
	// End of lock

	logger.LogDbgF(func()interface{} {
		return fmt.Sprintf("Is dead %v, context: %v, latencyAvg: %v\n",
			s.isDead(), s.latencyContext(), s.latencyAvg)
	})

	// react to faulty services.
	switch {
	case s.isDead():
		logger.LogDbg("Supervisor " + s.name + " is dead")
		// Reactions to service being dead:
		// If we have serviceMaker, try make a new service out of it.
		if s.serviceMaker != nil {
			if atomic.CompareAndSwapInt32((*int32)(&s.replacerRunning), 0, 1) {
				// setting service to nil prevents service being used till new serivce is created.
				s.svcLock.Lock()
				if err := s.Close(); err != nil {
					logger.LogInfoF(func()interface{} {
						return fmt.Sprintf("Error closing a service %v, the error is %v", s.name, err)
					})
				}
				s.service = nil
				s.svcLock.Unlock()
				go func() {
					logger.LogInfoF(func()interface{} {
						return "Service \"%v\" gone bad, start replacer routine. This will "+
							   "try replacing underlying service at fixed interval, until "+
							   "service become healthy." + s.name
					})
					for {
						newService, err := s.serviceMaker.Make()
						if err == nil {
							logger.LogInfoF(func() interface{} {
								return fmt.Sprintf("replacer obtained new service for \"%v\"", s.name)
							})
							s.svcLock.Lock()
							s.service = newService
							s.latencies.Clear()
							s.latencyAvg = 0
							s.svcLock.Unlock()
							logger.LogInfoF(func() interface{} {
								return fmt.Sprintf("replacer of \"%v\" successfully switched on a new service. Now exiting.", s.name)
							})
							atomic.StoreInt32((*int32)(&s.replacerRunning), 0)
							break
						} else {
							logger.LogInfoF(func() interface{} {
								return fmt.Sprintf("replacer errors out for \"%v\", will try later. %v", s.name, err)
							})
						}
						time.Sleep(time.Duration(int64(replacerFreqSec)) * time.Second)
					}
				}()
			}
		}
		// if we have a prober, set to probe it, since traffic to this end point will be very limited.
		if s.proberReq != nil {
			if atomic.CompareAndSwapInt32((*int32)(&s.proberRunning), 0, 1) {
				go func() {
					logger.LogInfoF(func() interface{} {
						return fmt.Sprintf("Service \"%v\" gone bad, start probing\n", s.name)
					})
					for {
						time.Sleep(time.Duration(int64(proberFreqSec)) * time.Second)
						if !s.isDead() {
							logger.LogInfoF(func() interface{} {
								return fmt.Sprintf("Service \"%v\" recovered, exit prober routine\n", s.name)
							})
							atomic.StoreInt32((*int32)(&s.proberRunning), 0)
							break
						}
						logger.LogInfoF(func() interface{} {
							return fmt.Sprintf("Service %v is dead, probing..", s.name)
						})

						switch s.proberReq.(type) {
						case ProberReqLastFailType:
							s.Serve(req, rsp, nil)
						default:
							s.Serve(s.proberReq, rsp, nil)
						}
					}
				}()
			}
		}
	}
	return
}

// Wrapper of a service to provide better behavior like timeout and retries. Timeouts are not
// retried.
type RobustService struct {
	Service Service
	Timeout time.Duration
	Retries int

	// A function to determine if we should retry. If nil, all errors are retried.
	RetryFn func(req interface{}, rsp interface{}, err error) bool
}

func (s *RobustService) Close() error {
	return s.Close()
}

func (s *RobustService) Serve(req interface{}, rsp interface{}, cancel *bool) (err error) {
	err = CancelledErr
	tries := s.Retries + 1
	// loop if 1. is first request; or 2. retry fn is not set (retry on err); or 3 retry fn set and
	// allow retry. Conditioned on tries > 0
	for ; (err == CancelledErr || s.RetryFn == nil || s.RetryFn(req, rsp, err)) &&
		tries > 0; tries += 1 {
		// check for cancellation
		if cancel != nil && *cancel {
			return
		}
		if s.Timeout > 0 {
			tick := time.After(s.Timeout)
			// need at least 1 capacity so that when a rpc call return after timeout has occurred,
			// it doesn't block the goroutine sending such notification.
			// not sure why rpc package uses capacity 10 though.
			done := make(chan error, 1)

			go func() { done <- s.Service.Serve(req, rsp, cancel) }()

			select {
			case err = <-done:
			case <-tick:
				err = TimeoutErr
			}
		} else {
			err = s.Service.Serve(req, rsp, cancel)
		}
		// check for error
		if err == nil {
			return
		}
	}
	return
}

/*
 * A cluster represents a load balanced set of services. At higher level, it can be load balanced
 * services across multiple machines. At lower level, it can also be used to manage connection
 * pool to a single host.
 *
 * TODO: don't query dead service at all?
 * TODO: the logic to grow and shrink the service pool is not implemented yet.
 */
type Cluster struct {
	Name     string
	Services []*Supervisor
	// if there's failure, retry another host, this is the number of times to retry
	Retries  int
	// stats reporter of how cluster, rolled up from each host
	Reporter ServiceReporter

	// internals, default values' fine
	lock sync.RWMutex // guard services
}

// this is only called if there's at least one downstream service register. so this must succeed
func (c *Cluster) LatencyAvg() float64 {
	c.lock.RLock()
	defer c.lock.RUnlock()

	sum := 0.0
	for _, s := range c.Services {
		sum += float64(atomic.LoadInt64(&(s.latencyAvg)))
	}
	return sum / float64(len(c.Services))
}

// this is only called if there's at least one downstream service register. so this must succeed
func (c *Cluster) pickAService() *Supervisor {
	logger.LogDbg(fmt.Sprintf("Cluster %v needs to pick a service", c.Name))
	prob := 0.0
	tries := 0
	latencyC := c.LatencyAvg()
	var s *Supervisor
	for ; rand.Float64() >= prob && tries < maxSelectorRetry; tries += 1 {
		s = c.Services[rand.Int()%len(c.Services)]
		if s.isDead() {
			// if all services are dead, we will choose the last one, this is by design
			continue
		}
		prob = math.Min(1, (latencyC*latencyBuffer)/float64(math.Max(float64(s.latencyAvg), 1)))
	}
	logger.LogDbg(fmt.Sprintf("Cluster %v picked a service after %v tries", c.Name, tries))
	return s
}

func (c *Cluster) serveOnce(req interface{}, rsp interface{}, cancel *bool) (err error) {
	logger.LogDbg(fmt.Sprintf("Cluster %v serve once", c.Name))
	c.lock.RLock()
	defer c.lock.RUnlock()

	then := time.Now()

	if len(c.Services) == 0 {
		logger.LogDbg(fmt.Sprintf("Cluster %v no services registered", c.Name))
		err = NilUnderlyingServiceErr
		return
	}

	// pick one random
	logger.LogDbg(fmt.Sprintf("Cluster %v to pick a service", c.Name))
	s := c.pickAService()

	// serve
	logger.LogDbg(fmt.Sprintf("Cluster %v to serve", c.Name))
	err = s.Serve(req, rsp, cancel)

	if c.Reporter != nil {
		latency := MicroTilNow(then)
		logger.LogDbg(fmt.Sprintf("Cluster %v served request in %v micro", c.Name, latency))
		c.Reporter.Report(req, rsp, err, latency)
	}

	return
}

// error returned would be the last
func (c *Cluster) Close() (err error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	for _, s := range c.Services {
		if e := s.Close(); e != nil {
			err = e
		}
	}
	return
}

func (c *Cluster) Serve(req interface{}, rsp interface{}, cancel *bool) (err error) {
	for i := 0; i <= c.Retries; i += 1 {
		logger.LogDbg(fmt.Sprintf("Cluster %v serves request with try %v", c.Name, i+1))
		err = c.serveOnce(req, rsp, cancel)
		if err == nil {
			return
		} else {
			logger.LogInfoF(func() interface{} {
				return fmt.Sprintf("Error serving request in cluster %v. Error is: %v\n", c.Name, err)
			})
		}
		if cancel != nil && *cancel {
			return
		}
	}
	logger.LogInfoF(func() interface{} {
		return fmt.Sprintf("Exhausted retries of serving request in cluster %v\n", c.Name)
	})
	return
}

type BasicStatsReporter struct {
	counterReq, counterSucc, counterFail, counterRspNil gostrich.Counter
	reqLatencyStat                                      gostrich.IntSampler
}

func NewBasicStatsReporter(stats gostrich.Stats) *BasicStatsReporter {
	return &BasicStatsReporter{
		counterReq:     stats.Counter("req"),
		counterSucc:    stats.Counter("req/success"),
		counterFail:    stats.Counter("req/fail"),
		reqLatencyStat: stats.Statistics("req/latency"),
		counterRspNil:  stats.Counter("rsp/nil"),
	}
}

func (r *BasicStatsReporter) Report(req interface{}, rsp interface{}, err error, micro int64) {
	r.reqLatencyStat.Observe(micro)
	r.counterReq.Incr(1)
	if err != nil {
		r.counterFail.Incr(1)
	} else {
		r.counterSucc.Incr(1)
		if rsp == nil {
			r.counterRspNil.Incr(1)
		}
	}
}

// Wrap rpc call service name and arguments in a single struct, to be used with a Service as
// request.
type RpcReq struct {
	Fn   string
	Args interface{}
}
type RpcCaller interface {
	Call(method string, request interface{}, response interface{}) error
}
type RpcGoer interface {
	Go(method string, request interface{}, response interface{}, done chan *rpc.Call) *rpc.Call
}
type RpcClient interface {
	RpcCaller
	RpcGoer
}

// Using cassandra as an example, typical setup is:
//                                     host1:conn1
//              host1       host1   /  Supervisor - KeyspaceService  --\
//            Supervisor - Cluster  -  Supervisor - KeyspaceService  ---->  Cassandra host 1
//          /                       \  Supervisor - KeyspaceService  --/
//  Name  /
// Cluster  
//         \
//          \                       /  Supervisor - KeyspaceService  --\
//            Supervisor - Cluster  -  Supervisor - KeyspaceService  ---->  Cassandra host 2
//                                  \  Supervisor - KeyspaceService  --/
//
// Create a reliable service out of a group of service makers. n services will be created by
// each ServiceMaker (think connections).
type ReliableServiceConf struct {
	Name         string
	Makers       []ServiceMaker // ClientBuilder
	Retries      int            // default to 0, retry at cluster level
	Concurrency  int            // default to 1
	Prober       interface{}    // default to nil
	Stats        gostrich.Stats // default to nil
	PerHostStats bool           // whether to report per host stats
}

func NewReliableService(conf ReliableServiceConf) Service {
	var reporter ServiceReporter

	hosts := make([]*Supervisor, len(conf.Makers))
	if conf.Stats != nil {
		reporter = NewBasicStatsReporter(conf.Stats)
	}
	top := &Cluster{
		Name:     conf.Name,
		Services: hosts,
		Retries:  conf.Retries,
		Reporter: reporter,
	}

	for i, maker := range conf.Makers {
		// per host
		var concur int
		if conf.Concurrency == 0 {
			concur = 1
		} else {
			concur = conf.Concurrency
		}
		conns := make([]*Supervisor, concur)
		hostName := maker.Name()
		host := &Cluster{Name: hostName, Services: conns}  // host is a cluster of connections
		for j := range conns {
			conn, err := maker.Make()
			if err != nil {
				logger.LogInfoF(func() interface{} {
					return fmt.Sprintf("Failed to make a service: %v %v. Error is %v", conf.Name, hostName, err)
				})
			}
			conns[j] = NewSupervisor(
				fmt.Sprintf("%v:%v:%v", conf.Name, hostName, j),
				conn,
				func() float64 {
					return host.LatencyAvg()
				},
				nil,
				nil,
				maker)
		}
		if conf.Stats != nil && conf.PerHostStats {
			reporter = NewBasicStatsReporter(conf.Stats.Scoped(hostName))
		} else {
			reporter = nil
		}
		hosts[i] = NewSupervisor(
			hostName,
			host,
			func() float64 {
				return top.LatencyAvg()
			},
			reporter,
			conf.Prober,
			nil)
	}
	return top
}

///////////////////////////////////////
// Private utilities
///////////////////////////////////////

// thread safe way to calculate average.
func average(ns []int64) float64 {
	sum := 0.0
	for i := range ns {
		sum += float64(atomic.LoadInt64(&ns[i]))
	}
	return float64(sum) / float64(len(ns))
}

func chance(n int32)float32 {
	q := float32((n / 10 + 1) * 10)           // upper floor to avoid extremes
	return float32(supervisorHistorySize) / q / float32(reactionPeriod)// the chance we should record a event
}

