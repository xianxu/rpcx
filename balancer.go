package rpcx

import (
	"github.com/xianxu/gostrich"

	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

// TODO: 
//   - Service discovery (ServerSet)
//   - Dynamic adjust of number of connections (do we need?)
//
// Generic load balancer logic in a distributed system. It provides:
//   - Load balancing among multiple hosts/connections evenly (number of qps).
//   - Aware of difference in machine power. Query fast machines more. 
//   - Probing when a service's dead (Supervisor).
//   - Recreate service when dead for a whlie (Replaceable, Supervisor)
//
// And later on for distributed tracking etc. client ids etc.
//

var (
	//TODO: better err reporting.
	TimeoutErr              = Error("rpcx.timeout")
	CancelledErr            = Error("request cancelled")
	CannotCloneErr          = Error("Clone failed due to incompatible types")
	NilUnderlyingServiceErr = Error("underlying service is nil")

	// Setting ProberReq to this value, the prober will use last request that triggers a service
	// being marked dead as the prober req. This works fine for idempotent requests. Otherwise
	// doesn't work and prober req would be in other form.
	ProberReqLastFail ProberReqLastFailType
)

const (
	proberFreqSec    int32 = 5
	replacerFreqSec  int32 = 30
	maxSelectorRetry int   = 20

	// Note, the following can be tweaked for fault tolerant behavior. They can be made as per
	// service configuration. However, I feel a good choice of values can provide sufficient values,
	// and freeing clients to figure out good values of those arcane numbers.
	flakyThreshold float64 = 2   // factor over normal to be considered flaky
	deadThreshold  float64 = 4   // factor over normal to be considered dead
	errorFactor    float64 = 30  // treat errors as X times of normal latency
	latencyBuffer  float64 = 1.5 // factor of how bad latency compared to context before react

	// treating all errors as such latency when calculating latency stats.
	maxErrorMicros int64   = 60000000

	//TODO: hmm, how much to keep track? no good value without knowing qps. seems too complex to
	//      keep track of actual qps to worth it. Let's just have a reasonable value? Say targeting
	//      10K qps and range of 10 second. Meaning 100K values. If we sample at 1 per 1K, need to
	//      keep track of 100. When traffic's low, the synptom's the load balancer will be slow
	//      to react to error conditions.
	//
	//      The problem of keeping track of last 100? A short lived network glitch would trigger
	//      state changes. Now, consider the following cases:
	//
	//        - network glitch on a host, while others are fine. This means service to that host
	//          will timeout, and thus get disabled and later replaced, which while not ideal, is
	//          fine.
	//        - network glitch affecting all hosts. Latencies on all hosts would go up, and since
	//          the latencyContext() goes up, individual hosts will not be marked dead, we should
	//          be fine.

	// size of history to keep
	supervisorHistorySize int = 100
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
	Serve(req interface{}, rsp interface{}, cancel chan int) error
}

/*
 * Maker of a service, this is used to recreate a service in case of persisted errors. Maker would
 * typically contain state such as which host to connect to etc.
 */
type ServiceMaker interface {
	Make() (name string, s Service, e error)
}

// General interface used to do basic reporting of service status. the parameters in order are:
// req, rep, error and latency
type ServiceReporter interface {
	Report(interface{}, interface{}, error, int64)
}

// whether to use last req that errors out as probing req.
type ProberReqLastFailType int

// Wrapping on top of a Service, keeping track service history for load balancing purpose.
// There are two strategy dealing with faulty services, either we can keep probing it, or
// we can create a new service to replace the faulty one.
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
}

// A Balancer is supervisor that tracks last 100 service call status. It recovers mostly by keep
// probing. In other cases, ServiceMaker may be invoked to recreate all underlying services.
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
	}
}

// A replaceable service that recovers from error by replacing underlying service with a new one
// from service maker.
func NewReplaceable(
	name string,
	service Service,
	latencyContext func() float64,
	reporter ServiceReporter,
	serviceMaker ServiceMaker) *Supervisor {
	return &Supervisor{
		sync.RWMutex{},
		service,
		name,
		gostrich.NewIntSampler(2),
		0,
		latencyContext,
		0,
		0,
		nil,
		serviceMaker,
		reporter,
	}
}

func MicroTilNow(then time.Time) int64 {
	return time.Now().Sub(then).Nanoseconds() / 1000
}

func (s *Supervisor) isDead() bool {
	if s.latencies.Count() <= supervisorHistoryPadding || s.latencyContext == nil {
		return false
	}
	avg := atomic.LoadInt64(&(s.latencyAvg))
	overallAvg := s.latencyContext()
	return float64(avg) >= deadThreshold*overallAvg
}

func (s *Supervisor) Close() error {
	s.svcLock.RLock()
	defer s.svcLock.RUnlock()

	return s.service.Close()
}

/*
 * Serve request. The basic logic's to call underlying service, keep track of latency and optionally
 * trigger prober/replacer.
 */
func (s *Supervisor) Serve(req interface{}, rsp interface{}, cancel chan int) (err error) {
	then := time.Now()
	s.svcLock.RLock()
	if s.service == nil {
		err = NilUnderlyingServiceErr
		//TODO: logging
	} else {
		err = s.service.Serve(req, rsp, cancel)
	}

	// micro seconds
	latency := MicroTilNow(then)

	// collect stats before adjusting latency
	if s.reporter != nil {
		s.reporter.Report(req, rsp, err, latency)
	}

	if err != nil {
		latency = int64(math.Min(s.latencyContext()*errorFactor, float64(maxErrorMicros)))
	}

	//TODO: observe selectively, we only keep track of 100 request latencies. Ideally it should be
	//      spread over 1 sec interval. one way to do it is to have some chan sending signaling
	//      every 1 second. each supervisor keeps track of call counts, it's cleared when tick
	//      is received. this way we can roughly keep track of qps. then we just need to sample
	//      based on that qps. Call this qps estimator.
	//
	//      Another approach is the approximate sampling:
    //			https://github.com/samuel/go-metrics/blob/master/metrics/histogram_mp.go
	s.latencies.Observe(latency)
	sampled := s.latencies.Sampled()
	avg := average(sampled)

	// set average for faster access later on.
	atomic.StoreInt64(&(s.latencyAvg), int64(avg))
	//log.Printf("current %v avg %v, %v", latency, avg, sampled)
	s.svcLock.RUnlock()
	// End of lock

	// react to faulty services.
	switch {
	case s.isDead():
		// Reactions to service being dead:
		// If we have serviceMaker, try make a new service out of it.
		if s.serviceMaker != nil {
			if atomic.CompareAndSwapInt32((*int32)(&s.replacerRunning), 0, 1) {
				// setting service to nil prevents service being used till new serivce is created.
				s.svcLock.Lock()
				if err := s.Close(); err != nil {
					log.Printf("Error closing a service %v, the error is %v", s.name, err)
				}
				s.service = nil
				s.svcLock.Unlock()
				go func() {
					log.Printf("Service \"%v\" gone bad, start replacer routine. This will "+
						"try replacing underlying service at fixed interval, until "+
						"service become healthy.", s.name)
					for {
						_, newService, err := s.serviceMaker.(ServiceMaker).Make()
						if err == nil {
							log.Printf("replacer obtained new service for \"%v\"", s.name)
							s.svcLock.Lock()
							s.service = newService
							s.latencies.Clear()
							s.latencyAvg = 0
							s.svcLock.Unlock()
							log.Printf("replacer of \"%v\" successfully switched on a new service. Now exiting.", s.name)
							atomic.StoreInt32((*int32)(&s.replacerRunning), 0)
							break
						} else {
							log.Printf("replacer errors out for \"%v\", will try later. %v", s.name, err)
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
					log.Printf("Service \"%v\" gone bad, start probing\n", s.name)
					for {
						time.Sleep(time.Duration(int64(proberFreqSec)) * time.Second)
						if !s.isDead() {
							log.Printf("Service \"%v\" recovered, exit prober routine\n", s.name)
							atomic.StoreInt32((*int32)(&s.proberRunning), 0)
							break
						}
						log.Printf("Service %v is dead, probing..", s.name)

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

func (s *RobustService) Serve(req interface{}, rsp interface{}, cancel chan int) (err error) {
	err =CancelledErr
	tries := s.Retries + 1
	for ; (err == CancelledErr || s.RetryFn == nil || s.RetryFn(req, rsp, err)) &&
		tries > 0; tries += 1 {
		// check for cancellation
		if isCancelled(cancel) {
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
// TODO: tricky part though is the case of cold start
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
	prob := 0.0
	latencyC := c.LatencyAvg()
	var s *Supervisor
	for retries := 0; rand.Float64() >= prob && retries < maxSelectorRetry; retries += 1 {
		s = c.Services[rand.Int()%len(c.Services)]
		//TODO:
		//  - tweak formula
		//  - should we not call dead service at all? current it's still called with certain prob.
		if s.isDead() {
			// if all services are dead, we will choose the last one, this is by design
			continue
		}
		prob = math.Min(1, (latencyC*latencyBuffer)/float64(s.latencyAvg))
	}
	return s
}

func (c *Cluster) serveOnce(req interface{}, rsp interface{}, cancel chan int) (err error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	then := time.Now()

	if len(c.Services) == 0 {
		err = Error("There's no underlying service in cluster " + c.Name)
		return
	}

	// pick one random
	s := c.pickAService()

	// serve
	err = s.Serve(req, rsp, cancel)

	if c.Reporter != nil {
		latency := MicroTilNow(then)
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

func (c *Cluster) Serve(req interface{}, rsp interface{}, cancel chan int) (err error) {
	for i := 0; i <= c.Retries; i += 1 {
		err = c.serveOnce(req, rsp, cancel)
		if err == nil {
			return
		} else {
			log.Printf("Error serving request in cluster %v. Error is: %v\n", c.Name, err)
		}
		if isCancelled(cancel) {
			return
		}
	}
	log.Printf("Exhausted retries of serving request in cluster %v\n", c.Name)
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
//
//                                  /  Replaceable - KeyspaceService  --\
//            Supervisor - Cluster  -  Replaceable - KeyspaceService  ---->  Cassandra host 1
//          /                       \  Replaceable - KeyspaceService  --/
//         / 
// Cluster  
//         \
//          \                       /  Replaceable - KeyspaceService  --\
//            Supervisor - Cluster  -  Replaceable - KeyspaceService  ---->  Cassandra host 2
//                                  \  Replaceable - KeyspaceService  --/
//
// Create a reliable service out of a group of service makers. n services will be created by
// each ServiceMaker (think connections).
type ReliableServiceConf struct {
	Name         string
	Makers       []ServiceMaker // ClientBuilder
	Retries      int            // default to 0
	Concurrency  int            // default to 1
	Prober       interface{}    // default to nil
	Stats        gostrich.Stats // default to nil
	PerHostStats bool           // whether to report per host stats
}

func NewReliableService(conf ReliableServiceConf) Service {
	var sname string
	var svc Service
	var err error
	var reporter ServiceReporter

	supers := make([]*Supervisor, len(conf.Makers))
	if conf.Stats != nil {
		reporter = NewBasicStatsReporter(conf.Stats)
	}
	top := &Cluster{
		Name:     conf.Name,
		Services: supers,
		Retries:  conf.Retries,
		Reporter: reporter,
	}

	for i, maker := range conf.Makers {
		var concur int
		if conf.Concurrency == 0 {
			concur = 1
		} else {
			concur = conf.Concurrency
		}
		services := make([]*Supervisor, concur)
		cluster := &Cluster{Name: sname, Services: services}
		for j := range services {
			sname, svc, err = maker.Make()
			if err != nil {
				log.Printf("Failed to make a service: %v %v. Error is %v", conf.Name, sname, err)
			}
			services[j] = NewReplaceable(
				fmt.Sprintf("%v:conn:%v", conf.Name, j),
				svc,
				func() float64 {
					return cluster.LatencyAvg()
				},
				nil,
				maker)
		}
		if conf.Stats != nil && conf.PerHostStats {
			reporter = NewBasicStatsReporter(conf.Stats.Scoped(sname))
		} else {
			reporter = nil
		}
		supers[i] = NewSupervisor(
			sname,
			cluster,
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

func isCancelled(cancel chan int) bool {
	if cancel != nil {
		select {
		case <-cancel:
			return true
		default:
		}
	}
	return false
}
