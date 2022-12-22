package portforward

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/phayes/freeport"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	k8sportforward "k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

type PortForwardPair struct {
	LocalPort  int
	RemotePort int
}

// KubeForwarder is a portforwarder for forwarding from a local port to a kubernetes Pod and port.
// It is equivalent to "kubectl port-forward".
type KubeForwarderConfig struct {
	PodName      string
	PodNamespace string

	// List of port forward pairs
	Ports []PortForwardPair

	RESTConfig *rest.Config
	ClientSet  *kubernetes.Clientset
}

type KubeForwarderChannels struct {
	ready chan struct{}
	done  chan struct{}
	stop  chan struct{}
}

func NewKubeForwarderChannels() *KubeForwarderChannels {
	return &KubeForwarderChannels{
		ready: make(chan struct{}),    // Closed when portforwarding ready.
		done:  make(chan struct{}),    // Closed when portforwarding is done.
		stop:  make(chan struct{}, 1), // is never closed by k8sportforward
	}
}

type KubeForwarder struct {
	sync.Mutex

	KubeForwarderConfig
	shouldStop bool

	// channels per local port
	channels map[int]*KubeForwarderChannels

	stopChOnce sync.Once
}

// NewKubeForwarder creates a new KubeForwarder.
func NewKubeForwarder(cfg KubeForwarderConfig) (*KubeForwarder, error) {
	var err error

	// check all the ports are valid, looking for a free local port if not specified
	ports := []PortForwardPair{}
	for _, pair := range cfg.Ports {
		if pair.LocalPort == 0 {
			pair.LocalPort, err = freeport.GetFreePort()
			if err != nil {
				return nil, err
			}
		}
		if pair.RemotePort > 0 {
			// skip pairs with an invalid remote port
			ports = append(ports, pair)
		}
	}
	cfg.Ports = ports

	res := &KubeForwarder{KubeForwarderConfig: cfg}

	res.channels = make(map[int]*KubeForwarderChannels, len(res.Ports))

	for _, pair := range res.Ports {
		localPort := pair.LocalPort
		res.channels[localPort] = NewKubeForwarderChannels()
	}

	return res, nil
}

// Run runs the port forwarding machinery.
// The function returns immediately, and the port forwarding is done in goroutines. You should check if
// the port-forwarding is ready with the Ready() channel.
func (o *KubeForwarder) Run(ctx context.Context) (<-chan struct{}, error) {
	pairWorker := func(pair PortForwardPair, channels *KubeForwarderChannels) error {
		log.Printf("Starting port-forward from :%d --> %s/%s:%d: dialing...", pair.LocalPort, o.PodNamespace, o.PodName, pair.RemotePort)
		req := o.ClientSet.CoreV1().RESTClient().Post().
			Resource("pods").
			Namespace(o.PodNamespace).
			Name(o.PodName).
			SubResource("portforward")
		transport, upgrader, err := spdy.RoundTripperFor(o.RESTConfig)
		if err != nil {
			return err
		}

		dialer := spdy.NewDialer(
			upgrader,
			&http.Client{Transport: transport},
			http.MethodPost,
			req.URL())

		pfwdPorts := []string{fmt.Sprintf("%d:%d", pair.LocalPort, pair.RemotePort)}

		streams := genericclioptions.IOStreams{
			In:     os.Stdin,
			Out:    os.Stdout,
			ErrOut: os.Stderr,
		}

		log.Printf("Waiting until %s/%s is ready for establishing port-forward...", o.PodNamespace, o.PodName)
		if err := WaitPodReady(ctx, o.RESTConfig, o.PodNamespace, o.PodName); err != nil {
			return err
		}
		log.Printf("... %s/%s seems to be ready.", o.PodNamespace, o.PodName)

		// loop forever, until the context is canceled.
	loop:
		for {
			select {
			case <-time.After(500 * time.Millisecond):
				pfwd, err := k8sportforward.New(dialer, pfwdPorts, channels.stop, channels.ready, streams.Out, streams.ErrOut)
				if err != nil {
					log.Printf("error port-forwarding from :%d --> %d: %v", pair.LocalPort, pair.RemotePort, err)
					continue
				}

				log.Printf("Running port-forward from :%d --> %s/%s:%d in a goroutine...", pair.LocalPort, o.PodNamespace, o.PodName, pair.RemotePort)
				err = pfwd.ForwardPorts() // blocks
				if err != nil {
					log.Printf("error port-forwarding from :%d --> %d: %v", pair.LocalPort, pair.RemotePort, err)
					continue
				}

				// check if we are quitting because someone called Stop() or because the port-forward was broken
				// in the last case, loop again
				o.Lock()
				shouldStop := o.shouldStop
				o.Unlock()
				if shouldStop {
					log.Printf("Port-forward from :%d --> %s/%s:%d is done.", pair.LocalPort, o.PodNamespace, o.PodName, pair.RemotePort)
					break loop
				}
				log.Printf("Port-forward from :%d --> %s/%s:%d interrupted: retrying...", pair.LocalPort, o.PodNamespace, o.PodName, pair.RemotePort)
				channels.ready = make(chan struct{})
				channels.stop = make(chan struct{}, 1)

			case <-ctx.Done():
				break loop
			}
		}

		log.Printf("Port-forward is done...")
		close(channels.done)

		return nil
	}

	// start a goroutine for each port-forward pair, NOT waiting for them to finish
	for _, pair := range o.Ports {
		go func(pair PortForwardPair) {
			if err := pairWorker(pair, o.channels[pair.LocalPort]); err != nil {
				log.Printf("error port-forwarding from :%d --> %d: %v", pair.LocalPort, pair.RemotePort, err)
			}
		}(pair)
	}

	// start a goroutine to wait for the cancellation of the context
	go func() {
		<-ctx.Done()
		log.Printf("Context cancelled: stopping port-forwards to %s/%s.", o.PodNamespace, o.PodName)
		o.Stop()
	}()

	return o.Ready(), nil
}

func (o *KubeForwarder) Done() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		for _, v := range o.channels {
			<-v.done
		}
		close(done)
	}()
	return done
}

func (o *KubeForwarder) Ready() <-chan struct{} {
	ready := make(chan struct{})
	go func() {
		for _, v := range o.channels {
			<-v.ready
		}
		close(ready)
	}()

	return ready
}

func (o *KubeForwarder) Stop() error {
	// Make sure we only close the stopCh once.
	o.stopChOnce.Do(func() {
		log.Printf("Stopping port-forwards to %s/%s.", o.PodNamespace, o.PodName)

		o.Lock()
		o.shouldStop = true
		o.Unlock()

		for _, v := range o.channels {
			close(v.stop)
		}
	})
	return nil
}
