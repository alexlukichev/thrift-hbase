package hbase

import (
	"context"
	"time"

	hb "github.com/alexlukichev/thrift-hbase/gen-go/hbase"
	"github.com/apache/thrift/lib/go/thrift"
)

type HBaseClient struct {
	hbase     hb.Hbase
	transport thrift.TTransport
}

func (c HBaseClient) HBase() hb.Hbase {
	return c.hbase
}

type Pool struct {
	hostPort string

	clients   chan *HBaseClient
	createSem chan bool

	poolSize int

	protocolFactory  thrift.TProtocolFactory
	transportFactory thrift.TTransportFactory

	idleCleanup *time.Ticker
	stopCleanup chan bool
}

func NewPool(hostPort string, poolSize int) *Pool {
	return NewPoolWithIdleTimeout(hostPort, poolSize, 30*time.Second)
}

func NewPoolWithIdleTimeout(hostPort string, poolSize int, cleanupInterval time.Duration) *Pool {
	ticker := time.NewTicker(cleanupInterval)

	pool := &Pool{
		hostPort:         hostPort,
		clients:          make(chan *HBaseClient, poolSize),
		createSem:        make(chan bool, poolSize),
		poolSize:         poolSize,
		protocolFactory:  thrift.NewTBinaryProtocolFactoryDefault(), // "binary" protocol
		transportFactory: thrift.NewTBufferedTransportFactory(8192), // "buffered" transport
		idleCleanup:      ticker,
		stopCleanup:      make(chan bool),
	}

	// start cleanup by timer
	go pool.cleanup()

	return pool
}

func (p *Pool) cleanup() {
	for {
		select {
		case <-p.stopCleanup:
			return
		case <-p.idleCleanup.C:
			// release all idle clients
			for {
				stop := func() bool {
					select {
					case client := <-p.clients:
						<-p.createSem
						client.transport.Close()
						return false
					default:
						return true
					}
				}()
				if stop {
					break
				}
			}
		}
	}
}

func (p *Pool) Acquire(ctx context.Context) (*HBaseClient, error) {
	select {
	case hbase := <-p.clients:
		return hbase, nil
	case <-time.After(time.Millisecond):
		select {
		case hbase := <-p.clients:
			return hbase, nil
		case p.createSem <- true:
			// No clients available, let's make a new one
			hbase, err := func() (*HBaseClient, error) {
				var transport thrift.TTransport
				transport, err := thrift.NewTSocket(p.hostPort)
				if err != nil {
					return nil, err
				}

				err = transport.Open()
				if err != nil {
					return nil, err
				}

				transport, err = p.transportFactory.GetTransport(transport)
				if err != nil {
					return nil, err
				}

				return &HBaseClient{
					hbase:     hb.NewHbaseClientFactory(transport, p.protocolFactory),
					transport: transport,
				}, nil
			}()
			if err != nil {
				// On error, release sem
				<-p.createSem
			}
			return hbase, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (p *Pool) Release(hbase *HBaseClient) {
	select {
	case p.clients <- hbase:
	default:
		// pool overflow
		<-p.createSem
		hbase.transport.Close()
	}
}

func (p *Pool) Close(ctx context.Context) error {
	p.idleCleanup.Stop()
	p.stopCleanup <- true
	close(p.createSem)
	close(p.clients)
	for client := range p.clients {
		client.transport.Close()
	}
	return nil
}
