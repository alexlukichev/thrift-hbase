package hbase

import (
	"context"
	"fmt"
	"sync"
	"time"

	hb "github.com/alexlukichev/thrift-hbase/gen-go/hbase"
	"github.com/apache/thrift/lib/go/thrift"
)

var PoolClosed = fmt.Errorf("Pool is closed")

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

	poolSize   int
	clientLock sync.Locker

	protocolFactory  thrift.TProtocolFactory
	transportFactory thrift.TTransportFactory

	closing bool
}

func NewPool(hostPort string, poolSize int) *Pool {
	return &Pool{
		hostPort:         hostPort,
		clients:          make(chan *HBaseClient, poolSize),
		createSem:        make(chan bool, poolSize),
		poolSize:         poolSize,
		clientLock:       &sync.Mutex{},
		protocolFactory:  thrift.NewTBinaryProtocolFactoryDefault(), // "binary" protocol
		transportFactory: thrift.NewTBufferedTransportFactory(8192), // "buffered" transport
		closing:          false,
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
			// No existing client, let's make a new one
			hbase, err := func() (*HBaseClient, error) {
				p.clientLock.Lock()
				defer p.clientLock.Unlock()

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
				// On error, release our create hold
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
	close(p.clients)
	for hbaseClient := range p.clients {
		hbaseClient.transport.Close()
	}
	return nil
}
