package hbase

import (
	"context"
	"fmt"
	"sync"

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

	clients       chan *HBaseClient
	poolSize      int
	activeClients int
	clientLock    sync.Locker

	protocolFactory  thrift.TProtocolFactory
	transportFactory thrift.TTransportFactory

	closing bool
}

func NewPool(hostPort string, poolSize int) *Pool {
	return &Pool{
		hostPort:         hostPort,
		clients:          make(chan *HBaseClient, poolSize),
		poolSize:         poolSize,
		activeClients:    0,
		clientLock:       &sync.Mutex{},
		protocolFactory:  thrift.NewTBinaryProtocolFactoryDefault(), // "binary" protocol
		transportFactory: thrift.NewTBufferedTransportFactory(8192), // "buffered" transport
		closing:          false,
	}
}

func (p *Pool) Acquire(ctx context.Context) (*HBaseClient, error) {
	if p.closing {
		return nil, PoolClosed
	}

	if p.activeClients < p.poolSize {
		select {
		case hbase := <-p.clients:
			return hbase, nil
		default:
			hbase, err := func() (*HBaseClient, error) {
				p.clientLock.Lock()
				defer p.clientLock.Unlock()

				if p.activeClients < p.poolSize {
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

					p.activeClients += 1
					return &HBaseClient{
						hbase:     hb.NewHbaseClientFactory(transport, p.protocolFactory),
						transport: transport,
					}, nil
				} else {
					return nil, nil
				}
			}()

			if err != nil {
				return nil, err
			}

			if hbase != nil {
				return hbase, nil
			}
		}
	}

	select {
	case hbase := <-p.clients:
		return hbase, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *Pool) Release(hbase *HBaseClient) {
	p.clients <- hbase
}

func (p *Pool) Close(ctx context.Context) error {
	for p.activeClients > 0 {
		select {
		case hbaseClient := <-p.clients:
			hbaseClient.transport.Close()
			p.activeClients -= 1
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
