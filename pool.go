package hbase

import (
	"context"
	"sync/atomic"
	"time"

	hb "github.com/alexlukichev/thrift-hbase/gen-go/hbase"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("thrift-hbase")

var clientId uint64 = 0

type HBaseClient struct {
	hbase        hb.Hbase
	transport    thrift.TTransport
	id           uint64
	cleanupTimer *time.Timer
}

func (c HBaseClient) HBase() hb.Hbase {
	return c.hbase
}

func (c *HBaseClient) stopCleanupTimer() error {
	log.Debugf("stopped cleanup timer for client (id=%d)", c.id)
	if c.cleanupTimer != nil {
		res := c.cleanupTimer.Stop()
		if !res {
			log.Error("cleanup timer is already expired")
		}
		c.cleanupTimer = nil
	} else {
		log.Error("cleanup algorithm error: stop for non-existing timer")
	}
	return nil
}

type Pool struct {
	hostPort string

	clients   chan *HBaseClient
	createSem chan bool

	poolSize int

	protocolFactory  thrift.TProtocolFactory
	transportFactory thrift.TTransportFactory

	cleanupInterval time.Duration
}

func NewPool(hostPort string, poolSize int) *Pool {
	return NewPoolWithIdleTimeout(hostPort, poolSize, 30*time.Second)
}

func NewPoolWithIdleTimeout(hostPort string, poolSize int, cleanupInterval time.Duration) *Pool {
	log.Debugf("new pool hostPort=%s, poolSize=%d, cleanupInterval=%s", hostPort, poolSize, cleanupInterval.String())
	pool := &Pool{
		hostPort:         hostPort,
		clients:          make(chan *HBaseClient, poolSize),
		createSem:        make(chan bool, poolSize),
		poolSize:         poolSize,
		protocolFactory:  thrift.NewTBinaryProtocolFactoryDefault(), // "binary" protocol
		transportFactory: thrift.NewTBufferedTransportFactory(8192), // "buffered" transport
		cleanupInterval:  cleanupInterval,
	}
	return pool
}

func (p *Pool) startCleanupTimer(c *HBaseClient) {
	log.Debugf("started cleanup timer for client (id=%d)", c.id)
	c.cleanupTimer = time.AfterFunc(p.cleanupInterval, func() {
		log.Debugf("client (id=%d): cleanup timer is triggered", c.id)
		select {
		case client := <-p.clients:
			if client.id == c.id {
				log.Debugf("client(id=%d): closing idle client", client.id)
				<-p.createSem
				client.transport.Close()
			} else {
				log.Error("cleanup algorithm error: client id is different id=%s", client.id)
				p.Release(client)
			}
		default:
			log.Error("cleanup algorithm error: timer was not stopped")
		}
	})
}

func (p *Pool) Acquire(ctx context.Context) (*HBaseClient, error) {
	select {
	case hbase := <-p.clients:
		hbase.stopCleanupTimer()
		log.Debugf("acquire success: using existing client (id=%d)", hbase.id)
		return hbase, nil
	case <-time.After(time.Millisecond):
		select {
		case hbase := <-p.clients:
			hbase.stopCleanupTimer()
			log.Debugf("acquire success: using existing client (id=%d)", hbase.id)
			return hbase, nil
		case p.createSem <- true:
			log.Debug("acquire: no clients available, opening new socket")
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

				newId := atomic.AddUint64(&clientId, +1)

				return &HBaseClient{
					hbase:     hb.NewHbaseClientFactory(transport, p.protocolFactory),
					transport: transport,
					id:        newId,
				}, nil
			}()
			if err != nil {
				log.Errorf("acquire failure: couldn't create new client: %s", err.Error())
				// On error, release sem
				<-p.createSem
				return nil, err
			}
			log.Debugf("acquire success: created new client (id=%d)", hbase.id)
			return hbase, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (p *Pool) Release(hbase *HBaseClient) {
	select {
	case p.clients <- hbase:
		p.startCleanupTimer(hbase)
		log.Debugf("client (id=%d): returned to queue", hbase.id)
	default:
		log.Debugf("pool overflow: closing client (id=%d)", hbase.id)
		// pool overflow
		<-p.createSem
		err := hbase.transport.Close()
		if err != nil {
			log.Errorf("client (id=%d): error on closing client: %s", err.Error())
		} else {
			log.Debugf("client (id=%d): closed successfully", hbase.id)
		}
	}
}

func (p *Pool) Close(ctx context.Context) error {
	log.Debug("closing pool...")
	close(p.createSem)
	close(p.clients)
	for client := range p.clients {
		client.stopCleanupTimer()
		client.transport.Close()
		log.Debugf("client (id=%d) was released", client.id)
	}
	log.Debug("pool closed successfully")
	return nil
}
