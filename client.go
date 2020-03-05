package hbase

import (
	"context"
	"fmt"

	hb "github.com/alexlukichev/thrift-hbase/gen-go/hbase"
)

type Client struct {
	batchSize    int32
	putBatchSize int

	pool *Pool
}

func NewClient(hostPort string, poolSize, batchSize, putBatchSize int) *Client {
	return &Client{
		batchSize:    int32(batchSize),
		putBatchSize: putBatchSize,
		pool:         NewPool(hostPort, poolSize),
	}
}

// Scan creates a table scanner. The scanner can be iterated with Next() and must be closed with Close() after
// usage
func (c *Client) Scan(ctx context.Context, table []byte, start []byte, end []byte, columns [][]byte) (*Scanner, error) {
	hbase, err := c.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	var _columns []hb.Text
	for _, c := range columns {
		_columns = append(_columns, c)
	}

	scannerId, err := hbase.HBase().ScannerOpenWithStop(ctx, table, start, end, _columns, map[string]hb.Text{})
	if err != nil {
		c.pool.Release(hbase)
		return nil, err
	}
	return newScanner(
		hbase,
		scannerId,
		c.batchSize,
		func() {
			c.pool.Release(hbase)
		}), nil
}

type Row struct {
	Row     []byte
	Columns map[string][]byte
}

func (c *Client) putBatch(ctx context.Context, table hb.Text, batch []*hb.BatchMutation) error {
	hbase, err := c.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer c.pool.Release(hbase)

	err = hbase.HBase().MutateRows(ctx, table, batch, map[string]hb.Text{})
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) Put(ctx context.Context, table []byte, rows []Row) error {
	batch := make([]*hb.BatchMutation, 0, c.putBatchSize)
	for _, row := range rows {
		mutations := make([]*hb.Mutation, 0, len(row.Columns))
		for key, value := range row.Columns {
			mutations = append(mutations, &hb.Mutation{
				IsDelete:   false,
				Column:     hb.Text(key),
				Value:      hb.Text(value),
				WriteToWAL: true,
			})
		}
		batch = append(batch, &hb.BatchMutation{
			Row:       row.Row,
			Mutations: mutations,
		})
		if len(batch) >= c.putBatchSize {
			err := c.putBatch(ctx, table, batch)
			if err != nil {
				return err
			}
			batch = make([]*hb.BatchMutation, 0, c.putBatchSize)
		}
	}
	if len(batch) > 0 {
		err := c.putBatch(ctx, table, batch)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) Close(ctx context.Context) {
	c.pool.Close(ctx)
}

var AlreadyExists = fmt.Errorf("Table already exists")

func (c *Client) CreateTable(ctx context.Context, table []byte, columnFamilies [][]byte) error {
	columnFamiliesObj := make([]*hb.ColumnDescriptor, 0, len(columnFamilies))
	for _, cf := range columnFamilies {
		columnFamiliesObj = append(columnFamiliesObj,
			&hb.ColumnDescriptor{
				Name:            []byte(cf),
				MaxVersions:     1,
				Compression:     "NONE",
				InMemory:        false,
				BloomFilterType: "NONE",
				TimeToLive:      -1,
			})
	}

	hbase, err := c.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer c.pool.Release(hbase)

	err = hbase.HBase().CreateTable(ctx, table, columnFamiliesObj)
	if err != nil {
		if _, ok := err.(*hb.AlreadyExists); ok {
			return AlreadyExists
		}
		return err
	}
	return nil
}
