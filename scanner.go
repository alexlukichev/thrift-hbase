package hbase

import (
	"context"
	"io"
	"time"

	hb "github.com/alexlukichev/thrift-hbase/gen-go/hbase"
)

type Scanner struct {
	hbase     *HBaseClient
	scannerId hb.ScannerID
	batchSize int32
	index     int
	batch     []*hb.TRowResult_

	release func()
}

func newScanner(
	hbase *HBaseClient,
	scannerId hb.ScannerID,
	batchSize int32,
	release func(),
) *Scanner {
	return &Scanner{
		hbase:     hbase,
		scannerId: scannerId,
		batchSize: batchSize,
		index:     0,
		batch:     nil,
		release:   release,
	}
}

var EOF = io.EOF

func (s *Scanner) Next(ctx context.Context) ([]byte, map[string][]byte, error) {
	if s.batch == nil || s.index >= len(s.batch) {
		var err error
		s.batch, err = s.hbase.HBase().ScannerGetList(ctx, s.scannerId, s.batchSize)
		if err != nil {
			return nil, nil, err
		}
		if len(s.batch) == 0 {
			return nil, nil, EOF
		}
		s.index = 0
	}
	row := s.batch[s.index]
	cells := make(map[string][]byte)
	if row.Columns != nil {
		for key, cell := range row.Columns {
			if cell != nil {
				cells[key] = cell.Value
			}
		}
	}
	s.index += 1
	return row.Row, cells, nil
}

func (s *Scanner) Close() error {
	defer s.release()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return s.hbase.HBase().ScannerClose(ctx, s.scannerId)
}
