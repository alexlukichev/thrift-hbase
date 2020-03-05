package hbase

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"gotest.tools/assert"
)

func getEnv(name string, def string) string {
	value, ok := os.LookupEnv(name)
	if !ok {
		return def
	}
	return value
}

var hostPort = getEnv("HBASE_ADDRESS", "localhost:9090")

func TestPutAndScan(t *testing.T) {
	client := NewClient(hostPort, 10, 1, 1)
	defer client.Close(context.Background())

	err := client.CreateTable(context.Background(), []byte("test"), [][]byte{
		[]byte("cf"),
	})
	if err != nil && err != AlreadyExists {
		t.Errorf("Cannot create table: %s", err.Error())
		return
	}

	dataStr := map[string]map[string]string{
		"r0": map[string]string{
			"cf:c0": "123",
			"cf:c1": "456",
		},
		"r1": map[string]string{
			"cf:c0": "789",
			"cf:c1": "123",
		},
	}

	dataBinary := make(map[string]map[string][]byte)
	for rkey, rvalue := range dataStr {
		rbinary := make(map[string][]byte)
		for ckey, cvalue := range rvalue {
			rbinary[ckey] = []byte(cvalue)
		}
		dataBinary[rkey] = rbinary
	}

	ctx := context.Background()
	err = client.Put(ctx, []byte("test"), []Row{
		Row{[]byte("r0"), map[string][]byte{"cf:c0": []byte("123"), "cf:c1": []byte("456")}},
		Row{[]byte("r1"), map[string][]byte{"cf:c0": []byte("789"), "cf:c1": []byte("123")}},
	})
	if err != nil {
		t.Errorf("Cannot put data in the DB: %s", err.Error())
		return
	}

	scanner, err := client.Scan(ctx, []byte("test"), []byte("!"), []byte("~"), [][]byte{[]byte("cf:c0"), []byte("cf:c1")})
	if err != nil {
		t.Errorf("Cannot create scanner: %s", err.Error())
		return
	}
	defer scanner.Close()

	result := make(map[string]map[string]string)
	for {
		row, cols, err := scanner.Next(ctx)
		if err == EOF {
			break
		}
		if err != nil {
			t.Errorf("Cannot iterate scanner: %s", err.Error())
			return
		}
		rowMap := make(map[string]string)
		for colNameBytes, colValueBytes := range cols {
			rowMap[string(colNameBytes)] = string(colValueBytes)
		}
		result[string(row)] = rowMap
	}

	dataStrJson, err := json.Marshal(dataStr)
	if err != nil {
		t.Errorf("Cannot marshal: %v", dataStr)
		return
	}
	resultStrJson, err := json.Marshal(result)
	if err != nil {
		t.Errorf("Cannot marshal: %v", result)
		return
	}

	assert.Assert(t, string(dataStrJson) == string(resultStrJson), "Result doesn't match: %v != %v", string(dataStrJson), string(resultStrJson))
}
