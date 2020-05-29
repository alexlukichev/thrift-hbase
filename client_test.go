package hbase

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

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

	dataStr, err := createAndFillTable(t, client)
	if err != nil {
		t.Errorf("can't create and fill table: %s", err.Error())
		return
	}

	result, err := scanTable(t, client, "test", "!", "~", []string{"cf:c0", "cf:c1"})
	if err != nil {
		t.Error(err)
		return
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

func createAndFillTable(t *testing.T, client *Client) (map[string]map[string]string, error) {
	err := client.CreateTable(context.Background(), []byte("test"), [][]byte{
		[]byte("cf"),
	})
	if err != nil && err != AlreadyExists {
		t.Errorf("Cannot create table: %s", err.Error())
		return nil, err
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

	err = client.Put(context.Background(), []byte("test"), []Row{
		Row{[]byte("r0"), map[string][]byte{"cf:c0": []byte("123"), "cf:c1": []byte("456")}},
		Row{[]byte("r1"), map[string][]byte{"cf:c0": []byte("789"), "cf:c1": []byte("123")}},
	})
	if err != nil {
		t.Errorf("Cannot put data in the DB: %s", err.Error())
		return nil, err
	}
	return dataStr, err
}

func scanTable(t *testing.T, client *Client, table string, start string, end string, columns []string) (map[string]map[string]string, error) {
	byte_columns := make([][]byte, 0)
	for _, col := range columns {
		byte_columns = append(byte_columns, []byte(col))
	}
	scanner, err := client.Scan(context.Background(), []byte(table), []byte(start), []byte(end), byte_columns)
	if err != nil {
		t.Errorf("Cannot create scanner: %s", err.Error())
		return nil, err
	}
	defer scanner.Close()

	result := make(map[string]map[string]string)
	for {
		row, cols, err := scanner.Next(context.Background())
		if err == EOF {
			break
		}
		if err != nil {
			t.Errorf("Cannot iterate scanner: %s", err.Error())
			return nil, err
		}
		rowMap := make(map[string]string)
		for colNameBytes, colValueBytes := range cols {
			rowMap[string(colNameBytes)] = string(colValueBytes)
		}
		result[string(row)] = rowMap
	}

	return result, err
}

func TestScanAfterIdle(t *testing.T) {
	client := NewClient(hostPort, 10, 1, 1)
	defer client.Close(context.Background())

	_, err := createAndFillTable(t, client)
	if err != nil {
		t.Errorf("can't create and fill table: %s", err.Error())
		return
	}
	t.Log("Created and fill table")

	_, err = scanTable(t, client, "test", "!", "~", []string{"cf:c0", "cf:c1"})
	if err != nil {
		t.Error(err)
		return
	}
	t.Log("Completed first scan")

	time.Sleep(2 * time.Minute)

	t.Log("Started seconds scan")
	_, err = scanTable(t, client, "test", "!", "~", []string{"cf:c0", "cf:c1"})
	if err != nil {
		t.Error(err)
		return
	}
}
