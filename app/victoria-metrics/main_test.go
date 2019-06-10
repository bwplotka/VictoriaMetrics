// +build integration

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
)

const (
	dataDir    = "testdata/"
	httpServer = ":7654"
	graphite   = ":2003"
)

var (
	storagePath string
)

type test struct {
	name   string
	Data   json.RawMessage `json:"data"`
	Query  string          `json:"query"`
	Result []Row           `json:"result"`
}

type Row struct {
	Metric     map[string]string `json:"metric"`
	Values     []float64         `json:"values"`
	Timestamps []int64           `json:"timestamps"`
}

func (t test) empty() bool {
	return len(t.Data) == 0 || len(t.Result) == 0 || t.Query == ""
}
func TestMain(m *testing.M) {
	setUp()
	code := m.Run()
	tearDown()
	os.Exit(code)
}

func setUp() {
	storagePath = os.TempDir()
	processFlags()
	vmstorage.Init()
	vmselect.Init()
	vminsert.Init()
	go httpserver.Serve(*httpListenAddr, requestHandler)
	if err := waitFor(2*time.Second, func() bool {
		resp, err := http.Get("http://127.0.0.1" + httpServer + "/health")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == 200
	}); err != nil {
		log.Fatalf("http server can't start for %s seconds", 2*time.Second)
	}
}

func processFlags() {
	flag.Parse()
	for _, fs := range []struct {
		flag  string
		value string
	}{
		{flag: "storageDataPath", value: storagePath},
		{flag: "httpListenAddr", value: httpServer},
		{flag: "graphiteListenAddr", value: graphite},
	} {
		// panics if flag doesn't exist
		if err := flag.Lookup(fs.flag).Value.Set(fs.value); err != nil {
			log.Fatalf("unable to set %q with value %q, err: %v", fs.flag, fs.value, err)
		}
	}
}

func waitFor(timeout time.Duration, f func() bool) error {
	fraction := timeout / 10
	for i := fraction; i < timeout; i += fraction {
		if f() {
			return nil
		}
	}
	return fmt.Errorf("timeout")
}

func tearDown() {
	vminsert.Stop()
	vmstorage.Stop()
	vmselect.Stop()
	if err := httpserver.Stop(*httpListenAddr); err != nil {
		log.Fatalf("cannot stop the webservice: %s", err)
	}
	dir, _ := ioutil.ReadDir(storagePath)
	for _, f := range dir {
		os.RemoveAll(path.Join(storagePath, f.Name()))
	}
}

func TestInfluxDB(t *testing.T) {
	tt := readIn("influxdb", t)
	t.Run("influxdb", func(t *testing.T) {
		for _, test := range tt {
			t.Run(test.name, func(t *testing.T) {
				t.Parallel()
				httpWrite(t, "http://127.0.0.1"+httpServer+"/write", test.Data)
				time.Sleep(5 * time.Second)
				data := httpRead(t, "http://127.0.0.1"+httpServer, test.Query)
				RowContains(t, data, test.Result)
			})
		}
	})

}

func readIn(readFor string, t *testing.T) []test {
	t.Helper()
	s := newSuite(t)
	var tt []test
	s.NoError(filepath.Walk(filepath.Join(dataDir, readFor), func(path string, info os.FileInfo, err error) error {
		if filepath.Ext(path) != ".json" {
			return nil
		}
		b, err := ioutil.ReadFile(path)
		s.NoError(err)
		item := test{name: info.Name()}
		s.NoError(json.Unmarshal(b, &item))
		tt = append(tt, item)
		return nil
	}))
	if len(tt) == 0 {
		t.Fatalf("no test found in %s", filepath.Join(dataDir, readFor))
	}
	return tt
}

func httpWrite(t *testing.T, address string, data []byte) {
	t.Helper()
	s := newSuite(t)
	resp, err := http.Post(address, "", bytes.NewBuffer(data))
	s.NoError(err)
	s.NoError(resp.Body.Close())
	s.EqualInt(resp.StatusCode, 204)
}

func httpRead(t *testing.T, address, query string) []Row {
	t.Helper()
	s := newSuite(t)
	resp, err := http.Get(address + query)
	s.NoError(err)
	defer resp.Body.Close()
	s.EqualInt(resp.StatusCode, 200)
	var rows []Row
	for dec := json.NewDecoder(resp.Body); dec.More(); {
		var row Row
		s.NoError(dec.Decode(&row))
		rows = append(rows, row)
	}
	return rows
}

type suite struct{ t *testing.T }

func newSuite(t *testing.T) *suite { return &suite{t: t} }
func (s *suite) NoError(err error) {
	s.t.Helper()
	if err != nil {
		s.t.Errorf("unxepected error %v", err)
		s.t.FailNow()
	}
}
func (s *suite) EqualInt(a, b int) {
	s.t.Helper()
	if a != b {
		s.t.Errorf("%d not equal %d", a, b)
		s.t.FailNow()
	}
}

func RowContains(t *testing.T, rows, contains []Row) {
	for _, r := range rows {
		contains = compareAndRemove(r, contains)
	}
	if len(contains) > 0 {
		t.Fatalf("result rows %+v not found in %+v", contains, rows)
	}
}

func compareAndRemove(r Row, contains []Row) []Row {
	for i, item := range contains {
		if reflect.DeepEqual(r.Metric, item.Metric) && reflect.DeepEqual(r.Values, item.Values) {
			contains[i] = contains[len(contains)-1]
			return contains[:len(contains)-1]
		}
	}
	return contains
}
