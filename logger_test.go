package eslogger_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/NadiaSama/eslogger"
)

type (
	testTransport struct {
		check func(req *http.Request) (*http.Response, error)
	}
)

func newTestTransport(tt func(req *http.Request) (*http.Response, error)) http.RoundTripper {
	return &testTransport{
		check: tt,
	}
}

func (tt *testTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return tt.check(req)
}

func TestLogger(t *testing.T) {
	a := 1
	b := "hehe"
	c := 2
	d := "haha"
	addr := "http://127.0.0.1:1234"
	streamName := "test-stream"

	t.Run("test period sync", func(t *testing.T) {
		tt := newTestTransport(func(req *http.Request) (*http.Response, error) {
			if have, want := req.URL.String(), fmt.Sprintf("%s/%s/_bulk", addr, streamName); have != want {
				t.Errorf("test url fail have=%s want=%s", have, want)
			}

			reader, err := req.GetBody()
			if err != nil {
				t.Fatalf("get reader fail error=%s", err.Error())
			}
			defer reader.Close()

			raw, err := ioutil.ReadAll(reader)
			if err != nil {
				t.Fatalf("read reader fail error=%s", err.Error())
			}
			lines := strings.Split(string(raw), "\n")
			if lines[0] != lines[2] || lines[0] != `{"create":{}}` {
				t.Errorf("invalid lines[0], lines[2] %s %s", lines[0], lines[2])
			}

			if len(lines) != 5 {
				t.Errorf("invalid lines '%s'", string(raw))
			}

			var m map[string]interface{}
			json.Unmarshal([]byte(lines[1]), &m)
			if have, want := int(m["a"].(float64)), a; have != want {
				t.Errorf("invalid lines1 have=%d want=%d", have, want)
			}
			if have, want := m["b"].(string), b; have != want {
				t.Errorf("invalid lines1 have=%s want=%s", have, want)
			}

			json.Unmarshal([]byte(lines[3]), &m)
			if have, want := int(m["c"].(float64)), c; have != want {
				t.Errorf("invalid lines1 have=%d want=%d", have, want)
			}
			if have, want := m["d"], d; have != want {
				t.Errorf("invalid lines3 have=%s want=%s", have, want)
			}

			return &http.Response{
				StatusCode: 200,
			}, nil
		})

		cfg := eslogger.NewConfig(addr, streamName).Transport(tt)
		logger := eslogger.New(cfg)
		if err := logger.Open(context.Background()); err != nil {
			t.Fatalf("open logger fail error=%s", err.Error())
		}
		if err := logger.Log("a", a, "b", b); err != nil {
			t.Fatalf("log fail error=%s", err.Error())
		}
		if err := logger.Log("c", c, "d", d); err != nil {
			t.Fatalf("log fail error=%s", err.Error())
		}

		time.Sleep(time.Second * 1)
		logger.Close()
	})

	t.Run("test buf size out of maxSize", func(t *testing.T) {
		count := 0
		tt := newTestTransport(func(req *http.Request) (*http.Response, error) {
			count += 1
			reader, err := req.GetBody()
			if err != nil {
				t.Fatalf("get reader fail error=%s", err.Error())
			}
			defer reader.Close()

			raw, err := ioutil.ReadAll(reader)
			if err != nil {
				t.Fatalf("read reader fail error=%s", err.Error())
			}
			lines := strings.Split(string(raw), "\n")
			if lines[0] != `{"create":{}}` {
				t.Errorf("invalid lines[0], lines[2] %s %s", lines[0], lines[2])
			}
			if len(lines) != 3 {
				t.Errorf("invalid request '%s'", string(raw))
			}
			var m map[string]interface{}
			json.Unmarshal([]byte(lines[1]), &m)
			if count == 1 {
				if have, want := int(m["a"].(float64)), a; have != want {
					t.Errorf("invalid lines1 have=%d want=%d", have, want)
				}

				if have, want := m["b"].(string), b; have != want {
					t.Errorf("invalid lines1 have=%s want=%s", have, want)
				}
			} else if count == 2 {
				if have, want := int(m["c"].(float64)), c; have != want {
					t.Errorf("invalid lines1 have=%d want=%d", have, want)
				}
				if have, want := m["d"], d; have != want {
					t.Errorf("invalid lines3 have=%s want=%s", have, want)
				}
			} else {
				return nil, errors.New("unexpected call")
			}

			return &http.Response{
				StatusCode: 200,
			}, nil
		})

		cfg := eslogger.NewConfig(addr, streamName).Transport(tt).BufMaxSize(1)
		logger := eslogger.New(cfg)
		if err := logger.Open(context.Background()); err != nil {
			t.Fatalf("open logger fail error=%s", err.Error())
		}
		if err := logger.Log("a", a, "b", b); err != nil {
			t.Fatalf("log fail error=%s", err.Error())
		}
		if err := logger.Log("c", c, "d", d); err != nil {
			t.Fatalf("log fail error=%s", err.Error())
		}

		time.Sleep(time.Second * 1)
		if count != 2 {
			t.Errorf("invalid count %d", count)
		}
		logger.Close()
	})
}
