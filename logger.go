package eslogger

import (
	"bytes"
	"context"
	"encoding"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"time"
)

type (
	ESDataStreamConfig struct {
		Address    string
		StreamName string
		Tick       time.Duration
		BufMaxSize int
	}

	ESDataStream struct {
		url       string
		config    ESDataStreamConfig
		data      chan map[string]interface{}
		buf       *bytes.Buffer
		transport http.RoundTripper
		logger    *log.Logger
	}
)

const (
	DefaultMaxSize  = 4096
	DefaultDuration = time.Millisecond * 100
)

var (
	createOp = []byte(`{"create":{}}`)
)

//NewESDataStream create a new ESDataStream instrance.
//config sepcific ealsticsearch address and log sync period
func NewESDataStream(config ESDataStreamConfig, transport http.RoundTripper, logger *log.Logger) *ESDataStream {
	c := config
	if c.BufMaxSize == 0 {
		c.BufMaxSize = DefaultMaxSize
	}

	if c.Tick == 0 {
		c.Tick = DefaultDuration
	}
	return &ESDataStream{
		url:       fmt.Sprintf("%s/%s/_bulk", config.Address, config.StreamName),
		config:    c,
		data:      make(chan map[string]interface{}, 32),
		buf:       bytes.NewBuffer(make([]byte, 0, config.BufMaxSize)),
		transport: transport,
		logger:    logger,
	}
}

//Open start working goroutine send bulk request to elasticsearch periodly
func (esds *ESDataStream) Open(ctx context.Context) error {
	go func() {
		ticker := time.NewTicker(esds.config.Tick)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := esds.putData(ctx); err != nil {
					if esds.logger != nil {
						esds.logger.Printf("putData error %s", err.Error())
					}
				}

			case raw := <-esds.data:
				b, _ := json.Marshal(raw)
				if esds.buf.Len()+len(b)+len(createOp)+2 > esds.config.BufMaxSize {
					if err := esds.putData(ctx); err != nil {
						if esds.logger != nil {
							esds.logger.Printf("putData error %s", err.Error())
						}
					}
				}

				esds.buf.Write(createOp)
				esds.buf.Write([]byte("\n"))
				esds.buf.Write(b)
				esds.buf.Write([]byte("\n"))
			}
		}
	}()
	return nil
}

//Log send data to working goroutine
func (esds *ESDataStream) Log(keyvals ...interface{}) error {
	//build map[string]interface{} according keyvals
	//copy from https://github.com/go-kit/log/blob/main/json_logger.go

	if len(keyvals)%2 != 0 {
		return fmt.Errorf("invalid keyvals len=%d", len(keyvals))
	}

	n := (len(keyvals)) / 2
	m := make(map[string]interface{}, n)
	for i := 0; i < len(keyvals); i += 2 {
		k := keyvals[i]
		v := keyvals[i+1]
		merge(m, k, v)
	}

	m["@timestamp"] = time.Now().Format(time.RFC3339)

	select {
	case esds.data <- m:
	default:
		return fmt.Errorf("the log queue full len=%d", len(esds.data))
	}
	return nil
}

func (esds *ESDataStream) putData(ctx context.Context) error {
	defer func() {
		esds.buf.Reset()
	}()

	if esds.buf.Len() == 0 {
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, esds.url, esds.buf)
	if err != nil {
		return fmt.Errorf("build request fail %w", err)
	}
	req.Header.Add("Content-Type", "application/json")

	var client *http.Client
	if esds.transport != nil {
		client = &http.Client{
			Transport: esds.transport,
		}
	} else {
		client = http.DefaultClient
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("do http request fail %w", err)
	}
	if resp.Body != nil {
		defer resp.Body.Close()
	}

	raw, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read body error %w", err)
	}

	if resp.StatusCode > 299 {
		return fmt.Errorf("invalid status code %d body %s", resp.StatusCode, string(raw))
	}

	return nil
}

func merge(dst map[string]interface{}, k, v interface{}) {
	var key string
	switch x := k.(type) {
	case string:
		key = x
	case fmt.Stringer:
		key = safeString(x)
	default:
		key = fmt.Sprint(x)
	}

	// We want json.Marshaler and encoding.TextMarshaller to take priority over
	// err.Error() and v.String(). But json.Marshall (called later) does that by
	// default so we force a no-op if it's one of those 2 case.
	switch x := v.(type) {
	case json.Marshaler:
	case encoding.TextMarshaler:
	case error:
		v = safeError(x)
	case fmt.Stringer:
		v = safeString(x)
	}

	dst[key] = v
}

func safeString(str fmt.Stringer) (s string) {
	defer func() {
		if panicVal := recover(); panicVal != nil {
			if v := reflect.ValueOf(str); v.Kind() == reflect.Ptr && v.IsNil() {
				s = "NULL"
			} else {
				panic(panicVal)
			}
		}
	}()
	s = str.String()
	return
}

func safeError(err error) (s interface{}) {
	defer func() {
		if panicVal := recover(); panicVal != nil {
			if v := reflect.ValueOf(err); v.Kind() == reflect.Ptr && v.IsNil() {
				s = nil
			} else {
				panic(panicVal)
			}
		}
	}()
	s = err.Error()
	return
}
