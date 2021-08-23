# eslogger
package `eslogger` implements [kit log interface](https://github.com/go-kit/log) write log message into elasticsearch datastream

## usage
create a logger with streamName `logs-test-log`. 

streamName should match one of index templates index patterns
[for detail](https://www.elastic.co/guide/en/elasticsearch/reference/current/data-streams.html)
```go
//create config
conf := eslogger.NewConfig("http://127.0.0.1:9200", "logs-test-log")

//set sync duration
conf.Tick(time.Second)

//create logger
logger := eslogger.New(conf)

//start working loop
if err := logger.Open(context.Background()); err != nil {
    panic(err)
}


logger.Log("message", "Test message", "val", 23)

//stop running loop
logger.Close()
```