package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"gopkg.in/Shopify/sarama.v1"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

var (
	addr           = flag.String("addr", ":8080", "web address")
	brokers        = flag.String("brokers", os.Getenv("KAFAKA_PEERS"), "The kafaka brokers to connect to")
	verbose        = flag.Bool("verbose", false, "Turn on Sarama logging")
	clientCertFile = flag.String("clientCert", "", "The optional certificate file for client authentication")
	clientKeyFile  = flag.String("clientKey", "", "The optional key file for client authentication")
	caFile         = flag.String("ca", "", "The optional certificate authority file for TLS client authentication")
	verifySsl      = flag.Bool("verify", false, "Optional verify ssl certificates chain")
)

type Server struct {
	DataCollector     sarama.SyncProducer
	AccessLogProducer sarama.AsyncProducer
}

type accessLogEntry struct {
	Method       string  `json:"method"`
	Host         string  `json:"host"`
	Path         string  `json:"path"`
	IP           string  `json:"ip"`
	ResponseTime float64 `json:"response_time"`
	encoded      []byte
	err          error
}

func main() {
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	if *verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama test]", log.LstdFlags|log.Lshortfile)
	}
	if *brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}
	brokerList := strings.Split(*brokers, ",")
	log.Printf("kafaka brokers:%s", strings.Join(brokerList, ","))
	server := &Server{
		DataCollector:     newDataCollector(brokerList),
		AccessLogProducer: newAccessLogProducer(brokerList),
	}
	defer func() {
		if err := server.Close(); err != nil {
			log.Println("Fail to close server", err)
		}
	}()
	log.Fatal(server.Run(*addr))

}

func (self *Server) Run(addr string) error {
	httpServer := &http.Server{
		Addr:    addr,
		Handler: self.Handler(),
	}
	log.Printf("Listening for request on %s...\n", addr)
	return httpServer.ListenAndServe()

}

func (self *Server) Handler() http.Handler {
	return self.withAccessLog(self.collectQueryStringData())
}

func (self *Server) Close() error {

	if err := self.DataCollector.Close(); err != nil {
		log.Println("Failed to shut down data collector cleanly", err)
	}
	if err := self.AccessLogProducer.Close(); err != nil {
		log.Println("Faild to shut down access log producer cleanly", err)
	}
	return nil
}
func (self *Server) collectQueryStringData() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		partion, offset, err := self.DataCollector.SendMessage(&sarama.ProducerMessage{
			Topic: "test",
			Value: sarama.StringEncoder(r.URL.RawQuery),
			//Partition:0,  选择分区
		})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "Failed to store your data:%s", err)
		} else {
			fmt.Fprintf(w, "Your data is stored with unique identifier important/%d/%d", partion, offset)
		}

		//同步消息可以这么处理吗？判定发送消息的状态
		//select {
		//case success := <-self.AccessLogProducer.Successes():
		//	log.Println("offset: ", success.Offset, "timestamp: ", success.Timestamp.String(), "partitions: ", success.Partition)
		//case fail := <-self.AccessLogProducer.Errors():
		//	log.Println("err: ", fail.Err)
		//}
	})
}

func (s *accessLogEntry) ensureEncode() {

	if s.encoded == nil && s.err == nil {
		s.encoded, s.err = json.Marshal(s)
	}
}
func (s *accessLogEntry) Length() int {
	s.ensureEncode()
	return len(s.encoded)
}
func (s *accessLogEntry) Encode() ([]byte, error) {
	s.ensureEncode()
	return s.encoded, s.err
}

//异步消息发送
func (self *Server) withAccessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		next.ServeHTTP(w, r)
		entry := &accessLogEntry{
			Method:       r.Method,
			Host:         r.Host,
			Path:         r.RequestURI,
			IP:           r.RemoteAddr,
			ResponseTime: float64(time.Since(started)) / float64(time.Second),
		}
		self.AccessLogProducer.Input() <- &sarama.ProducerMessage{
			Topic: "access_log",
			Key:   sarama.StringEncoder(r.RemoteAddr),
			Value: entry,
		}
		//消息的发送状态通过协程异步处理

	})

}

// AsyncProducer使用非阻塞API发布Kafka消息。 它将消息路由到提供的主题分区的正确代理，根据需要刷新元数据，并解析错误响应。 您必须从Errors（）通道读取或生成器将死锁。 您必须在生产者上调用Close（）或AsyncClose（）以避免泄漏：当它超出范围时，它不会自动被垃圾收集。
func newAccessLogProducer(borkerList []string) sarama.AsyncProducer {
	config := sarama.NewConfig()
	tlsConfig := createTlsConfiguration()
	if tlsConfig != nil {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
	}

	//sarama.NewManualPartitioner() //返回一个手动选择分区的分割器,也就是获取msg中指定的`partition`
	//sarama.NewRandomPartitioner() //通过随机函数随机获取一个分区号
	//sarama.NewRoundRobinPartitioner() //环形选择,也就是在所有分区中循环选择一个
	//sarama.NewHashPartitioner() //通过msg中的key生成hash值,选择分区,

	//config.Producer.Partitioner = sarama.NewManualPartitioner

	// sarama.WaitForAll 等待服务器所有副本都保存成功后的响应
	// sarama.WaitForLocal 等待仅本地提交成功后响应。
	// sarama.NoResponse 不想要

	//config.Producer.RequiredAcks = sarama.NoResponse

	//是否等待成功和失败后的响应,只有上面的RequireAcks设置不是NoReponse这里才有用.
	//config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Flush.Frequency = 500 * time.Millisecond
	producer, err := sarama.NewAsyncProducer(borkerList, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	//异步消息的返回时间不确定，所以通过协程异步处理
	go func() {
		for err := range producer.Errors() {
			log.Println("Failed to write access log entry:", err)
		}
	}()
	return producer

}

//并且确认消息时提供的实际持久性保证取决于`Producer.RequiredAcks`的配置值。
//  有些配置可能有时会丢失SyncProducer确认的消息。
//  出于实现原因，SyncProducer要求在其配置中将`Producer.Return.Errors`和`Producer.Return.Successes`设置为true。
func newDataCollector(brokerList []string) sarama.SyncProducer {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   //retry up to 10 times to product the message
	config.Producer.Return.Successes = true
	tlsConfig := createTlsConfiguration()
	if tlsConfig != nil {
		config.Net.TLS.Config = tlsConfig
		config.Net.TLS.Enable = true
	}

	log.Println(brokerList)
	log.Println(config)

	client, err := sarama.NewClient(brokerList, config)
	if err != nil {
		log.Fatalln(err)
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		log.Fatalln(err)
	}
	return producer
}

func createTlsConfiguration() (t *tls.Config) {
	if *clientCertFile != "" && *clientKeyFile != "" && *caFile != "" {

		log.Println(*clientCertFile, *clientKeyFile)
		cert, err := tls.LoadX509KeyPair(*clientCertFile, *clientKeyFile)
		if err != nil {
			log.Fatal(err)
		}
		caCert, err := ioutil.ReadFile(*caFile)
		if err != nil {
			log.Fatal(err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		t = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			InsecureSkipVerify: *verifySsl,
		}
		t.BuildNameToCertificate()
	}
	return
}
