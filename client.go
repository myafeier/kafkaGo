package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"gopkg.in/Shopify/sarama.v1"
	"io/ioutil"
	"log"
	"os"
	"sync"
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

func init() {
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	if *verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama test]", log.LstdFlags|log.Lshortfile)
	}
}
func main() {

	config := sarama.NewConfig()
	tlsConfig := createTlsConfiguration()
	if tlsConfig == nil {
		log.Panicln("tlsConfig is nil")
	}
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = tlsConfig
	config.Producer.Compression = sarama.CompressionSnappy
	config.ClientID = "goConsumer"
	//接收失败通知
	config.Consumer.Return.Errors = true

	client, err := sarama.NewClient([]string{"localhost:9093"}, config)
	if err != nil {
		log.Panicln(err)
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Panicln(err)
	}
	offsetManager, err := sarama.NewOffsetManagerFromClient("", client)
	if err != nil {
		log.Panicln(err)
	}

	defer consumer.Close()
	defer offsetManager.Close()

	ConsumerLoop(consumer, offsetManager, "test")
}

func ConsumerLoop(consumer sarama.Consumer, offsetManager sarama.OffsetManager, topic string) {
	partitions, err := consumer.Partitions(topic)
	log.Println(partitions)
	if err != nil {
		log.Panicln(err)
	}

	signals := make(chan os.Signal, 1)

	var wg sync.WaitGroup
	for partition := range partitions {
		wg.Add(1)
		go func() {
			consumerPartition(consumer, offsetManager, int32(partition), signals)
			wg.Done()
		}()
	}
	wg.Wait()
}

func consumerPartition(consumer sarama.Consumer, offsetManager sarama.OffsetManager, partition int32, signal chan os.Signal) {

	log.Println("Receving on partition:", partition)
	//相当于订阅了该主题/分区的消息
	partitionConsumer, err := consumer.ConsumePartition("test", partition, sarama.OffsetOldest)

	if err != nil {
		log.Println(err)
		return
	}

	offsetPartitionManager, err := offsetManager.ManagePartition("", partition)
	if err != nil {
		log.Println(err)
		return
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Println(err)
		}
		if err := offsetPartitionManager.Close(); err != nil {
			log.Println(err)
		}
	}()
	consumed := 0
ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():

			//partitionConsumer.AsyncClose()
			log.Printf("Consumed message offset %d,Data:%s\n", msg.Offset, msg.Value)
			consumed++

			log.Println(offsetPartitionManager.NextOffset())

		case sig := <-signal:
			log.Println("capture signal:", sig)
			break ConsumerLoop
		}
	}

	log.Printf("Consumed %d \n", consumed)
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
