package main

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

var (
	sample = "13110000001:::::::::::WW:::::OFFICE FOR NATIONAL STATISTICS:::::::::0001:"
	client *pubsub.Client
	ctx    context.Context
)

func TestMain(m *testing.M) {

	//create a fake Pub Sub serer
	ctx = context.Background()
	// Start a fake server running locally.
	srv := pstest.NewServer()
	defer srv.Close()
	// Connect to the server without using TLS.
	conn, _ := grpc.Dial(srv.Addr, grpc.WithInsecure())

	defer conn.Close()
	// Use the connection when creating a pubsub client.
	client, _ = pubsub.NewClient(ctx, "rm-ras-sandbox", option.WithGRPCConn(conn))
	defer client.Close()

	os.Exit(m.Run())
}

func TestSubscribe(t *testing.T) {
	assert := assert.New(t)
	configure()

	topic, err := createTopic(assert)
	defer topic.Delete(ctx)

	dlqTopic := createTopicDLQ(err, assert, topic)
	defer dlqTopic.Delete(ctx)

	sub := createSubscription(err, topic, assert)
	defer sub.Delete(ctx)

	dlqTopicSub := createDLQSubscription(err, dlqTopic, assert, sub)
	defer dlqTopicSub.Delete(ctx)

	sampleJson := parseSample(err, assert)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		assert.Nil(err)
		assert.Equal(sampleJson, body)
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("OK"))
	}))

	defer ts.Close()

	fmt.Printf("Setting sample service base url %v", ts.URL)
	err = os.Setenv("SAMPLE_SERVICE_BASE_URL", ts.URL)
	assert.Nil(err)

	msg := &pubsub.Message{
		Data: []byte(sample),
		Attributes: map[string]string{
			"sample_summary_id": "test",
		},
	}

	id, err := topic.Publish(ctx, msg).Get(ctx)
	assert.Nil(err)
	fmt.Println(id)

	worker := CSVWorker{}
	configure()
	go worker.subscribe(ctx, client)

	var dlqMsgData []byte
	go dlqTopicSub.Receive(ctx, func(ctx context.Context, dlqMsg *pubsub.Message) {
		dlqMsgData = dlqMsg.Data
	})
	//sleep a second for the test to complete, then allow everything to shut down
	time.Sleep(1 * time.Second)

	//nothing should be on the DLW
	assert.Nil(dlqMsgData)
}

func parseSample(err error, assert *assert.Assertions) []byte {
	s := parse([]byte(sample))
	sampleJson, err := s.marshall()
	assert.Nil(err)
	return sampleJson
}

func createDLQSubscription(err error, dlqTopic *pubsub.Topic, assert *assert.Assertions, sub *pubsub.Subscription) *pubsub.Subscription {
	dlqTopicSub, err := client.CreateSubscription(ctx, "sample-jobs-dead-letter", pubsub.SubscriptionConfig{
		Topic: dlqTopic,
	})
	assert.Nil(err)
	assert.NotNil(sub)
	fmt.Println(sub)
	return dlqTopicSub
}

func createSubscription(err error, topic *pubsub.Topic, assert *assert.Assertions) *pubsub.Subscription {
	sub, err := client.CreateSubscription(ctx, "sample-workers", pubsub.SubscriptionConfig{
		Topic: topic,
	})
	assert.Nil(err)
	assert.NotNil(sub)
	fmt.Println(sub)
	return sub
}

func createTopicDLQ(err error, assert *assert.Assertions, topic *pubsub.Topic) *pubsub.Topic {
	dlqTopic, err := client.CreateTopic(ctx, "sample-jobs-dead-letter")
	assert.Nil(err)
	assert.NotNil(topic)
	fmt.Println(topic)
	return dlqTopic
}

func createTopic(assert *assert.Assertions) (*pubsub.Topic, error) {
	topic, err := client.CreateTopic(ctx, "sample-jobs")
	assert.Nil(err)
	assert.NotNil(topic)
	fmt.Println(topic)
	return topic, err
}

func TestDeadletterAsSampleSummaryIdMissing(t *testing.T) {
	assert := assert.New(t)

	topic, err := createTopic(assert)
	defer topic.Delete(ctx)

	dlqTopic := createTopicDLQ(err, assert, topic)
	defer dlqTopic.Delete(ctx)

	sub := createSubscription(err, topic, assert)
	defer sub.Delete(ctx)

	dlqTopicSub := createDLQSubscription(err, dlqTopic, assert, sub)
	defer dlqTopicSub.Delete(ctx)

	s := parse([]byte(sample))
	sampleJson, err := s.marshall()
	assert.Nil(err)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		assert.Nil(err)
		assert.Equal(sampleJson, body)
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("OK"))
	}))

	defer ts.Close()

	fmt.Printf("Setting sample service base url %v", ts.URL)
	err = os.Setenv("SAMPLE_SERVICE_BASE_URL", ts.URL)
	assert.Nil(err)

	msg := &pubsub.Message{
		Data: []byte(sample),
	}

	id, err := topic.Publish(ctx, msg).Get(ctx)
	assert.Nil(err)
	fmt.Println(id)

	worker := CSVWorker{}
	configure()
	go worker.subscribe(ctx, client)

	var dlqMsgData []byte
	go dlqTopicSub.Receive(ctx, func(ctx context.Context, dlqMsg *pubsub.Message) {
		assert.NotNil(dlqMsg)
		assert.Equal(msg.Data, dlqMsg.Data)
		dlqMsgData = dlqMsg.Data
	})
	//sleep a second for the test to complete, then allow everything to shut down
	time.Sleep(1 * time.Second)

	assert.NotNil(dlqMsgData)
}
