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

var line = "13110000001:::::::::::WW:::::OFFICE FOR NATIONAL STATISTICS:::::::::0001:"

func TestSubscribe(t *testing.T) {

	//create a fake Pub Sub serer
	ctx := context.Background()
	// Start a fake server running locally.
	srv := pstest.NewServer()
	defer srv.Close()
	// Connect to the server without using TLS.
	conn, _ := grpc.Dial(srv.Addr, grpc.WithInsecure())

	defer conn.Close()
	// Use the connection when creating a pubsub client.
	client, _ := pubsub.NewClient(ctx, "rm-ras-sandbox", option.WithGRPCConn(conn))
	defer client.Close()

	assert := assert.New(t)
	configure()

	topic, err := createTopic(client, ctx, assert)
	defer topic.Delete(ctx)

	sub := createSubscription(client, ctx, err, topic, assert)
	defer sub.Delete(ctx)

	sampleJson := parseSample(err, assert)

	sampleServer := httptest.NewServer(http.HandlerFunc( func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		assert.Nil(err)
		assert.Equal(sampleJson, body)
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("{\"id\":\"1111\"}"))
	}))

	partyServer := httptest.NewServer(http.HandlerFunc( func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("{\"id\":\"1111\"}"))
	}))

	defer sampleServer.Close()
	defer partyServer.Close()

	fmt.Printf("Setting sample url %s", sampleServer.URL)
	err = os.Setenv("SAMPLE_SERVICE_BASE_URL", sampleServer.URL)
	fmt.Printf("Setting party url %s", partyServer.URL)
	err = os.Setenv("PARTY_SERVICE_BASE_URL", partyServer.URL)
	assert.Nil(err)

	msg := &pubsub.Message{
		Data: []byte(line),
		Attributes: map[string]string{
			"sample_summary_id": "test",
		},
		ID: "1",
	}

	id, err := topic.Publish(ctx, msg).Get(ctx)
	assert.Nil(err)
	fmt.Println(id)

	worker := CSVWorker{}
	go worker.subscribe(ctx, client)

	//sleep a second for the test to complete, then allow everything to shut down
	time.Sleep(1 * time.Second)

	messages := srv.Messages()
	//check there is a single message
	assert.Equal(len(messages), 1, "should have been a single message sent to the server")

	//and check it has been ack
	assert.Equal(1, messages[0].Acks)
}

func parseSample(err error, assert *assert.Assertions) []byte {
	sample, err := readSampleLine([]byte(line))
	s := create(sample)
	sampleJson, err := s.marshall()
	assert.Nil(err)
	return sampleJson
}

func createSubscription(client *pubsub.Client, ctx  context.Context, err error, topic *pubsub.Topic, assert *assert.Assertions) *pubsub.Subscription {
	sub, err := client.CreateSubscription(ctx, "sample-file", pubsub.SubscriptionConfig{
		Topic: topic,
	})
	assert.Nil(err)
	assert.NotNil(sub)
	fmt.Println(sub)
	return sub
}

func createTopic(client *pubsub.Client, ctx  context.Context, assert *assert.Assertions) (*pubsub.Topic, error) {
	topic, err := client.CreateTopic(ctx, "sample-file")
	assert.Nil(err)
	assert.NotNil(topic)
	fmt.Println(topic)
	return topic, err
}

func TestNoAckAsSampleSummaryIdMissing(t *testing.T) {

	//create a fake Pub Sub serer
	ctx := context.Background()
	// Start a fake server running locally.
	srv := pstest.NewServer()
	defer srv.Close()
	// Connect to the server without using TLS.
	conn, _ := grpc.Dial(srv.Addr, grpc.WithInsecure())

	defer conn.Close()
	// Use the connection when creating a pubsub client.
	client, _ := pubsub.NewClient(ctx, "rm-ras-sandbox", option.WithGRPCConn(conn))
	defer client.Close()

	assert := assert.New(t)

	topic, err := createTopic(client, ctx, assert)
	defer topic.Delete(ctx)

	sub := createSubscription(client, ctx, err, topic, assert)
	defer sub.Delete(ctx)

	msg := &pubsub.Message{
		Data: []byte(line),
		ID:   "1",
	}

	id, err := topic.Publish(ctx, msg).Get(ctx)
	assert.Nil(err)
	fmt.Println(id)

	worker := CSVWorker{}
	configure()
	go worker.subscribe(ctx, client)

	//sleep a second for the test to complete, then allow everything to shut down
	time.Sleep(1 * time.Second)

	messages:= srv.Messages()
	//check there is a single message
	assert.Equal(len(messages),  1, "should have been a single message sent to the server")

	//and check it hasn't been ack
	assert.Equal(0, messages[0].Acks)
}
