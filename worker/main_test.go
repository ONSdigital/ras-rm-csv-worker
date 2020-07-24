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

func TestSubscribe(t *testing.T) {
	assert := assert.New(t)
	//create a fake Pub Sub serer
	ctx := context.Background()
	// Start a fake server running locally.
	srv := pstest.NewServer()
	defer srv.Close()
	// Connect to the server without using TLS.
	conn, err := grpc.Dial(srv.Addr, grpc.WithInsecure())
	assert.Nil(err)
	defer conn.Close()
	// Use the connection when creating a pubsub client.
	client, err := pubsub.NewClient(ctx, "rm-ras-sandbox", option.WithGRPCConn(conn))
	assert.Nil(err)
	defer client.Close()

	topic, err := client.CreateTopic(ctx, "test")
	assert.Nil(err)
	assert.NotNil(topic)
	fmt.Println(topic)

	sub, err := client.CreateSubscription(ctx, "sample-workers", pubsub.SubscriptionConfig{
		Topic: topic,
	})
	assert.Nil(err)
	assert.NotNil(sub)
	fmt.Println(sub)

	sample := "13110000001:::::::::::WW:::::OFFICE FOR NATIONAL STATISTICS:::::::::0001:"

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
	go worker.subscribe(ctx, client)

	//sleep a second for the test to complete, then allow everything to shut down
	time.Sleep(1 * time.Second)
}
