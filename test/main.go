package main

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

/*
	Simple program that tests sending a message to a topic and setting up a web server to mimic the sample service api
	TODO - remove once complete
*/
func sendCSV() {
	sample := "13110000001:::::::::::WW:::::OFFICE FOR NATIONAL STATISTICS:::::::::0001:"
	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, getEnv("GOOGLE_CLOUD_PROJECT", "rm-ras-sandbox"))
	if err != nil {
		log.Fatal(err)
	}

	topicName := getEnv("PUBSUB_TOPIC", "sample-jobs")
	topic := client.Topic(topicName)

	for i := 0; i < 10; i++ {
		msg := &pubsub.Message{
			Data: []byte(sample),
		}
		id, err := topic.Publish(ctx, msg).Get(ctx)
		if err != nil {
			fmt.Printf("Error publishing sample")
			return
		}
		fmt.Printf("Published a sample; msg ID: %v\n", id)
	}
}

func getEnv(key string, defaultVar string) string {
	v := os.Getenv(key)
	if v == "" {
		fmt.Printf("%s environment variable not set.\n", key)
		return defaultVar
	}
	return v
}

func startWebServer() {

	http.HandleFunc("/samples", func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("FAIL"))
		}
		fmt.Printf("received request %v\n", string(body))
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("OK"))
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}

func main() {
	fmt.Println("Sending sample to topic")
	sendCSV()
	startWebServer()
}
