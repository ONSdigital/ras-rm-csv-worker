package main

import (
	"cloud.google.com/go/pubsub"
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"os"
)

type CSVWorker struct {
}

func configureLogging() {
	verbose := viper.GetBool("VERBOSE")
	if verbose {
		//anything debug and above
		log.SetLevel(log.DebugLevel)
	} else {
		//otherwise keep it to info
		//log.SetLevel(log.InfoLevel)
	}
}

func (cw CSVWorker) start() {
	log.Debug("starting worker process")
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, viper.GetString("GOOGLE_CLOUD_PROJECT"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	log.Debug("about to subscribe")
	cw.subscribe(ctx, client)
}

func (cw CSVWorker) subscribe(ctx context.Context, client *pubsub.Client) {
	subId := viper.GetString("PUBSUB_SUB_ID")
	log.WithField("subId", subId).Info("subscribing to subscription")
	sub := client.Subscription(subId)
	cctx, cancel := context.WithCancel(ctx)
	log.Debug("waiting to receive")
	err := sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		log.Info("sample received - processing")
		log.WithField("data", string(msg.Data)).Debug("sample data")

		if msg.DeliveryAttempt != nil {
			log.WithField("delivery attempts", *msg.DeliveryAttempt).Info("Message delivery attempted")
		}

		sample := msg.Data
		attribute := msg.Attributes
		sampleSummaryId, ok := attribute["sample_summary_id"]
		if ok  {
			log.WithField("sampleSummaryId", sampleSummaryId).Info("about to process sample")
			err := processSample(sample, sampleSummaryId)
			if err != nil {
				log.WithError(err).Error("error processing sample - nacking message")
				//after x number of nacks message will be DLQ
				msg.Nack()
			} else {
				log.Info("sample processed - acking message")
				msg.Ack()
			}
		} else {
			log.Error("missing sample summary id - sending to DLQ")
			deadLetter(ctx, client, msg)
		}
	})

	if err != nil {
		log.WithError(err).Error("error subscribing")
		cancel()
	}
}

// send message to DLQ immediately
func deadLetter(ctx context.Context, client *pubsub.Client,msg *pubsub.Message) {
	//DLQ are always named TOPIC + -dead-letter in our terraform scripts
	deadLetterTopic := viper.GetString("PUB_SUB_TOPIC") + "-dead-letter"
	dlq := client.Topic(deadLetterTopic)
	id, err := dlq.Publish(ctx, msg).Get(ctx)
	if err != nil {
		log.WithField("msg", string(msg.Data)).WithError(err).Error("unable to forward to dead letter topic")
		msg.Nack()
	}
	log.WithField("id", id).Info("published to dead letter topic")

}

func setDefaults() {

	viper.SetDefault("PUBSUB_SUB_ID", "sample-workers")
	viper.SetDefault("PUB_SUB_TOPIC", "sample-jobs")
	viper.SetDefault("GOOGLE_CLOUD_PROJECT", "rm-ras-sandbox")
	viper.SetDefault("WORKERS", "10")
	viper.SetDefault("VERBOSE", true)
	viper.SetDefault("SAMPLE_SERVICE_BASE_URL", "http://localhost:8080")
}

func work() {
	csvWorker := &CSVWorker{}
	csvWorker.start()
}

func configure() {
	//config
	viper.AutomaticEnv()
	setDefaults()
	configureLogging()
}

func main() {
	log.Info("starting")
	configure()

	workers := viper.GetInt("WORKERS")
	for i := 0; i < workers; i++ {
		go work()
	}

	signals := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	go func() {
		signal := <-signals
		log.WithField("signal", signal).Info("kill signal received")
		done <- true
	}()

	log.Info("started")
	<-done
	log.Info("exiting")
}
