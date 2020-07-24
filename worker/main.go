package main

import (
	"cloud.google.com/go/pubsub"
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"os"
)

type CSVWorker struct {
	sample []byte
}

func init() {
	verbose := viper.GetBool("VERBOSE")
	if verbose {
		//anything debug and above
		log.SetLevel(log.DebugLevel)
	} else {
		//otherwise keep it to info
		log.SetLevel(log.InfoLevel)
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
		cw.sample = msg.Data
		err := processSample(cw.sample)
		if err != nil {
			log.WithError(err).Error("error processing sample - nacking message")
			msg.Nack()
		} else {
			log.Info("sample processed - acking message")
			msg.Ack()
		}
	})

	if err != nil {
		log.WithError(err).Error("error subscribing")
		cancel()
	}
}

func setDefaults() {
	viper.SetDefault("PUBSUB_SUB_ID", "sample-workers")
	viper.SetDefault("GOOGLE_CLOUD_PROJECT", "rm-ras-sandbox")
	viper.SetDefault("WORKERS", "10")
	viper.SetDefault("VERBOSE", true)
	viper.SetDefault("SAMPLE_SERVICE_BASE_URL", "http://localhost:8080")
	viper.SetDefault("SAMPLE_SERVICE_PATH", "/samples")
}

func main() {
	log.Info("starting")
	//config
	viper.AutomaticEnv()
	setDefaults()

	workers := viper.GetInt("WORKERS")
	for i := 0; i < workers; i++ {
		csvWorker := &CSVWorker{}
		go csvWorker.start()
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
