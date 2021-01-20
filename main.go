package main

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/blendle/zapdriver"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var logger *zap.Logger

type CSVWorker struct{}

func configureLogging() {
	var err error
	verbose := viper.GetBool("VERBOSE")

	if verbose {
		config := zapdriver.NewProductionConfig()
		config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
		// disable sampling to ensure we get all log messages
		config.Sampling = nil
		logger, err = config.Build()
		if err != nil {
			panic(err)
		}
	} else {
		logger, err = zapdriver.NewProduction()
		if err != nil {
			panic(err)
		}
	}
}

func (cw CSVWorker) start() {
	logger.Debug("starting worker process")
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, viper.GetString("GOOGLE_CLOUD_PROJECT"))
	if err != nil {
		logger.Fatal("failed to create client", zap.Error(err))
	}
	defer client.Close()
	logger.Debug("about to subscribe")
	cw.subscribe(ctx, client)
}

func (cw CSVWorker) subscribe(ctx context.Context, client *pubsub.Client) {
	subId := viper.GetString("PUBSUB_SUB_ID")
	logger.Info("subscribing to subscription", zap.String("subId", subId))
	sub := client.Subscription(subId)
	cctx, cancel := context.WithCancel(ctx)
	logger.Debug("waiting to receive")
	err := sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		logger.Info("sample received - processing", zap.String("messageId", msg.ID))
		logger.Debug("sample data", zap.String("data", string(msg.Data)))

		if msg.DeliveryAttempt != nil {
			logger.Info("Message delivery attempted", zap.Int("delivery attempts", *msg.DeliveryAttempt))
		}

		sample := msg.Data
		attribute := msg.Attributes
		sampleSummaryId, ok := attribute["sample_summary_id"]
		if ok {
			logger.Info("about to process sample", zap.String("sampleSummaryId", sampleSummaryId))
			err := processSample(sample, sampleSummaryId, msg)
			if err != nil {
				logger.Error("error processing sample - nacking message", zap.Error(err))
				//after x number of nacks message will be DLQ
				msg.Nack()
			} else {
				logger.Info("sample processed - acking message")
				msg.Ack()
			}
		} else {
			logger.Error("missing sample summary id - sending to DLQ")
			err := deadLetter(ctx, client, msg)
			if err != nil {
				msg.Nack()
			}
		}
	})

	if err != nil {
		logger.Error("error subscribing")
		cancel()
	}
}

// send message to DLQ immediately
func deadLetter(ctx context.Context, client *pubsub.Client, msg *pubsub.Message) error {
	//DLQ are always named TOPIC + -dead-letter in our terraform scripts
	deadLetterTopic := viper.GetString("PUB_SUB_TOPIC") + "-dead-letter"
	dlq := client.Topic(deadLetterTopic)
	id, err := dlq.Publish(ctx, msg).Get(ctx)
	if err != nil {
		logger.Error("unable to forward to dead letter topic",
			zap.String("msg", string(msg.Data)),
			zap.Error(err))
		return err
	}
	logger.Info("published to dead letter topic", zap.String("id", id))
	return nil
}

func setDefaults() {
	viper.SetDefault("PUBSUB_SUB_ID", "sample-workers")
	viper.SetDefault("PUB_SUB_TOPIC", "sample-jobs")
	viper.SetDefault("GOOGLE_CLOUD_PROJECT", "rm-ras-sandbox")
	viper.SetDefault("VERBOSE", true)
	viper.SetDefault("SAMPLE_SERVICE_BASE_URL", "http://localhost:8080")
}

func work() {
	csvWorker := &CSVWorker{}
	logger.Info("started")
	csvWorker.start()
}

func configure() {
	//config
	viper.AutomaticEnv()
	setDefaults()
	configureLogging()
}

func main() {
	configure()
	logger.Info("starting")
	work()
	logger.Info("exiting")
}
