package kafka

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/sirupsen/logrus"
	"io"
	"strings"
)

type PublishKafka struct {
	definitions.BaseProcessor
	ctx      context.Context
	config   *publishKafkaConfig
	producer sarama.SyncProducer
}

type publishKafkaConfig struct {
	BootstrapServers string `mapstructure:"bootstrap_servers"` // comma separated list of brokers
	Topic            string `mapstructure:"topic"`
	Acks             string `mapstructure:"acks"` // all, none, or local
}

func NewPublishKafka() definitions.Processor {
	return &PublishKafka{
		ctx: context.Background(),
	}
}

func (p *PublishKafka) Name() string {
	return "PublishKafka"
}

func (p *PublishKafka) SetConfig(config map[string]interface{}) error {
	conf := &publishKafkaConfig{}
	err := p.DecodeMap(config, conf)
	if err != nil {
		logrus.WithError(err).Errorf("failed to decode config")
		return err
	}
	p.config = conf

	// Configure producer
	producerConfig := sarama.NewConfig()
	switch strings.ToLower(p.config.Acks) {
	case "all":
		producerConfig.Producer.RequiredAcks = sarama.WaitForAll
	case "none":
		producerConfig.Producer.RequiredAcks = sarama.NoResponse
	case "local":
		producerConfig.Producer.RequiredAcks = sarama.WaitForLocal
	default:
		return fmt.Errorf("invalid acks value: %s", p.config.Acks)
	}

	producerConfig.Producer.Return.Successes = true

	brokers := strings.Split(p.config.BootstrapServers, ",")
	p.producer, err = sarama.NewSyncProducer(brokers, producerConfig)
	if err != nil {
		logrus.WithError(err).Errorf("failed to create Kafka producer")
		return err
	}

	return nil
}

func (p *PublishKafka) Close() error {
	if p.producer != nil {
		return p.producer.Close()
	}
	return nil
}

func (p *PublishKafka) Execute(info *definitions.EngineFlowObject, fileHandler definitions.ProcessorFileHandler, log *logrus.Logger) (*definitions.EngineFlowObject, error) {
	log.Trace("starting PublishKafka execution")
	reader, err := fileHandler.Read()
	if err != nil {
		log.WithError(err).Errorf("failed to get reader for file handler")
		return nil, err
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		log.WithError(err).Errorf("failed to read data from file handler")
		return nil, err
	}

	message := &sarama.ProducerMessage{
		Topic: p.config.Topic,
		Value: sarama.ByteEncoder(data),
	}

	partition, offset, err := p.producer.SendMessage(message)
	if err != nil {
		log.WithError(err).Errorf("failed to publish message to topic %s", p.config.Topic)
		return nil, err
	}

	log.Infof("Message published to topic %s, partition %d, offset %d", p.config.Topic, partition, offset)
	log.Debug("completed PublishKafka execution")

	return &definitions.EngineFlowObject{
		Metadata: map[string]interface{}{
			"PublishKafka.Topic":     p.config.Topic,
			"PublishKafka.Partition": partition,
			"PublishKafka.Offset":    offset,
		},
	}, nil
}
