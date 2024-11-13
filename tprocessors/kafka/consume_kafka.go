package kafka

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/sirupsen/logrus"
	"strings"
)

type ConsumeKafka struct {
	definitions.BaseProcessor
	config        *consumeKafkaConfig
	consumerGroup sarama.ConsumerGroup
	ctx           context.Context
	cancel        context.CancelFunc
}

type consumeKafkaConfig struct {
	TopicNames       string `mapstructure:"topic_names"`       // comma separated list of topics
	BootstrapServers string `mapstructure:"bootstrap_servers"` // comma separated list of brokers
	ConsumerGroup    string `mapstructure:"consumer_group"`
	Version          string `mapstructure:"kafka_version"`
	StartFromOldest  bool   `mapstructure:"start_from_oldest"`
}

func NewConsumeKafka() definitions.TriggerProcessor {
	c := &ConsumeKafka{}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	return c
}

type messageConsumer struct {
	ready              chan bool
	log                *logrus.Logger
	produceFileHandler func() definitions.ProcessorFileHandler
	info               *definitions.EngineFlowObject
	responses          []*definitions.TriggerProcessorResponse
}

func (consumer *messageConsumer) Setup(_ sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

func (consumer *messageConsumer) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *messageConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		consumer.log.Debugf("Message claimed: topic = %s, partition = %d, offset = %d", message.Topic, message.Partition, message.Offset)
		fileHandler := consumer.produceFileHandler()
		writer, err := fileHandler.Write()
		if err != nil {
			return err
		}

		_, err = writer.Write(message.Value)
		if err != nil {
			return err
		}

		session.MarkMessage(message, "")
		response := &definitions.TriggerProcessorResponse{
			EngineFlowObject: &definitions.EngineFlowObject{
				Metadata: map[string]interface{}{
					"ConsumeKafka.Topic":     message.Topic,
					"ConsumeKafka.Partition": message.Partition,
					"ConsumeKafka.Offset":    message.Offset,
				},
			},
			FileHandler: fileHandler,
		}
		consumer.responses = append(consumer.responses, response)
	}
	return nil
}

func (c *ConsumeKafka) Name() string {
	return "ConsumeKafka"
}

func (c *ConsumeKafka) GetScheduleType() definitions.ScheduleType {
	return definitions.EventDriven
}

func (c *ConsumeKafka) SetConfig(conf map[string]interface{}) error {
	c.config = &consumeKafkaConfig{}
	err := c.DecodeMap(conf, c.config)
	if err != nil {
		return err
	}
	if c.config.ConsumerGroup == "" {
		return fmt.Errorf("consumer group is required")
	}

	if c.config.BootstrapServers == "" {
		return fmt.Errorf("bootstrap servers are required")
	}

	if c.config.Version == "" {
		logrus.Warn("kafka version not found, will use default")
		c.config.Version = sarama.DefaultVersion.String()
	}

	consumerConfig := sarama.NewConfig()
	consumerConfig.Version, err = sarama.ParseKafkaVersion(c.config.Version)
	if err != nil {
		return err
	}

	if c.config.StartFromOldest {
		consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		consumerConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	c.consumerGroup, err = sarama.NewConsumerGroup(strings.Split(c.config.BootstrapServers, ","), c.config.ConsumerGroup, consumerConfig)
	if err != nil {
		return err
	}

	return err
}

func (c *ConsumeKafka) Execute(
	info *definitions.EngineFlowObject,
	produceFileHandler func() definitions.ProcessorFileHandler,
	log *logrus.Logger,
) ([]*definitions.TriggerProcessorResponse, error) {
	log.Trace("ConsumeKafka.Execute")
	if c.ctx == nil {
		c.ctx, c.cancel = context.WithCancel(context.Background())
	}
	topics := strings.Split(c.config.TopicNames, ",")

	consumer := &messageConsumer{
		ready:              make(chan bool),
		log:                log,
		produceFileHandler: produceFileHandler,
		info:               info,
	}

	if err := c.consumerGroup.Consume(c.ctx, topics, consumer); err != nil {
		return nil, err
	}

	return consumer.responses, nil
}

func (c *ConsumeKafka) HandleSessionUpdate(update definitions.SessionUpdate) {
	panic("implement me")
}

func (c *ConsumeKafka) Close() error {
	c.cancel()
	c.ctx = nil
	return c.consumerGroup.Close()
}
