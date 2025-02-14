package pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/go-streamline/interfaces/utils"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/option"
	"io"
)

type PublishPubSub struct {
	definitions.BaseProcessor
	ctx    context.Context
	config *publishPubSubConfig
	client *pubsub.Client
	topic  *pubsub.Topic
}

type publishPubSubConfig struct {
	Credentials string `mapstructure:"credentials"`
	Project     string `mapstructure:"project"`
	Topic       string `mapstructure:"topic"`
	CreateTopic bool   `mapstructure:"create_topic"`
}

func NewPublishPubSub() definitions.Processor {
	return &PublishPubSub{
		ctx: context.Background(),
	}
}

func (p *PublishPubSub) Name() string {
	return "PublishPubSub"
}

func (p *PublishPubSub) SetConfig(config map[string]interface{}) error {
	conf := &publishPubSubConfig{}
	err := p.DecodeMap(config, conf)
	if err != nil {
		logrus.WithError(err).Errorf("failed to decode config")
		return err
	}
	p.config = conf
	credentials, err := utils.EvaluateExpression(p.config.Credentials, nil)
	if err != nil {
		logrus.WithError(err).Errorf("failed to evaluate credentials")
		return err
	}
	client, err := pubsub.NewClient(p.ctx, p.config.Project, option.WithCredentialsJSON([]byte(credentials)))
	if err != nil {
		logrus.WithError(err).Errorf("failed to create Pub/Sub client")
		return err
	}
	p.client = client

	// Get or create topic
	topic := p.client.Topic(p.config.Topic)
	exists, err := topic.Exists(p.ctx)
	if err != nil {
		logrus.WithError(err).Errorf("failed to check if topic exists")
		return err
	}

	if !exists && p.config.CreateTopic {
		topic, err = p.client.CreateTopic(p.ctx, p.config.Topic)
		if err != nil {
			logrus.WithError(err).Errorf("failed to create topic")
			return err
		}
	} else if !exists {
		return fmt.Errorf("topic %s does not exist and create_topic is false", p.config.Topic)
	}

	p.topic = topic
	return nil
}

func (p *PublishPubSub) Close() error {
	if p.client != nil {
		return p.client.Close()
	}
	return nil
}

func (p *PublishPubSub) Execute(info *definitions.EngineFlowObject, fileHandler definitions.ProcessorFileHandler, log *logrus.Logger) (*definitions.EngineFlowObject, error) {
	log.Trace("starting PublishPubSub execution")
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

	message := &pubsub.Message{
		Data: data,
	}

	result := p.topic.Publish(p.ctx, message)
	_, err = result.Get(p.ctx)
	if err != nil {
		log.WithError(err).Errorf("failed to publish message to topic %s", p.config.Topic)
		return nil, err
	}

	log.Infof("Message published to topic %s", p.config.Topic)
	log.Debug("completed PublishPubSub execution")

	return &definitions.EngineFlowObject{
		Metadata: map[string]interface{}{
			"PublishPubSub.Topic": p.config.Topic,
		},
	}, nil
}
