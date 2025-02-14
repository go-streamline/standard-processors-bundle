package pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/go-streamline/interfaces/utils"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/option"
)

type ConsumePubSub struct {
	definitions.BaseProcessor
	ctx          context.Context
	config       *consumePubSuConfig
	client       *pubsub.Client
	subscription *pubsub.Subscription
	messages     map[string]*pubsub.Message
}

type consumePubSuConfig struct {
	Credentials      string `mapstructure:"credentials"`
	Project          string `mapstructure:"project"`
	Topic            string `mapstructure:"topic"`
	SubscriptionName string `mapstructure:"subscription_name"`
	CreateTopic      bool   `mapstructure:"create_topic"`
	AckImmediately   bool   `mapstructure:"ack_immediately"` // whether to ack upon receiving the message or after getting a finishing session update
}

func NewConsumePubSub() definitions.TriggerProcessor {
	return &ConsumePubSub{
		ctx: context.Background(),
	}
}

func (c *ConsumePubSub) Name() string {
	return "ConsumePubSub"
}

func (c *ConsumePubSub) SetConfig(config map[string]interface{}) error {
	conf := &consumePubSuConfig{}
	err := c.DecodeMap(config, conf)
	if err != nil {
		logrus.WithError(err).Errorf("failed to decode config")
		return err
	}
	c.config = conf
	credentials, err := utils.EvaluateExpression(c.config.Credentials, nil)
	if err != nil {
		logrus.WithError(err).Errorf("failed to evaluate credentials expression")
		return err
	}
	client, err := pubsub.NewClient(c.ctx, c.config.Project, option.WithCredentialsJSON([]byte(credentials)))
	if err != nil {
		logrus.WithError(err).Errorf("failed to create Pub/Sub client")
		return err
	}
	c.client = client

	// Get or create topic
	topic := c.client.Topic(c.config.Topic)
	exists, err := topic.Exists(c.ctx)
	if err != nil {
		logrus.WithError(err).Errorf("failed to check if topic exists")
		return err
	}

	if !exists && c.config.CreateTopic {
		topic, err = c.client.CreateTopic(c.ctx, c.config.Topic)
		if err != nil {
			logrus.WithError(err).Errorf("failed to create topic")
			return err
		}
	} else if !exists {
		return fmt.Errorf("topic %s does not exist and create_topic is false", c.config.Topic)
	}

	c.subscription = c.client.Subscription(c.config.SubscriptionName)
	return nil
}

func (c *ConsumePubSub) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

func (c *ConsumePubSub) Execute(info *definitions.EngineFlowObject, produceFileHandler func() definitions.ProcessorFileHandler, log *logrus.Logger) ([]*definitions.TriggerProcessorResponse, error) {
	log.Trace("starting ConsumePubSub execution")
	var responses []*definitions.TriggerProcessorResponse

	err := c.subscription.Receive(c.ctx, func(ctx context.Context, msg *pubsub.Message) {
		log.Debugf("Message received: ID = %s, Data = %s", msg.ID, string(msg.Data))
		fileHandler := produceFileHandler()
		writer, err := fileHandler.Write()
		if err != nil {
			log.WithError(err).Errorf("failed to get writer for file handler")
			msg.Nack()
			return
		}

		_, err = writer.Write(msg.Data)
		if err != nil {
			log.WithError(err).Errorf("failed to write message data to file handler")
			msg.Nack()
			return
		}

		tpMark := uuid.New().String()
		metadata := map[string]interface{}{
			"ConsumePubSub.Topic":       c.config.Topic,
			"ConsumePubSub.MessageID":   msg.ID,
			"ConsumePubSub.PublishTime": msg.PublishTime,
		}
		// copy msg attributes to metadata
		for k, v := range msg.Attributes {
			metadata[fmt.Sprintf("ConsumePubSub.Attributes.%s", k)] = v
		}
		engineFlowObject := &definitions.EngineFlowObject{
			Metadata: metadata,
			TPMark:   tpMark,
		}

		responses = append(responses, &definitions.TriggerProcessorResponse{
			EngineFlowObject: engineFlowObject,
			FileHandler:      fileHandler,
		})

		if c.config.AckImmediately {
			msg.Ack()
		} else {
			c.messages[tpMark] = msg
		}
	})

	if err != nil {
		return nil, err
	}

	log.Debug("completed ConsumePubSub execution")
	return responses, nil
}

func (c *ConsumePubSub) GetScheduleType() definitions.ScheduleType {
	return definitions.EventDriven
}

func (c *ConsumePubSub) HandleSessionUpdate(update definitions.SessionUpdate) {
	if update.Finished {
		logrus.Infof("Session %s finished successfully", update.SessionID)
		if !c.config.AckImmediately {
			logrus.Infof("Acknowledging message for session %s", update.SessionID)
			msg, ok := c.messages[update.TPMark]
			if !ok {
				logrus.Errorf("Message not found for session %s", update.SessionID)
				return
			}
			if update.Error != nil {
				logrus.WithError(update.Error).Errorf("Session %s finished with error", update.SessionID)
				msg.Nack()
			} else {
				msg.Ack()
			}

			delete(c.messages, update.TPMark)
		}
	}
}
