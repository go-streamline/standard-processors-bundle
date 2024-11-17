package standard_processors_bundle

import (
	"fmt"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/go-streamline/standard-processors-bundle/processors"
	"github.com/go-streamline/standard-processors-bundle/processors/io"
	"github.com/go-streamline/standard-processors-bundle/processors/pubsub"
	"github.com/go-streamline/standard-processors-bundle/processors/uploadhttp"
	tio "github.com/go-streamline/standard-processors-bundle/tprocessors/io"
	tkafka "github.com/go-streamline/standard-processors-bundle/tprocessors/kafka"
	tpubsub "github.com/go-streamline/standard-processors-bundle/tprocessors/pubsub"
	"github.com/google/uuid"
)

var (
	ErrUnsupportedProcessorType = fmt.Errorf("unsupported processor type")
)

type Factory struct {
	stateManagerFactory definitions.StateManagerFactory
}

func Create(stateManagerFactory definitions.StateManagerFactory) definitions.ProcessorFactory {
	return &Factory{
		stateManagerFactory: stateManagerFactory,
	}
}

func (f *Factory) GetProcessor(id uuid.UUID, typeName string) (definitions.Processor, error) {
	switch typeName {
	case (&io.ReadFile{}).Name():
		return io.NewReadFile(), nil
	case (&io.WriteFile{}).Name():
		return io.NewWriteFile(), nil
	case (&uploadhttp.UploadHTTP{}).Name():
		return uploadhttp.NewUploadHTTP(), nil
	case (&processors.RunExecutable{}).Name():
		return processors.NewRunExecutable(), nil
	case (&pubsub.PublishPubSub{}).Name():
		return pubsub.NewPublishPubSub(), nil
	case (&processors.UpdateMetadata{}).Name():
		return processors.NewUpdateMetadata(f.stateManagerFactory.CreateStateManager(id)), nil
	default:
		return nil, ErrUnsupportedProcessorType
	}
}

func (f *Factory) GetTriggerProcessor(id uuid.UUID, typeName string) (definitions.TriggerProcessor, error) {
	switch typeName {
	case (&tio.ReadDir{}).Name():
		return tio.NewReadDir(f.stateManagerFactory.CreateStateManager(id)), nil
	case (&tkafka.ConsumeKafka{}).Name():
		return tkafka.NewConsumeKafka(), nil
	case (&tpubsub.ConsumePubSub{}).Name():
		return tpubsub.NewConsumePubSub(), nil
	default:
		return nil, ErrUnsupportedProcessorType
	}
}
