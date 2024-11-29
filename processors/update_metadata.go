package processors

import (
	"fmt"
	"github.com/expr-lang/expr"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/sirupsen/logrus"
)

type UpdateMetadata struct {
	definitions.BaseProcessor
	config       *map[string]any
	stateManager definitions.StateManager
	exprOptions  []expr.Option
}

func NewUpdateMetadata(stateManager definitions.StateManager) *UpdateMetadata {
	return &UpdateMetadata{
		stateManager: stateManager,
		exprOptions: []expr.Option{
			expr.Function("getState", func(params ...any) (any, error) {
				if len(params) != 1 {
					return nil, fmt.Errorf("getState requires 1 parameter")
				}
				return stateManager.GetState(definitions.StateType(params[0].(string)))
			}),
			expr.Function("setState", func(params ...any) (any, error) {
				if len(params) != 2 {
					return nil, fmt.Errorf("setState requires 2 parameters")
				}
				// check if value is map
				value := params[1]
				if valueMap, ok := value.(map[string]interface{}); ok {
					return nil, stateManager.SetState(definitions.StateType(params[0].(string)), valueMap)
				}

				return nil, fmt.Errorf("value must be a map")
			}),
		},
	}
}

func (p *UpdateMetadata) Name() string {
	return "UpdateMetadata"
}

func (p *UpdateMetadata) SetConfig(config map[string]interface{}) error {
	conf := &map[string]any{}
	err := p.DecodeMap(config, conf)
	if err != nil {
		logrus.WithError(err).Errorf("failed to decode config")
		return err
	}
	p.config = conf
	return nil
}

func (p *UpdateMetadata) Close() error {
	return nil
}

func (p *UpdateMetadata) Execute(info *definitions.EngineFlowObject, fileHandler definitions.ProcessorFileHandler, log *logrus.Logger) (*definitions.EngineFlowObject, error) {
	log.Trace("starting UpdateMetadata execution")

	var err error
	for k, v := range *p.config {
		info.Metadata[k], err = info.EvaluateExpression(fmt.Sprintf("%v", v), p.exprOptions...)
		if err != nil {
			return nil, err
		}
	}
	return info, nil
}
