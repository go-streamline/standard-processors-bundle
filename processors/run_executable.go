package processors

import (
	"fmt"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/sirupsen/logrus"
	"os/exec"
)

type RunExecutable struct {
	definitions.BaseProcessor
	config *runExecConfig
}

type runExecConfig struct {
	Executable string   `mapstructure:"executable"`
	Args       []string `mapstructure:"args"`
}

func NewRunExecutable() definitions.Processor {
	return &RunExecutable{}
}

func (r *RunExecutable) Close() error {
	return nil
}

func (r *RunExecutable) SetConfig(conf map[string]interface{}) error {
	r.config = &runExecConfig{}
	return r.DecodeMap(conf, r.config)
}

func (r *RunExecutable) Name() string {
	return "RunExecutable"
}

func (r *RunExecutable) Execute(
	info *definitions.EngineFlowObject,
	fileHandler definitions.ProcessorFileHandler,
	log *logrus.Logger,
) (*definitions.EngineFlowObject, error) {
	var err error
	// convert templated args to actual args
	parsedArgs := make([]string, len(r.config.Args))
	for i, arg := range r.config.Args {
		parsedArgs[i], err = info.EvaluateExpression(arg)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate expression for arg %s: %w", arg, err)
		}
	}

	log.Debugf("Converting file using executable: %s args: %v", r.config.Executable, parsedArgs)

	cmd := exec.Command(r.config.Executable, parsedArgs...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.WithError(err).Errorf("failed to run executable %s: %s", r.config.Executable, output)
		return nil, fmt.Errorf("failed to run executable %s: %w. Output: %s", r.config.Executable, err, output)
	}
	return info, nil
}
