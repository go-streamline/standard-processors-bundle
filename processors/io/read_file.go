package io

import (
	"github.com/go-streamline/interfaces/definitions"
	"github.com/sirupsen/logrus"
	"io"
	"os"
)

type ReadFile struct {
	definitions.BaseProcessor
	config *readFileConfig
}

type readFileConfig struct {
	Input        string `mapstructure:"input"`
	RemoveSource bool   `mapstructure:"remove_source"`
}

func NewReadFile() definitions.Processor {
	return &ReadFile{}
}

func (r *ReadFile) SetConfig(conf map[string]interface{}) error {
	r.config = &readFileConfig{}
	return r.DecodeMap(conf, r.config)
}

func (r *ReadFile) Name() string {
	return "ReadFile"
}

func (r *ReadFile) Close() error {
	return nil
}

func (r *ReadFile) Execute(
	info *definitions.EngineFlowObject,
	fileHandler definitions.ProcessorFileHandler,
	log *logrus.Logger,
) (*definitions.EngineFlowObject, error) {
	log.Trace("handling ReadFile")
	writer, err := fileHandler.Write()
	if err != nil {
		return nil, err
	}

	log.Debugf("evaluating expression %s", r.config.Input)
	inputPath, err := info.EvaluateExpression(r.config.Input)
	if err != nil {
		return nil, err
	}
	log.Debugf("input path: %s", inputPath)

	reader, err := os.Open(inputPath)
	if err != nil {
		return nil, err
	}

	log.Debugf("copying %s to output", inputPath)
	_, err = io.Copy(writer, reader)
	if err != nil {
		return nil, err
	}

	err = reader.Close()
	if r.config.RemoveSource {
		if err != nil {
			log.WithError(err).Errorf("failed to close input file %s", inputPath)
			return nil, err
		}
		log.Debugf("removing source file %s", inputPath)
		err = os.Remove(inputPath)
		if err != nil {
			return nil, err
		}
	}
	log.Debugf("setting metadata ReadFile.Source to %s", inputPath)

	info.Metadata["ReadFile.Source"] = inputPath

	return info, nil
}
