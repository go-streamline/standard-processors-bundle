package io

import (
	"github.com/go-streamline/interfaces/definitions"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"path/filepath"
)

type WriteFile struct {
	definitions.BaseProcessor
	config *writeFileHandlerConfig
}

type writeFileHandlerConfig struct {
	Output string `mapstructure:"output"`
}

func NewWriteFile() definitions.Processor {
	return &WriteFile{}
}

func (w *WriteFile) SetConfig(conf map[string]interface{}) error {
	w.config = &writeFileHandlerConfig{}
	return w.DecodeMap(conf, &w.config)
}

func (w *WriteFile) Name() string {
	return "WriteFile"
}

func (w *WriteFile) Close() error {
	return nil
}

func (w *WriteFile) Execute(
	info *definitions.EngineFlowObject,
	fileHandler definitions.ProcessorFileHandler,
	log *logrus.Logger,
) (*definitions.EngineFlowObject, error) {
	log.Trace("WriteFileHandler.Handle")
	log.Debugf("writing file to %s", w.config.Output)
	reader, err := fileHandler.Read()
	if err != nil {
		return nil, err

	}
	log.Debugf("evaluating expression %s", w.config.Output)
	outputPath, err := info.EvaluateExpression(w.config.Output)
	if err != nil {
		return nil, err
	}
	log.Debugf("evaluated expression to %s", outputPath)

	err = os.MkdirAll(filepath.Dir(outputPath), os.ModePerm)
	if err != nil {
		return nil, err
	}

	log.Debugf("creating file %s", outputPath)

	writer, err := os.Create(outputPath)
	if err != nil {
		return nil, err
	}
	defer writer.Close()

	log.Debugf("copying file to %s", outputPath)
	_, err = io.Copy(writer, reader)
	if err != nil {
		return nil, err
	}

	log.Debugf("setting metadata WriteFile.OutputPath to %s", outputPath)

	info.Metadata["WriteFile.OutputPath"] = outputPath

	return info, nil
}
