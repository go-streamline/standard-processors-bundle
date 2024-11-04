package io

import (
	"github.com/go-streamline/interfaces/definitions"
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"regexp"
)

type ReadDir struct {
	definitions.BaseProcessor
	config       *readDirConfig
	stateManager definitions.StateManager
}

type readDirConfig struct {
	Input        string `mapstructure:"input"`
	RemoveSource bool   `mapstructure:"remove_source"`
	RegexFilter  string `mapstructure:"regex_filter"`
	Recursive    bool   `mapstructure:"recursive"`
}

func (r *ReadDir) GetScheduleType() definitions.ScheduleType {
	return definitions.CronDriven
}

func NewReadDir() definitions.TriggerProcessor {
	return &ReadDir{}
}

func (*ReadDir) HandleSessionUpdate(update definitions.SessionUpdate) {

}

func (r *ReadDir) SetConfig(conf map[string]interface{}) error {
	r.config = &readDirConfig{}
	return r.DecodeMap(conf, r.config)
}

func (r *ReadDir) Name() string {
	return "ReadDir"
}

func (r *ReadDir) Close() error {
	return nil
}

func (r *ReadDir) Execute(
	info *definitions.EngineFlowObject,
	fileHandler definitions.ProcessorFileHandler,
	log *logrus.Logger,
) ([]*definitions.EngineFlowObject, error) {
	log.Trace("handling ReadDir")
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

	m, err := r.stateManager.GetState(definitions.StateTypeLocal)
	if err != nil {
		return nil, err
	}

	var lastModifiedTime int64
	_, ok := m["last_modified_time"]
	if ok {
		lastModifiedTime = m["last_modified_time"].(int64)
	} else {
		lastModifiedTime = 0
	}

	log.Debugf("last modified time: %d", lastModifiedTime)

	files, err := r.readFiles(inputPath, lastModifiedTime)
	if err != nil {
		return nil, err
	}

	newModifiedTime := lastModifiedTime
	for _, file := range files {
		if file.ModTime().Unix() > newModifiedTime {
			newModifiedTime = file.ModTime().Unix()
		}

		if r.config.RegexFilter != "" {
			matched, err := regexp.MatchString(r.config.RegexFilter, file.Name())
			if err != nil {
				return nil, err
			}
			if !matched {
				continue
			}
		}

		filePath := filepath.Join(inputPath, file.Name())
		log.Debugf("processing file: %s", filePath)

		content, err := os.ReadFile(filePath)
		if err != nil {
			return nil, err
		}

		_, err = writer.Write(content)
		if err != nil {
			return nil, err
		}

		if r.config.RemoveSource {
			err = os.Remove(filePath)
			if err != nil {
				return nil, err
			}
			log.Debugf("removed source file: %s", filePath)
		}
	}

	m["last_modified_time"] = newModifiedTime
	err = r.stateManager.SetState(definitions.StateTypeLocal, m)
	if err != nil {
		return nil, err
	}

	log.Debug("completed ReadDir execution")
	return nil, nil
}

func (r *ReadDir) readFiles(inputPath string, lastModifiedTime int64) ([]os.FileInfo, error) {
	var files []os.FileInfo
	err := filepath.Walk(inputPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && info.ModTime().Unix() > lastModifiedTime {
			files = append(files, info)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return files, nil
}
