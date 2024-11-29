package io

import (
	"github.com/go-streamline/interfaces/definitions"
	"github.com/sirupsen/logrus"
	"io"
	"maps"
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

func NewReadDir(stateManager definitions.StateManager) definitions.TriggerProcessor {
	return &ReadDir{
		stateManager: stateManager,
	}
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

func (r *ReadDir) listFiles(dir string, lastModifiedTime int64) ([]string, error) {
	var files []string

	// Define a walk function
	walkFn := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories unless recursive is enabled
		if info.IsDir() && path != dir {
			if !r.config.Recursive {
				return filepath.SkipDir
			}
			return nil
		}

		// Add file paths to the list
		if !info.IsDir() && info.ModTime().Unix() > lastModifiedTime {
			if r.config.RegexFilter != "" {
				matched, err := regexp.MatchString(r.config.RegexFilter, info.Name())
				if err != nil {
					return err
				}
				if !matched {
					return nil
				}
			}
			absPath, err := filepath.Abs(path)
			if err != nil {
				return err
			}
			files = append(files, absPath)
		}
		return nil
	}

	// Walk through the directory
	err := filepath.Walk(dir, walkFn)
	if err != nil {
		return nil, err
	}

	return files, nil
}

func (r *ReadDir) Execute(
	info *definitions.EngineFlowObject,
	produceFileHandler func() definitions.ProcessorFileHandler,
	log *logrus.Logger,
) ([]*definitions.TriggerProcessorResponse, error) {
	log.Trace("handling ReadDir")

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
		lastModifiedTime = int64(m["last_modified_time"].(float64))
	} else {
		lastModifiedTime = 0
	}

	log.Debugf("last modified time: %d", lastModifiedTime)

	files, err := r.listFiles(inputPath, lastModifiedTime)
	if err != nil {
		return nil, err
	}

	newModifiedTime := lastModifiedTime
	var responses []*definitions.TriggerProcessorResponse
	for _, filePath := range files {
		currentEngineFile := &definitions.EngineFlowObject{
			Metadata: maps.Clone(info.Metadata),
		}

		log.Debugf("processing file: %s", filePath)

		fileHandler := produceFileHandler()
		writer, err := fileHandler.Write()
		if err != nil {
			return nil, err
		}

		file, err := os.Open(filePath)
		if err != nil {
			return nil, err
		}
		defer file.Close()

		buf := make([]byte, 4096)
		for {
			n, err := file.Read(buf)
			if err != nil && err != io.EOF {
				return nil, err
			}
			if n == 0 {
				break
			}

			_, err = writer.Write(buf[:n])
			if err != nil {
				return nil, err
			}
		}

		if r.config.RemoveSource {
			err = os.Remove(filePath)
			if err != nil {
				return nil, err
			}
			log.Debugf("removed source file: %s", filePath)
		}
		currentEngineFile.Metadata["ReadDir.InputPath"] = inputPath
		currentEngineFile.Metadata["ReadDir.FilePath"] = filePath

		responses = append(responses, &definitions.TriggerProcessorResponse{
			EngineFlowObject: currentEngineFile,
			FileHandler:      fileHandler,
		})
	}

	m["last_modified_time"] = newModifiedTime
	err = r.stateManager.SetState(definitions.StateTypeLocal, m)
	if err != nil {
		return nil, err
	}

	log.Debug("completed ReadDir execution")
	return responses, nil
}
