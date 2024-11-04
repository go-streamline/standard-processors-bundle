package uploadhttp

import (
	"fmt"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/go-streamline/interfaces/utils"
	"github.com/sirupsen/logrus"
	"io"
	"mime/multipart"
	"net/http"
)

type UploadHTTP struct {
	definitions.BaseProcessor
	config *config
	client utils.HTTPClient
}

type sendFileType string

const (
	sendFileMultipart sendFileType = "multipart"
	sendFileBase64    sendFileType = "base64"
)

type config struct {
	URL                     string            `mapstructure:"url"`
	ExtraHeaders            map[string]string `mapstructure:"extra_headers,omitempty"`
	Type                    sendFileType      `mapstructure:"type"`
	PutResponseAsContents   bool              `mapstructure:"put_response_as_contents"`
	MultipartFieldName      string            `mapstructure:"multipart_field_name,omitempty"`
	MultipartFilename       string            `mapstructure:"multipart_filename,omitempty"`
	MultipartContentType    string            `mapstructure:"multipart_content_type,omitempty"`
	Base64BodyFormat        string            `mapstructure:"base64_body_format,omitempty"`
	WriteResponseToMetadata bool              `mapstructure:"write_response_to_metadata,omitempty"`
	UseStreaming            bool              `mapstructure:"use_streaming,omitempty"`
}

type bas64FormatTemplate struct {
	Base64Contents string
}

func CreateUploadHTTP() definitions.Processor {
	return &UploadHTTP{
		client: utils.NewHTTPClient(),
	}
}

func (h *UploadHTTP) SetConfig(conf map[string]interface{}) error {
	h.config = &config{}
	err := h.DecodeMap(conf, h.config)
	if err != nil {
		logrus.WithError(err).Errorf("failed to decode config")
		return fmt.Errorf("failed to decode config: %w", err)
	}
	if h.config.Type == "" {
		h.config.Type = sendFileMultipart
	}
	if h.config.MultipartFieldName == "" && h.config.Type == sendFileMultipart {
		return fmt.Errorf("multipart field name is required for multipart type")
	}
	if h.config.Base64BodyFormat == "" && h.config.Type == sendFileBase64 {
		return fmt.Errorf("base64 format is required for base64 type")
	}

	if h.config.ExtraHeaders == nil || len(h.config.ExtraHeaders) == 0 {
		h.config.ExtraHeaders = make(map[string]string)
	}

	if h.config.MultipartFilename == "" {
		h.config.MultipartFilename = h.config.MultipartFieldName
	}

	if h.config.MultipartContentType == "" {
		h.config.MultipartContentType = "application/octet-stream"
	}
	return nil
}

func (*UploadHTTP) Name() string {
	return "UploadHTTP"
}

func (h *UploadHTTP) formatBase64Content(base64Content string, info *definitions.EngineFlowObject) (string, error) {
	base64Format, err := info.EvaluateExpression(h.config.Base64BodyFormat)
	if err != nil {
		return "", fmt.Errorf("failed to evaluate base64 format: %w", err)
	}

	formattedContent, err := utils.ParseTemplate(base64Format, bas64FormatTemplate{Base64Contents: base64Content})
	if err != nil {
		return "", fmt.Errorf("failed to parse base64 format template: %w", err)
	}

	return formattedContent, nil
}

func (h *UploadHTTP) Close() error {
	return nil
}

func (h *UploadHTTP) Execute(
	info *definitions.EngineFlowObject,
	fileHandler definitions.ProcessorFileHandler,
	log *logrus.Logger,
) (*definitions.EngineFlowObject, error) {
	reader, err := fileHandler.Read()
	if err != nil {
		log.WithError(err).Errorf("failed to read file")
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	url := h.config.URL
	url, err = info.EvaluateExpression(url)
	if err != nil {
		log.WithError(err).Errorf("failed to evaluate URL")
		return nil, fmt.Errorf("failed to evaluate URL: %w", err)
	}

	var req *http.Request

	if h.config.UseStreaming {
		req, err = h.generateStreamingRequest(log, url, info, reader)
		if err != nil {
			return nil, err
		}
	} else {
		req, err = h.generateMemoryLoaderRequest(log, url, info, reader)
		if err != nil {
			return nil, err
		}
	}

	for key, value := range h.config.ExtraHeaders {
		key, err = info.EvaluateExpression(key)
		if err != nil {
			log.WithError(err).Errorf("failed to evaluate header key")
			return nil, fmt.Errorf("failed to evaluate header key: %w", err)
		}
		value, err = info.EvaluateExpression(value)
		if err != nil {
			log.WithError(err).Errorf("failed to evaluate header value")
			return nil, fmt.Errorf("failed to evaluate header value: %w", err)
		}
		req.Header.Set(key, value)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		log.WithError(err).Errorf("failed to send HTTP request")
		return nil, fmt.Errorf("failed to send HTTP request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		log.WithError(err).Errorf("failed to read response body")
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Errorf("received non-2xx response: %s, body: %s", resp.Status, string(respBody))
		return nil, fmt.Errorf("received non-2xx response: %s", resp.Status)
	}

	log.Debugf("Response status: %s", resp.Status)
	log.Debugf("Response body: %s", string(respBody))

	if h.config.PutResponseAsContents {
		writer, err := fileHandler.Write()
		if err != nil {
			log.WithError(err).Errorf("failed to write response to file")
			return nil, fmt.Errorf("failed to write response to file: %w", err)
		}
		_, err = writer.Write(respBody)
		if err != nil {
			log.WithError(err).Errorf("failed to write response to file")
			return nil, fmt.Errorf("failed to write response to file: %w", err)
		}
	}

	info.Metadata["UploadHTTP.ResponseStatusCode"] = resp.StatusCode
	if h.config.WriteResponseToMetadata {
		info.Metadata["UploadHTTP.ResponseBody"] = string(respBody)
		info.Metadata["UploadHTTP.ResponseHeaders"] = resp.Header
	}
	info.Metadata["UploadHTTP.URL"] = url
	return info, nil
}

func (h *UploadHTTP) generateMultipart(
	log *logrus.Logger,
	info *definitions.EngineFlowObject,
	writer *multipart.Writer,
	reader io.Reader,
) error {
	fieldName, err := evaluateAndLog(log, info, h.config.MultipartFieldName, "field name")
	if err != nil {
		return err
	}

	filename, err := evaluateAndLog(log, info, h.config.MultipartFilename, "filename")
	if err != nil {
		return err
	}

	_, err = createFormFile(log, writer, fieldName, filename, reader, h.config.MultipartContentType)
	if err != nil {
		return err
	}

	log.Debugf("closing writer")
	err = writer.Close()
	if err != nil {
		log.WithError(err).Errorf("failed to close writer")
		return fmt.Errorf("failed to close writer: %w", err)
	}

	return nil
}

func evaluateAndLog(
	log *logrus.Logger,
	info *definitions.EngineFlowObject,
	expression, name string,
) (string, error) {
	value, err := info.EvaluateExpression(expression)
	if err != nil {
		log.WithError(err).Errorf("failed to evaluate %s", name)
		return "", fmt.Errorf("failed to evaluate %s: %w", name, err)
	}
	log.Debugf("evaluated %s: %s", name, value)
	return value, nil
}

func createFormFile(
	log *logrus.Logger,
	writer *multipart.Writer,
	fieldName, filename string,
	reader io.Reader,
	contentType string,
) (io.Writer, error) {
	log.Debugf("creating form file: %s", filename)
	part, err := writer.CreatePart(map[string][]string{
		"Content-Disposition": {"form-data; name=\"" + fieldName + "\"; filename=\"" + filename + "\""},
		"Content-Type":        {contentType},
	})
	if err != nil {
		log.WithError(err).Errorf("failed to create form file")
		return nil, fmt.Errorf("failed to create form file: %w", err)
	}
	log.Debugf("copying file to form")
	_, err = io.Copy(part, reader)
	if err != nil {
		log.WithError(err).Errorf("failed to copy file to form")
		return nil, fmt.Errorf("failed to copy file to form: %w", err)
	}
	return part, nil
}
