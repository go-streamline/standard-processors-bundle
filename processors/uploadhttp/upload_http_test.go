package uploadhttp

import (
	"bytes"
	"github.com/go-streamline/interfaces/definitions"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"io"
	"net/http"
	"testing"
)

// MockHTTPClient is a mock implementation of HTTPClient for testing
type MockHTTPClient struct {
	mock.Mock
}

func (m *MockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	args := m.Called(req)
	return args.Get(0).(*http.Response), args.Error(1)
}

// MockEngineFileHandler is a mock implementation of EngineFileHandler for testing
type MockEngineFileHandler struct {
	reader io.Reader
	writer *bytes.Buffer
}

func (m *MockEngineFileHandler) Read() (io.Reader, error) {
	return m.reader, nil
}

func (m *MockEngineFileHandler) Write() (io.Writer, error) {
	return m.writer, nil
}

func (m *MockEngineFileHandler) Close() {

}

func TestSendHTTPHandler_Multipart_Non_Streaming(t *testing.T) {
	mockResp := &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewBufferString("mock response")),
		Header:     make(http.Header),
	}

	mockClient := new(MockHTTPClient)
	mockClient.On("Do", mock.AnythingOfType("*http.Request")).Return(mockResp, nil)

	mockFileHandler := &MockEngineFileHandler{
		reader: bytes.NewBufferString("mock file content"),
		writer: new(bytes.Buffer),
	}

	h := &UploadHTTP{
		client: mockClient,
	}
	err := h.SetConfig(map[string]interface{}{
		"url":                      "http://example.com/upload",
		"type":                     "multipart",
		"multipart_field_name":     "file",
		"put_response_as_contents": true,
	})
	assert.NoError(t, err)

	info := &definitions.EngineFlowObject{
		Metadata: map[string]interface{}{},
	}

	newInfo, err := h.Execute(info, mockFileHandler, logrus.New())
	assert.NoError(t, err)

	// Assertions
	mockClient.AssertExpectations(t)
	assert.Equal(t, 200, newInfo.Metadata["UploadHTTP.ResponseStatusCode"])
	assert.Equal(t, "http://example.com/upload", newInfo.Metadata["UploadHTTP.URL"])
	assert.Contains(t, mockFileHandler.writer.String(), "mock response")
}

func TestSendHTTPHandler_Base64_Non_Streaming(t *testing.T) {
	mockResp := &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewBufferString("mock response")),
		Header:     make(http.Header),
	}

	// Mock the HTTP client
	mockClient := new(MockHTTPClient)
	mockClient.On("Do", mock.AnythingOfType("*http.Request")).Return(mockResp, nil)

	// Mock file handler
	mockFileHandler := &MockEngineFileHandler{
		reader: bytes.NewBufferString("mock file content"),
		writer: new(bytes.Buffer),
	}

	h := &UploadHTTP{
		client: mockClient,
	}
	err := h.SetConfig(map[string]interface{}{
		"url":                      "http://example.com/upload",
		"type":                     "base64",
		"base64_body_format":       "data:text/plain;base64,{{.Base64Contents}}",
		"put_response_as_contents": true,
	})
	assert.NoError(t, err)

	info := &definitions.EngineFlowObject{
		Metadata: map[string]interface{}{},
	}

	// Run the handler
	newInfo, err := h.Execute(info, mockFileHandler, logrus.New())
	assert.NoError(t, err)

	// Assertions
	mockClient.AssertExpectations(t)
	assert.Equal(t, 200, newInfo.Metadata["UploadHTTP.ResponseStatusCode"])
	assert.Equal(t, "http://example.com/upload", newInfo.Metadata["UploadHTTP.URL"])
	assert.Contains(t, mockFileHandler.writer.String(), "mock response")
}

func TestSendHTTPHandler_Multipart_Streaming(t *testing.T) {
	mockResp := &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewBufferString("mock response")),
		Header:     make(http.Header),
	}

	mockClient := new(MockHTTPClient)
	mockClient.On("Do", mock.AnythingOfType("*http.Request")).Return(mockResp, nil)

	mockFileHandler := &MockEngineFileHandler{
		reader: bytes.NewBufferString("mock file content"),
		writer: new(bytes.Buffer),
	}

	h := &UploadHTTP{
		client: mockClient,
	}
	err := h.SetConfig(map[string]interface{}{
		"url":                      "http://example.com/upload",
		"type":                     "multipart",
		"multipart_field_name":     "file",
		"put_response_as_contents": true,
		"use_streaming":            true,
	})
	assert.NoError(t, err)

	info := &definitions.EngineFlowObject{
		Metadata: map[string]interface{}{},
	}

	newInfo, err := h.Execute(info, mockFileHandler, logrus.New())
	assert.NoError(t, err)

	// Assertions
	mockClient.AssertExpectations(t)
	assert.Equal(t, 200, newInfo.Metadata["UploadHTTP.ResponseStatusCode"])
	assert.Equal(t, "http://example.com/upload", newInfo.Metadata["UploadHTTP.URL"])
	assert.Contains(t, mockFileHandler.writer.String(), "mock response")
}

func TestSendHTTPHandler_Base64_Streaming(t *testing.T) {
	mockResp := &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewBufferString("mock response")),
		Header:     make(http.Header),
	}

	// Mock the HTTP client
	mockClient := new(MockHTTPClient)
	mockClient.On("Do", mock.AnythingOfType("*http.Request")).Return(mockResp, nil)

	// Mock file handler
	mockFileHandler := &MockEngineFileHandler{
		reader: bytes.NewBufferString("mock file content"),
		writer: new(bytes.Buffer),
	}

	h := &UploadHTTP{
		client: mockClient,
	}
	err := h.SetConfig(map[string]interface{}{
		"url":                      "http://example.com/upload",
		"type":                     "base64",
		"base64_body_format":       "data:text/plain;base64,{{.Base64Contents}}",
		"put_response_as_contents": true,
		"use_streaming":            true,
	})
	assert.NoError(t, err)

	info := &definitions.EngineFlowObject{
		Metadata: map[string]interface{}{},
	}

	// Run the handler
	newInfo, err := h.Execute(info, mockFileHandler, logrus.New())
	assert.NoError(t, err)

	// Assertions
	mockClient.AssertExpectations(t)
	assert.Equal(t, 200, newInfo.Metadata["UploadHTTP.ResponseStatusCode"])
	assert.Equal(t, "http://example.com/upload", newInfo.Metadata["UploadHTTP.URL"])
	assert.Contains(t, mockFileHandler.writer.String(), "mock response")
}

func TestSendHTTPHandler_Error(t *testing.T) {
	// Mock the HTTP client to simulate an error
	mockClient := new(MockHTTPClient)
	mockClient.On("Do", mock.AnythingOfType("*http.Request")).Return((*http.Response)(nil), assert.AnError)

	// Mock file handler
	mockFileHandler := &MockEngineFileHandler{
		reader: bytes.NewBufferString("mock file content"),
		writer: new(bytes.Buffer),
	}

	// Create the handler with the mock client
	h := &UploadHTTP{
		client: mockClient, // Inject the mock client
	}
	err := h.SetConfig(map[string]interface{}{
		"url":                      "http://example.com/upload",
		"type":                     "base64",
		"base64_body_format":       "data:text/plain;base64,{{.Base64Contents}}",
		"put_response_as_contents": true, // Ensure this is set
	})
	assert.NoError(t, err)

	info := &definitions.EngineFlowObject{
		Metadata: map[string]interface{}{},
	}

	// Run the handler and expect an error
	_, err = h.Execute(info, mockFileHandler, logrus.New())
	assert.Error(t, err)
	mockClient.AssertExpectations(t)
}
