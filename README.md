# Standard Processors Bundle
This bundle provides a basic set of processors & trigger processors to be used in go-streamline.

<!-- TOC -->
* [Standard Processors Bundle](#standard-processors-bundle)
  * [Processors](#processors)
    * [ReadFile](#readfile)
      * [Configuration](#configuration)
        * [Metadata](#metadata)
    * [WriteFile](#writefile)
      * [Configuration](#configuration-1)
        * [Metadata](#metadata-1)
    * [PublishKafka](#publishkafka)
      * [Configuration](#configuration-2)
        * [Metadata](#metadata-2)
    * [PublishPubSub](#publishpubsub)
      * [Configuration](#configuration-3)
        * [Metadata](#metadata-3)
    * [UploadHTTP](#uploadhttp)
      * [Configuration](#configuration-4)
        * [Metadata](#metadata-4)
    * [RunExecutable](#runexecutable)
      * [Configuration](#configuration-5)
        * [Metadata](#metadata-5)
    * [UpdateMetadata](#updatemetadata)
      * [Configuration](#configuration-6)
      * [Metadata](#metadata-6)
    * [Trigger Processors](#trigger-processors)
      * [ReadDir](#readdir)
      * [Configuration](#configuration-7)
        * [Metadata](#metadata-7)
      * [ConsumeKafka](#consumekafka)
      * [Configuration](#configuration-8)
        * [Metadata](#metadata-8)
      * [ConsumePubSub](#consumepubsub)
      * [Configuration](#configuration-9)
        * [Metadata](#metadata-9)
<!-- TOC -->

## Processors

### ReadFile
Reads a file from the filesystem and loads its contents into the flow file. 

#### Configuration
- `input` - (supports expr) - the absolute path to the file to be read.
- `remove_source` - if set to true, the source file will be removed after reading it. 

##### Metadata
This processor adds the following metadata to the flow file:
- `ReadFile.Source` - the absolute path to the file that was read.

### WriteFile
Writes the contents of the flow file to a file on the filesystem.

#### Configuration
- `output` - (supports expr) - the absolute path to the file to be written.

##### Metadata
This processor adds the following metadata to the flow file:
- `WriteFile.OutputPath` - the absolute path to the file that was written.

### PublishKafka
Publishes the contents of the flow file to a Kafka topic.

#### Configuration
- `bootstrap_servers` - the Kafka bootstrap servers.
- `topic` - the Kafka topic to publish to.
- `acks` - Required acks. can be `all`, `none`, or `local` that correspond to `sarama`'s `WaitForAll`, `NoResponse` and `WaitForLocal`.

##### Metadata
This processor adds the following metadata to the flow file:
- `PublishKafka.Topic` - the Kafka topic that was published to.
- `PublishKafka.Partition` - the Kafka partition that was published to.
- `PublishKafka.Offset` - the Kafka offset that was published to.

### PublishPubSub
Publishes the contents of the flow file to a Google Cloud Pub/Sub topic.

#### Configuration
- `credentials` - (supports expr without metadata) - the json credentials for the Google Cloud project.
- `project` - the Google Cloud project.
- `topic` - the Google Cloud Pub/Sub topic to publish to.
- `create_topic` - boolean. If set to true, the topic will be created if it does not exist.

##### Metadata
- `PublishPubSub.Topic` - the Google Cloud Pub/Sub topic that was published to.

### UploadHTTP
Uploads the contents of the flow file to an HTTP endpoint.

#### Configuration
- `url` - (supports expr) - the URL to upload the file to.
- `extra_headers` - (each value supports expr individually) - a map of extra headers to send with the request.
- `type` - Either `multipart` or `base64`. If `multipart`, the file will be uploaded as a multipart form. If `base64`, the fill will be sent as a base64 encoded string.
- `put_response_as_contents` - boolean. If set to true, the response body will be set as the contents of the flow file.
- `multipart_field_name` - (supports expr) - the name of the field to use when uploading the file as a multipart form.
- `multipart_filename` - (supports expr) - the name of the file to use when uploading the file as a multipart form.
- `multipart_content_type` - the content type of the file to use when uploading the file as a multipart form.
- `base64_body_format` - (supports expr) - the format of the base64 encoded body. It uses go templating to format the body so you can use the `{{ .Base64Contents }}` to access the contents of the flow file. For example: `{ "file": "{{ .Base64Contents }}" }`.
- `write_response_to_metadata` - boolean. If set to true, the response body will be written to the metadata of the flow file.
- `use_streaming` - boolean. If set to true, the file will be streamed to the server(hence the file will not be fully loaded into memory).

##### Metadata
- `UploadHTTP.ResponseStatusCode` - the status code of the response.
- `UploadHTTP.ResponseBody` - the body of the response(will be set only if `write_response_to_metadata` is set to true).
- `UploadHTTP.ResponseHeaders` - the headers of the response(will be set only if `write_response_to_metadata` is set to true).
- `UploadHTTP.URL` - the URL that was uploaded to.


### RunExecutable
Runs a command on the host machine.

#### Configuration
- `executable` - the path to the executable to run.
- `args` - (each value supports expr individually) - a list of arguments to pass to the executable.

##### Metadata
- `RunExecutable.Stdout` - the standard output of the command.

### UpdateMetadata
Updates the metadata of the flow file. Its expr also supports 2 additional functions:
- `getState` - `GetState(stateType StateType) (map[string]any, error)` from `StateManager` to get the state of the processor.
- `setState` - `SetState(stateType StateType, state map[string]any) error` from `StateManager` to set the state of the processor.

#### Configuration
- `metadata` - (each value supports expr individually) - a map of metadata keys and values to update.

#### Metadata
Each key-value pair in the `metadata` configuration will be added/override the flow file's metadata.

### Trigger Processors

#### ReadDir
Reads a directory and emits a flow file for each file in the directory.
It saves the latest modified time to state manager to avoid reprocessing the same files.

#### Configuration
- `input` - (supports expr) - the absolute path to the directory to read.
- `remove_source` - if set to true, the source file will be removed after reading it.
- `regex_filter` - a regex filter to apply to the files in the directory.
- `recursive` - boolean. If set to true, the directory will be read recursively.

##### Metadata
- `ReadDir.InputPath` - the absolute path to the directory that was read.
- `ReadDir.FilePath` - the absolute path to the file that was read.

#### ConsumeKafka
Consumes messages from a Kafka topic and emits a flow file for each message.

#### Configuration
- `bootstrap_servers` - the Kafka bootstrap servers.
- `consumer_group` - the Kafka consumer group.
- `topic_names` - a comma-separated list of Kafka topics to consume from.
- `kafka_version` - the Kafka version.
- `start_from_oldest` - boolean. If set to true, the consumer will start from the oldest message.

##### Metadata
- `ConsumeKafka.Topic` - the Kafka topic that was consumed from.
- `ConsumeKafka.Partition` - the Kafka partition that was consumed from.
- `ConsumeKafka.Offset` - the Kafka offset that was consumed from.


#### ConsumePubSub
Consumes messages from a Google Cloud Pub/Sub topic and emits a flow file for each message.

#### Configuration
- `credentials` - (supports expr without metadata) - the json credentials for the Google Cloud project.
- `project` - the Google Cloud project.
- `topic` - the Google Cloud Pub/Sub topic to consume from.
- `subscription_name` - the Google Cloud Pub/Sub subscription name.
- `create_topic` - boolean. If set to true, the topic will be created if it does not exist.
- `ack_immediately` - boolean. If set to true, the message will be acknowledged immediately. Otherwise, it will be acknowledged receiving a successful ending session update.

##### Metadata
- `ConsumePubSub.Topic` - the Google Cloud Pub/Sub topic that was consumed from.
- `ConsumePubSub.MessageID` - the Google Cloud Pub/Sub message ID.
- `ConsumePubSub.PublishTime` - the Google Cloud Pub/Sub message publish time.
- `ConsumePubSub.Attributes.<key>` - the Google Cloud Pub/Sub message attributes.