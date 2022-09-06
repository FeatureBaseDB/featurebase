package kinesis

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/featurebasedb/featurebase/v3/logger"
)

// ErrorType signifies the type of error encountered.
type ErrorType string

// These are the currently supported ErrorType values that can be emitted
// to an ErrorStore.
const (
	RecoverableErrorType = ErrorType("Error")
	PanicErrorType       = ErrorType("Panic") // Runtime errors.
)

// ErrorStore is an abstraction over a resource like external storage, a database,
// a queue, etc. that can receive and store error messages.
type ErrorStore interface {
	// Available checks if the backing resource can receive error
	// messages via Push.
	Available() bool

	// Push emits an error message of type ErrorType to the backing resource.
	//
	// The caller should NOT assume that Available is called implicitly
	// to check for ErrorStore availability.
	Push(ErrorType, string, logger.Logger) error
}

// SinkErrorPayload contains all data about a single IDK error message that will be
// emitted to an ErrorStore; includes a timestamp and a valid sink ID.
//
// This payload is not meant to be used outside the context of error propogation to
// an ErrorStore.
type SinkErrorPayload struct {
	SinkId       string    `json:"sink_id"`
	ErrorType    ErrorType `json:"error_type"`
	ErrorMessage string    `json:"error_msg"`
	Timestamp    string    `json:"time"`
}

// SinkErrorQueue is an ErrorStore implementation that uses an SQS queue as its backing
// resource to emit error and panic messages to.
//
// It also maps 1-to-1 to a Kinesis stream via a unique sink ID.
type SinkErrorQueue struct {
	sinkId string
	name   string
	url    string
	queue  sqsiface.SQSAPI
}

// NewSinkErrorQueue attempts to construct a SinkErrorQueue instance from an AWS SQS client,
// a queue name, and a sink ID.
//
// On success, callers can assume that a backing SQS queue resource exists and is fully initialized.
//
// Returns nil and an SQS error if an SQS queue URL cannot be resolved from the queue name and/or
// the AWS SQS client.
//
// This method assumes the sink ID argument is valid.
func NewSinkErrorQueue(queue sqsiface.SQSAPI, queueName, sinkId string) (*SinkErrorQueue, error) {
	input := &sqs.GetQueueUrlInput{QueueName: &queueName}
	output, err := queue.GetQueueUrl(input)
	if err != nil {
		return nil, err
	}

	return &SinkErrorQueue{sinkId, queueName, *output.QueueUrl, queue}, nil
}

// SinkErrorQueueFrom always constructs a SinkErrorQueue instance from an AWS SQS client and a
// kinesis.Source.
//
// Unlike NewSinkErrorQueue, this does NOT return an error if a queue URL cannot be resolved
// from the queue name and/or the AWS SQS client. Instead it will collapse to a SinkErrorQueue
// instance with a backing SQS resource that is ALWAYS unavailable. Attempting to invoke Push
// on this instance will not result in an error; instead it will just emit a warning that no
// backing SQS resource could be written to.
//
// This does check if the sink ID has a valid form: 'PREFIX'-VALID_UUID. If not, this collapses
// to a SinkErrorQueue instance that is ALWAYS unavailable.
func SinkErrorQueueFrom(queue sqsiface.SQSAPI, source *Source) *SinkErrorQueue {
	// The below failure conditions are handled by collapsing to a no-op ErrorStore.Push implementation.
	// - A missing SQS queue name.
	// - An invalid sink ID (i.e. not a valid UUID); valid sink ID: "PREFIX"-UUID.
	// - Unable to resolve queue URL from queue name.
	//
	// Means downstream ErrorStreamLogger behaves identical to its embedded Logger.
	if source.ErrorQueueName == "" {
		return &SinkErrorQueue{}
	}

	sinkId := strings.Join(strings.Split(source.StreamName, "-")[1:], "-")
	_, err := uuid.Parse(sinkId)
	if err != nil {
		// Keep invalid sink ID around in case something downstream wants to log.
		return &SinkErrorQueue{sinkId, source.ErrorQueueName, "", nil}
	}

	sinkErrorQueue, err := NewSinkErrorQueue(queue, source.ErrorQueueName, sinkId)
	if err != nil {
		return &SinkErrorQueue{sinkId, source.ErrorQueueName, "", nil}
	}

	return sinkErrorQueue
}

// Available checks that a valid SQS queue resource exists.
func (seq *SinkErrorQueue) Available() bool {
	return seq.url != "" && seq.queue != nil
}

// Push attempts to emit a single error message of type ErrorType to a SQS queue resource.
//
// If the backing SQS queue resource is not available, this function is a no-op and does NOT
// return an error. Instead it emits a warning to the logger.Logger instance specified by the log
// argument.
//
// The warning can be ignored entirely by passing a nil log argument.
func (seq *SinkErrorQueue) Push(errorType ErrorType, message string, log logger.Logger) error {
	if !seq.Available() {
		if log != nil {
			log.Warnf("Not pushing errors to an SQS queue='%+v' due to unavailability.", seq)
		}
		return nil
	}

	payload := SinkErrorPayload{
		SinkId:       seq.sinkId,
		ErrorType:    errorType,
		ErrorMessage: message,
		Timestamp:    time.Now().Format(time.RFC3339),
	}

	payloadBytes, err := json.Marshal(&payload)
	if err != nil {
		msgTemplate := "Unable to marshal payload='%+v' to send message='%s' to queue='%+v'."
		return errors.Wrap(err, fmt.Sprintf(msgTemplate, payload, message, seq.queue))
	}

	encodedPayload := base64.URLEncoding.EncodeToString(payloadBytes)
	input := &sqs.SendMessageInput{
		DelaySeconds: aws.Int64(10),
		MessageBody:  aws.String(encodedPayload),
		QueueUrl:     &seq.url,
	}

	_, err = seq.queue.SendMessage(input)
	if err != nil {
		msgTemplate := "Unable to send message='%s' to queue='%+v' with input='%+v'."
		return errors.Wrap(err, fmt.Sprintf(msgTemplate, message, seq.queue, input))
	}
	return nil
}

// ErrorStreamLogger is a logger.Logger implementation that decorates a base logger.Logger instance
// and emits error and panic messages to an ErrorStore.
//
// All other log levels delegate to the base logger.Logger implementation.
type ErrorStreamLogger struct {
	base  logger.Logger
	store ErrorStore
}

// NewErrorStreamLogger constructs an ErrorStreamLogger from a logger.Logger and an ErrorStore.
func NewErrorStreamLogger(base logger.Logger, store ErrorStore) *ErrorStreamLogger {
	return &ErrorStreamLogger{base, store}
}

// Printf just delegates to the wrapped/base logger.Logger's Printf implementation.
func (esl *ErrorStreamLogger) Printf(format string, v ...interface{}) {
	esl.base.Printf(format, v...)
}

// Debugf just delegates to the wrapped/base logger.Logger's Debugf implementation.
func (esl *ErrorStreamLogger) Debugf(format string, v ...interface{}) {
	esl.base.Debugf(format, v...)
}

// Infof just delegates to the wrapped/base logger.Logger's Infof implementation.
func (esl *ErrorStreamLogger) Infof(format string, v ...interface{}) {
	esl.base.Infof(format, v...)
}

// Warnf just delegates to the wrapped/base logger.Logger's Warnf implementation.
func (esl *ErrorStreamLogger) Warnf(format string, v ...interface{}) {
	esl.base.Warnf(format, v...)
}

// Errorf delegates to the wrapped/base logger.Logger's Errorf implementation and additionally/
// pushes an error message with RecoverableErrorType ErrorType to the ErrorStore.
//
// If an error occurs during a Push to the ErrorStore, the error is logged to the wrapped/base
// logger.Logger using Errorf.
func (esl *ErrorStreamLogger) Errorf(format string, v ...interface{}) {
	esl.base.Errorf(format, v...)

	err := esl.store.Push(RecoverableErrorType, fmt.Sprintf(format, v...), esl)
	if err != nil {
		errMsg := fmt.Sprintf("Failed during push to store='%+v'", esl.store)
		esl.base.Errorf(errors.Wrap(err, errMsg).Error())
	}
}

// Panicf delegates to the wrapped/base logger.Logger's Panicf implementation and additionally
// pushes an error message with PanicErrorType ErrorType to the ErrorStore.
//
// If an error occurs during a Push to the ErrorStore, the error is logged to the wrapped/base
// logger.Logger using Errorf NOT Panicf.
func (esl *ErrorStreamLogger) Panicf(format string, v ...interface{}) {
	esl.base.Panicf(format, v...)

	err := esl.store.Push(PanicErrorType, fmt.Sprintf(format, v...), esl)
	if err != nil {
		// A (recoverable) error occurred during a push to the store, so
		// log that error using `Errorf` not `Panicf`.
		errMsg := fmt.Sprintf("Failed during push to store='%+v'", esl.store)
		esl.base.Errorf(errors.Wrap(err, errMsg).Error())
	}
}
