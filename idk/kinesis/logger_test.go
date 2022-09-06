package kinesis

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/featurebasedb/featurebase/v3/idk/idktest/mocks"
	"github.com/featurebasedb/featurebase/v3/logger"
)

// In-memory featurebase/logger.Logger implementation that stores by level
// {"PRINTF", "DEBUG", "INFO", "WARN", "ERROR", "PANIC"} useful for unit tests.
type MapStashLogger struct {
	messages map[string][]string
}

func NewMapStashLogger() *MapStashLogger {
	messages := make(map[string][]string)
	messages["PRINTF"] = []string{}
	messages["DEBUG"] = []string{}
	messages["INFO"] = []string{}
	messages["WARN"] = []string{}
	messages["ERROR"] = []string{}
	messages["PANIC"] = []string{}
	return &MapStashLogger{messages}
}

func (msl *MapStashLogger) Printf(format string, v ...interface{}) {
	msl.messages["PRINTF"] = append(msl.messages["PRINTF"], fmt.Sprintf(format, v...))
}

func (msl *MapStashLogger) Debugf(format string, v ...interface{}) {
	msl.messages["DEBUG"] = append(msl.messages["DEBUG"], fmt.Sprintf(format, v...))
}

func (msl *MapStashLogger) Infof(format string, v ...interface{}) {
	msl.messages["INFO"] = append(msl.messages["INFO"], fmt.Sprintf(format, v...))
}

func (msl *MapStashLogger) Warnf(format string, v ...interface{}) {
	msl.messages["WARN"] = append(msl.messages["WARN"], fmt.Sprintf(format, v...))
}

func (msl *MapStashLogger) Errorf(format string, v ...interface{}) {
	msl.messages["ERROR"] = append(msl.messages["ERROR"], fmt.Sprintf(format, v...))
}

func (msl *MapStashLogger) Panicf(format string, v ...interface{}) {
	msl.messages["PANIC"] = append(msl.messages["PANIC"], fmt.Sprintf(format, v...))
}

// In-memory error store implementation to use in unit tests.
type MapStashErrorStore struct {
	storage        map[ErrorType][]string
	available      bool
	simulatedError error
}

func NewMapStashErrorStore(available bool, err error) *MapStashErrorStore {
	return &MapStashErrorStore{make(map[ErrorType][]string), available, err}
}

func (mses *MapStashErrorStore) Available() bool {
	return mses.available
}

func (mses *MapStashErrorStore) Push(errorType ErrorType, message string, log logger.Logger) error {
	if !mses.Available() {
		return nil
	}

	if mses.simulatedError != nil {
		return mses.simulatedError
	}

	mses.storage[errorType] = append(mses.storage[errorType], message)
	return nil
}

func TestNewSinkErrorQueueSuccess(t *testing.T) {
	mockSQS := &mocks.SQSAPI{}

	mockGetQueueUrlFn := func(input *sqs.GetQueueUrlInput) *sqs.GetQueueUrlOutput {
		name := *input.QueueName
		url := fmt.Sprintf("https://unit-test.queue.%s.url", name)
		return &sqs.GetQueueUrlOutput{QueueUrl: aws.String(url)}
	}

	mockSQS.On("GetQueueUrl", mock.MatchedBy(func(input *sqs.GetQueueUrlInput) bool {
		return input.QueueName != nil && *input.QueueName != ""
	})).Return(mockGetQueueUrlFn, nil)

	queue, err := NewSinkErrorQueue(mockSQS, "dummy-123", "a-b-c")

	assert.Nil(t, err)
	assert.Equal(t, "a-b-c", queue.sinkId)
	assert.Equal(t, "dummy-123", queue.name)
	assert.Equal(t, "https://unit-test.queue.dummy-123.url", queue.url)
	assert.Same(t, mockSQS, queue.queue)
}

func TestNewSinkErrorQueueFailSQSGetQueueUrlErrored(t *testing.T) {
	mockSQS := &mocks.SQSAPI{}

	mockGetQueueUrlFn := func(input *sqs.GetQueueUrlInput) *sqs.GetQueueUrlOutput {
		// Value ignored by the caller on failure.
		return &sqs.GetQueueUrlOutput{}
	}

	errMsg := "Could not retrieve queue URL."
	mockSQS.On("GetQueueUrl", mock.MatchedBy(func(input *sqs.GetQueueUrlInput) bool {
		return true
	})).Return(mockGetQueueUrlFn, errors.New(errMsg))

	queue, err := NewSinkErrorQueue(mockSQS, "dummy-123", "a-b-c")

	assert.Nil(t, queue)
	assert.NotNil(t, err)
	assert.Equal(t, errMsg, err.Error())
}

func TestSinkErrorQueueFromSuccess(t *testing.T) {
	mockSQS := &mocks.SQSAPI{}

	mockGetQueueUrlFn := func(input *sqs.GetQueueUrlInput) *sqs.GetQueueUrlOutput {
		name := *input.QueueName
		url := fmt.Sprintf("https://unit-test.queue.%s.url", name)
		return &sqs.GetQueueUrlOutput{QueueUrl: aws.String(url)}
	}

	mockSQS.On("GetQueueUrl", mock.MatchedBy(func(input *sqs.GetQueueUrlInput) bool {
		return input.QueueName != nil && *input.QueueName != ""
	})).Return(mockGetQueueUrlFn, nil)

	validUuid := "0f2af1d9-52db-4a1c-bc75-554d84b01850"
	streamName := fmt.Sprintf("sink-%s", validUuid) // Structure mimics how cloud ECS instance names Kinesis stream.
	source := &Source{
		ErrorQueueName: "my-queue-01234",
		StreamName:     streamName,
	}

	var queue *SinkErrorQueue
	queue = SinkErrorQueueFrom(mockSQS, source)

	assert.Equal(t, validUuid, queue.sinkId)
	assert.Equal(t, "my-queue-01234", queue.name)
	assert.Equal(t, "https://unit-test.queue.my-queue-01234.url", queue.url)
	assert.Same(t, mockSQS, queue.queue)
}

func TestSinkErrorQueueFromFailMissingQueueName(t *testing.T) {
	// Missing queue name on `Source` object creates a failure condition before SQS instance is even inspected.
	mockSQS := &mocks.SQSAPI{}

	validUuid := "0f2af1d9-52db-4a1c-bc75-554d84b01850"
	streamName := fmt.Sprintf("sink-%s", validUuid) // Structure mimics how cloud ECS instance names its Kinesis stream.
	source := &Source{StreamName: streamName}

	var queue *SinkErrorQueue
	queue = SinkErrorQueueFrom(mockSQS, source)

	assert.Equal(t, "", queue.sinkId)
	assert.Equal(t, "", queue.name)
	assert.Equal(t, "", queue.url)
	assert.Nil(t, queue.queue)
}

func TestSinkErrorQueueFromFailMalformedSinkId(t *testing.T) {
	mockSQS := &mocks.SQSAPI{}

	mockGetQueueUrlFn := func(input *sqs.GetQueueUrlInput) *sqs.GetQueueUrlOutput {
		// Value ignored by the caller on failure.
		return &sqs.GetQueueUrlOutput{}
	}

	mockSQS.On("GetQueueUrl", mock.MatchedBy(func(input *sqs.GetQueueUrlInput) bool {
		return true
	})).Return(mockGetQueueUrlFn, nil)

	malformedUuid := "28hsdfas636bd"
	streamName := fmt.Sprintf("sink-%s", malformedUuid)
	source := &Source{
		ErrorQueueName: "valid-queue-90123",
		StreamName:     streamName,
	}

	var queue *SinkErrorQueue
	queue = SinkErrorQueueFrom(mockSQS, source)

	assert.Equal(t, malformedUuid, queue.sinkId) // Keeps invalid sink ID around for downstream logging purposes.
	assert.Equal(t, "valid-queue-90123", queue.name)
	assert.Equal(t, "", queue.url) // Did not reach a point where `sqs.GetQueueUrl` is even called.
	assert.Nil(t, queue.queue)
}

func TestSinkErrorQueueFromFailEmptyStreamNameDoesNotCausePanic(t *testing.T) {
	mockSQS := &mocks.SQSAPI{}

	mockGetQueueUrlFn := func(input *sqs.GetQueueUrlInput) *sqs.GetQueueUrlOutput {
		// Value ignored by the caller on failure.
		return &sqs.GetQueueUrlOutput{}
	}

	mockSQS.On("GetQueueUrl", mock.MatchedBy(func(input *sqs.GetQueueUrlInput) bool {
		return true
	})).Return(mockGetQueueUrlFn, nil)

	source := &Source{
		ErrorQueueName: "valid-queue-88888888888",
		StreamName:     "",
	}

	var queue *SinkErrorQueue
	queue = SinkErrorQueueFrom(mockSQS, source)

	assert.Equal(t, "", queue.sinkId) // Keeps invalid sink ID around for downstream logging purposes.
	assert.Equal(t, "valid-queue-88888888888", queue.name)
	assert.Equal(t, "", queue.url) // Did not reach a point where `sqs.GetQueueUrl` is even called.
	assert.Nil(t, queue.queue)
}

func TestSinkErrorQueueFromFailSQSGetQueueUrlErrored(t *testing.T) {
	mockSQS := &mocks.SQSAPI{}

	mockGetQueueUrlFn := func(input *sqs.GetQueueUrlInput) *sqs.GetQueueUrlOutput {
		// Value ignored by the caller on failure.
		return &sqs.GetQueueUrlOutput{}
	}

	errMsg := "This error message isn't propagated but is handled within the method."
	mockSQS.On("GetQueueUrl", mock.MatchedBy(func(input *sqs.GetQueueUrlInput) bool {
		return true
	})).Return(mockGetQueueUrlFn, errors.New(errMsg))

	validUuid := "fa6f1631-7c29-4023-95ac-48a2f59fae9b"
	streamName := fmt.Sprintf("sink-%s", validUuid) // Structure mimics how cloud ECS instance names Kinesis stream.
	source := &Source{
		ErrorQueueName: "queue-that-we-stole-962463",
		StreamName:     streamName,
	}

	var queue *SinkErrorQueue
	queue = SinkErrorQueueFrom(mockSQS, source)

	assert.Equal(t, validUuid, queue.sinkId) // Keeps valid sink ID around for downstream logging purposes.
	assert.Equal(t, "queue-that-we-stole-962463", queue.name)
	assert.Equal(t, "", queue.url) // Could not resolve queue URL, so this remains empty.
	assert.Nil(t, queue.queue)
}

func TestSinkErrorQueueAvailable(t *testing.T) {
	emptySeq := &SinkErrorQueue{}
	assert.False(t, emptySeq.Available())

	// Only depends on URL being non-empty and a non-nil reference to an SQS queue.
	mockSQS := &mocks.SQSAPI{}
	seq := &SinkErrorQueue{
		url:   "asdfadfasdf2332798",
		queue: mockSQS,
	}
	assert.True(t, seq.Available())
}

func TestSinkErrorQueuePushWhenQueueNotAvailableDoesNotThrowErrorAndNoOps(t *testing.T) {
	emptySeq := &SinkErrorQueue{}

	// Logger is nil.
	err := emptySeq.Push(RecoverableErrorType, "werqw zxcw7228323974", nil)
	assert.Nil(t, err)

	// Logger is not nil; check that a warning is issued using the Logger instance.
	logger := NewMapStashLogger()
	err2 := emptySeq.Push(RecoverableErrorType, "werqw zxcw7228323974", logger)
	assert.Nil(t, err2)
	assert.Equal(t, 1, len(logger.messages["WARN"]))

	// Check warning is emitted to the logger.
	assert.True(t, strings.HasPrefix(logger.messages["WARN"][0], "Not pushing errors to an SQS queue="))
}

func TestSinkErrorQueuePushSuccess(t *testing.T) {
	mockSQS := &mocks.SQSAPI{}

	// Use variables defined outside the scope of a closure to save the message body and queueURL.
	var actualBody string
	var actualQueueUrl string
	mockSendMessageFn := func(input *sqs.SendMessageInput) *sqs.SendMessageOutput {
		actualBody = *input.MessageBody
		actualQueueUrl = *input.QueueUrl
		return &sqs.SendMessageOutput{MessageId: aws.String(actualBody[:5])}
	}

	mockSQS.On("SendMessage", mock.MatchedBy(func(input *sqs.SendMessageInput) bool {
		return input.QueueUrl != nil && *input.QueueUrl != ""
	})).Return(mockSendMessageFn, nil)

	seq := &SinkErrorQueue{
		sinkId: "xxx-000",
		name:   "pushover",
		url:    "https://unit-test.queue.push.url",
		queue:  mockSQS,
	}

	errMsg := "079 cxmn, 198sfakjl"
	err := seq.Push(PanicErrorType, errMsg, nil)
	assert.Nil(t, err)

	// Verify queue URL.
	assert.Equal(t, seq.url, actualQueueUrl)

	// Now check message body.
	decoded, err := base64.URLEncoding.DecodeString(actualBody)
	assert.Nil(t, err, "Error while base64 URL decoding.")

	payload := SinkErrorPayload{}
	err = json.Unmarshal(decoded, &payload)
	assert.Nil(t, err, "Error while de-serializing JSON into SinkErrorPayload object.") // An error de-serializing JSON fails the unit test.

	assert.Equal(t, "xxx-000", payload.SinkId)
	assert.Equal(t, "Panic", string(payload.ErrorType))
	assert.Equal(t, errMsg, payload.ErrorMessage)
	assert.GreaterOrEqual(t, time.Now().Format(time.RFC3339), payload.Timestamp) // Now's timestamp should be later than timestamp of sent message.
}

func TestNewErrorStreamLogger(t *testing.T) {
	baseLogger := NewMapStashLogger()
	store := NewMapStashErrorStore(true, nil)

	logger := NewErrorStreamLogger(baseLogger, store)
	assert.Same(t, baseLogger, logger.base)
	assert.Same(t, store, logger.store)
}

func TestErrorStreamLoggerPrintf(t *testing.T) {
	baseLogger := NewMapStashLogger()
	store := NewMapStashErrorStore(true, nil)

	logger := NewErrorStreamLogger(baseLogger, store)
	logger.Printf("printf:%d", 3)

	assert.Equal(t, 1, len(baseLogger.messages["PRINTF"]))
	assert.Equal(t, 0, len(baseLogger.messages["DEBUG"]))
	assert.Equal(t, 0, len(baseLogger.messages["INFO"]))
	assert.Equal(t, 0, len(baseLogger.messages["WARN"]))
	assert.Equal(t, 0, len(baseLogger.messages["ERROR"]))
	assert.Equal(t, 0, len(baseLogger.messages["PANIC"]))
	assert.Equal(t, "printf:3", baseLogger.messages["PRINTF"][0])

	assert.Equal(t, 0, len(store.storage[RecoverableErrorType]))
	assert.Equal(t, 0, len(store.storage[PanicErrorType]))
}

func TestErrorStreamLoggerDebugf(t *testing.T) {
	baseLogger := NewMapStashLogger()
	store := NewMapStashErrorStore(true, nil)

	logger := NewErrorStreamLogger(baseLogger, store)
	logger.Debugf("debug::%d", 4)

	assert.Equal(t, 0, len(baseLogger.messages["PRINTF"]))
	assert.Equal(t, 1, len(baseLogger.messages["DEBUG"]))
	assert.Equal(t, 0, len(baseLogger.messages["INFO"]))
	assert.Equal(t, 0, len(baseLogger.messages["WARN"]))
	assert.Equal(t, 0, len(baseLogger.messages["ERROR"]))
	assert.Equal(t, 0, len(baseLogger.messages["PANIC"]))
	assert.Equal(t, "debug::4", baseLogger.messages["DEBUG"][0])

	assert.Equal(t, 0, len(store.storage[RecoverableErrorType]))
	assert.Equal(t, 0, len(store.storage[PanicErrorType]))
}

func TestErrorStreamLoggerInfof(t *testing.T) {
	baseLogger := NewMapStashLogger()
	store := NewMapStashErrorStore(true, nil)

	logger := NewErrorStreamLogger(baseLogger, store)
	logger.Infof("info:::%d", 5)

	assert.Equal(t, 0, len(baseLogger.messages["PRINTF"]))
	assert.Equal(t, 0, len(baseLogger.messages["DEBUG"]))
	assert.Equal(t, 1, len(baseLogger.messages["INFO"]))
	assert.Equal(t, 0, len(baseLogger.messages["WARN"]))
	assert.Equal(t, 0, len(baseLogger.messages["ERROR"]))
	assert.Equal(t, 0, len(baseLogger.messages["PANIC"]))
	assert.Equal(t, "info:::5", baseLogger.messages["INFO"][0])

	assert.Equal(t, 0, len(store.storage[RecoverableErrorType]))
	assert.Equal(t, 0, len(store.storage[PanicErrorType]))
}

func TestErrorStreamLoggerWarnf(t *testing.T) {
	baseLogger := NewMapStashLogger()
	store := NewMapStashErrorStore(true, nil)

	logger := NewErrorStreamLogger(baseLogger, store)
	logger.Warnf("warn::::%d", 6)

	assert.Equal(t, 0, len(baseLogger.messages["PRINTF"]))
	assert.Equal(t, 0, len(baseLogger.messages["DEBUG"]))
	assert.Equal(t, 0, len(baseLogger.messages["INFO"]))
	assert.Equal(t, 1, len(baseLogger.messages["WARN"]))
	assert.Equal(t, 0, len(baseLogger.messages["ERROR"]))
	assert.Equal(t, 0, len(baseLogger.messages["PANIC"]))
	assert.Equal(t, "warn::::6", baseLogger.messages["WARN"][0])

	assert.Equal(t, 0, len(store.storage[RecoverableErrorType]))
	assert.Equal(t, 0, len(store.storage[PanicErrorType]))
}

func TestErrorStreamLoggerErrorfWithAvailableQueue(t *testing.T) {
	baseLogger := NewMapStashLogger()
	store := NewMapStashErrorStore(true, nil) // Queue is available so a push actually occurs.

	logger := NewErrorStreamLogger(baseLogger, store)
	logger.Errorf("error:::::%d", 7)

	assert.Equal(t, 0, len(baseLogger.messages["PRINTF"]))
	assert.Equal(t, 0, len(baseLogger.messages["DEBUG"]))
	assert.Equal(t, 0, len(baseLogger.messages["INFO"]))
	assert.Equal(t, 0, len(baseLogger.messages["WARN"]))
	assert.Equal(t, 1, len(baseLogger.messages["ERROR"]))
	assert.Equal(t, 0, len(baseLogger.messages["PANIC"]))
	assert.Equal(t, "error:::::7", baseLogger.messages["ERROR"][0])

	assert.Equal(t, 1, len(store.storage[RecoverableErrorType])) // Verifies a push occurred.
	assert.Equal(t, 0, len(store.storage[PanicErrorType]))
	assert.Equal(t, "error:::::7", store.storage[RecoverableErrorType][0])
}

func TestErrorStreamLoggerErrorfNoAvailableQueue(t *testing.T) {
	baseLogger := NewMapStashLogger()
	store := NewMapStashErrorStore(false, nil) // Queue is unavailable.

	logger := NewErrorStreamLogger(baseLogger, store)
	logger.Errorf("error:::::%d%s", 7, "a")

	assert.Equal(t, 0, len(baseLogger.messages["PRINTF"]))
	assert.Equal(t, 0, len(baseLogger.messages["DEBUG"]))
	assert.Equal(t, 0, len(baseLogger.messages["INFO"]))
	assert.Equal(t, 0, len(baseLogger.messages["WARN"]))
	assert.Equal(t, 1, len(baseLogger.messages["ERROR"])) // Verifies base logger is delegated to even when queue is unavailable.
	assert.Equal(t, 0, len(baseLogger.messages["PANIC"]))
	assert.Equal(t, "error:::::7a", baseLogger.messages["ERROR"][0])

	assert.Equal(t, 0, len(store.storage[RecoverableErrorType]))
	assert.Equal(t, 0, len(store.storage[PanicErrorType]))
}

func TestErrorStreamLoggerErrorfPushFailEmitsErrorToBaseLogger(t *testing.T) {
	baseLogger := NewMapStashLogger()
	errMsg := "ERROR: this error should be emitted to the base logger."
	store := NewMapStashErrorStore(true, errors.New(errMsg)) // Simulate error condition during push.

	logger := NewErrorStreamLogger(baseLogger, store)
	logger.Errorf("error:::::%d%s", 7, "aa")

	assert.Equal(t, 0, len(baseLogger.messages["PRINTF"]))
	assert.Equal(t, 0, len(baseLogger.messages["DEBUG"]))
	assert.Equal(t, 0, len(baseLogger.messages["INFO"]))
	assert.Equal(t, 0, len(baseLogger.messages["WARN"]))
	assert.Equal(t, 2, len(baseLogger.messages["ERROR"])) // Base logger should have a record of both errors and emit both.
	assert.Equal(t, 0, len(baseLogger.messages["PANIC"]))
	assert.Equal(t, "error:::::7aa", baseLogger.messages["ERROR"][0])
	assert.True(t, strings.Contains(baseLogger.messages["ERROR"][1], errMsg))                         // Check original push error message wrapped.
	assert.True(t, strings.Contains(baseLogger.messages["ERROR"][1], "Failed during push to store=")) // Check error contains info that it occurred during a push.

	assert.Equal(t, 0, len(store.storage[RecoverableErrorType]))
	assert.Equal(t, 0, len(store.storage[PanicErrorType]))
}

func TestErrorStreamLoggerPanicfWithAvailableQueue(t *testing.T) {
	baseLogger := NewMapStashLogger()
	store := NewMapStashErrorStore(true, nil) // Queue is available so a push actually occurs.

	logger := NewErrorStreamLogger(baseLogger, store)
	logger.Panicf("panic::::::%d", 8)

	assert.Equal(t, 0, len(baseLogger.messages["PRINTF"]))
	assert.Equal(t, 0, len(baseLogger.messages["DEBUG"]))
	assert.Equal(t, 0, len(baseLogger.messages["INFO"]))
	assert.Equal(t, 0, len(baseLogger.messages["WARN"]))
	assert.Equal(t, 0, len(baseLogger.messages["ERROR"]))
	assert.Equal(t, 1, len(baseLogger.messages["PANIC"]))
	assert.Equal(t, "panic::::::8", baseLogger.messages["PANIC"][0])

	assert.Equal(t, 0, len(store.storage[RecoverableErrorType])) // Verifies a push occurred.
	assert.Equal(t, 1, len(store.storage[PanicErrorType]))
	assert.Equal(t, "panic::::::8", store.storage[PanicErrorType][0])
}

func TestErrorStreamLoggerPanicfNoAvailableQueue(t *testing.T) {
	baseLogger := NewMapStashLogger()
	store := NewMapStashErrorStore(false, nil) // Queue is unavailable.

	logger := NewErrorStreamLogger(baseLogger, store)
	logger.Panicf("panic::::::%d%s", 8, "b")

	assert.Equal(t, 0, len(baseLogger.messages["PRINTF"]))
	assert.Equal(t, 0, len(baseLogger.messages["DEBUG"]))
	assert.Equal(t, 0, len(baseLogger.messages["INFO"]))
	assert.Equal(t, 0, len(baseLogger.messages["WARN"]))
	assert.Equal(t, 0, len(baseLogger.messages["ERROR"]))
	assert.Equal(t, 1, len(baseLogger.messages["PANIC"])) // Verifies base logger is delegated to even when queue is unavailable.
	assert.Equal(t, "panic::::::8b", baseLogger.messages["PANIC"][0])

	assert.Equal(t, 0, len(store.storage[RecoverableErrorType]))
	assert.Equal(t, 0, len(store.storage[PanicErrorType]))
}

func TestErrorStreamLoggerPanicfPushFailEmitsErrorToBaseLogger(t *testing.T) {
	baseLogger := NewMapStashLogger()
	errMsg := "PANIC: this error should be emitted to the base logger."
	store := NewMapStashErrorStore(true, errors.New(errMsg)) // Simulate error condition during push.

	logger := NewErrorStreamLogger(baseLogger, store)
	logger.Panicf("panic::::::%d%s", 8, "bb")

	assert.Equal(t, 0, len(baseLogger.messages["PRINTF"]))
	assert.Equal(t, 0, len(baseLogger.messages["DEBUG"]))
	assert.Equal(t, 0, len(baseLogger.messages["INFO"]))
	assert.Equal(t, 0, len(baseLogger.messages["WARN"]))
	assert.Equal(t, 1, len(baseLogger.messages["ERROR"])) // Base logger should have a record of the error due to a push.
	assert.Equal(t, 1, len(baseLogger.messages["PANIC"])) // Base logger should have a record of the original panic/runtime error.
	assert.Equal(t, "panic::::::8bb", baseLogger.messages["PANIC"][0])
	assert.True(t, strings.Contains(baseLogger.messages["ERROR"][0], errMsg))                         // Check original push error message wrapped.
	assert.True(t, strings.Contains(baseLogger.messages["ERROR"][0], "Failed during push to store=")) // Check error contains info that it occurred during a push.

	assert.Equal(t, 0, len(store.storage[RecoverableErrorType]))
	assert.Equal(t, 0, len(store.storage[PanicErrorType]))
}
