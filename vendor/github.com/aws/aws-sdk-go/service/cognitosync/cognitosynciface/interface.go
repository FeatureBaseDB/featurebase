// THIS FILE IS AUTOMATICALLY GENERATED. DO NOT EDIT.

// Package cognitosynciface provides an interface to enable mocking the Amazon Cognito Sync service client
// for testing your code.
//
// It is important to note that this interface will have breaking changes
// when the service model is updated and adds new API operations, paginators,
// and waiters.
package cognitosynciface

import (
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/cognitosync"
)

// CognitoSyncAPI provides an interface to enable mocking the
// cognitosync.CognitoSync service client's API operation,
// paginators, and waiters. This make unit testing your code that calls out
// to the SDK's service client's calls easier.
//
// The best way to use this interface is so the SDK's service client's calls
// can be stubbed out for unit testing your code with the SDK without needing
// to inject custom request handlers into the the SDK's request pipeline.
//
//    // myFunc uses an SDK service client to make a request to
//    // Amazon Cognito Sync.
//    func myFunc(svc cognitosynciface.CognitoSyncAPI) bool {
//        // Make svc.BulkPublish request
//    }
//
//    func main() {
//        sess := session.New()
//        svc := cognitosync.New(sess)
//
//        myFunc(svc)
//    }
//
// In your _test.go file:
//
//    // Define a mock struct to be used in your unit tests of myFunc.
//    type mockCognitoSyncClient struct {
//        cognitosynciface.CognitoSyncAPI
//    }
//    func (m *mockCognitoSyncClient) BulkPublish(input *cognitosync.BulkPublishInput) (*cognitosync.BulkPublishOutput, error) {
//        // mock response/functionality
//    }
//
//    TestMyFunc(t *testing.T) {
//        // Setup Test
//        mockSvc := &mockCognitoSyncClient{}
//
//        myfunc(mockSvc)
//
//        // Verify myFunc's functionality
//    }
//
// It is important to note that this interface will have breaking changes
// when the service model is updated and adds new API operations, paginators,
// and waiters. Its suggested to use the pattern above for testing, or using
// tooling to generate mocks to satisfy the interfaces.
type CognitoSyncAPI interface {
	BulkPublishRequest(*cognitosync.BulkPublishInput) (*request.Request, *cognitosync.BulkPublishOutput)

	BulkPublish(*cognitosync.BulkPublishInput) (*cognitosync.BulkPublishOutput, error)

	DeleteDatasetRequest(*cognitosync.DeleteDatasetInput) (*request.Request, *cognitosync.DeleteDatasetOutput)

	DeleteDataset(*cognitosync.DeleteDatasetInput) (*cognitosync.DeleteDatasetOutput, error)

	DescribeDatasetRequest(*cognitosync.DescribeDatasetInput) (*request.Request, *cognitosync.DescribeDatasetOutput)

	DescribeDataset(*cognitosync.DescribeDatasetInput) (*cognitosync.DescribeDatasetOutput, error)

	DescribeIdentityPoolUsageRequest(*cognitosync.DescribeIdentityPoolUsageInput) (*request.Request, *cognitosync.DescribeIdentityPoolUsageOutput)

	DescribeIdentityPoolUsage(*cognitosync.DescribeIdentityPoolUsageInput) (*cognitosync.DescribeIdentityPoolUsageOutput, error)

	DescribeIdentityUsageRequest(*cognitosync.DescribeIdentityUsageInput) (*request.Request, *cognitosync.DescribeIdentityUsageOutput)

	DescribeIdentityUsage(*cognitosync.DescribeIdentityUsageInput) (*cognitosync.DescribeIdentityUsageOutput, error)

	GetBulkPublishDetailsRequest(*cognitosync.GetBulkPublishDetailsInput) (*request.Request, *cognitosync.GetBulkPublishDetailsOutput)

	GetBulkPublishDetails(*cognitosync.GetBulkPublishDetailsInput) (*cognitosync.GetBulkPublishDetailsOutput, error)

	GetCognitoEventsRequest(*cognitosync.GetCognitoEventsInput) (*request.Request, *cognitosync.GetCognitoEventsOutput)

	GetCognitoEvents(*cognitosync.GetCognitoEventsInput) (*cognitosync.GetCognitoEventsOutput, error)

	GetIdentityPoolConfigurationRequest(*cognitosync.GetIdentityPoolConfigurationInput) (*request.Request, *cognitosync.GetIdentityPoolConfigurationOutput)

	GetIdentityPoolConfiguration(*cognitosync.GetIdentityPoolConfigurationInput) (*cognitosync.GetIdentityPoolConfigurationOutput, error)

	ListDatasetsRequest(*cognitosync.ListDatasetsInput) (*request.Request, *cognitosync.ListDatasetsOutput)

	ListDatasets(*cognitosync.ListDatasetsInput) (*cognitosync.ListDatasetsOutput, error)

	ListIdentityPoolUsageRequest(*cognitosync.ListIdentityPoolUsageInput) (*request.Request, *cognitosync.ListIdentityPoolUsageOutput)

	ListIdentityPoolUsage(*cognitosync.ListIdentityPoolUsageInput) (*cognitosync.ListIdentityPoolUsageOutput, error)

	ListRecordsRequest(*cognitosync.ListRecordsInput) (*request.Request, *cognitosync.ListRecordsOutput)

	ListRecords(*cognitosync.ListRecordsInput) (*cognitosync.ListRecordsOutput, error)

	RegisterDeviceRequest(*cognitosync.RegisterDeviceInput) (*request.Request, *cognitosync.RegisterDeviceOutput)

	RegisterDevice(*cognitosync.RegisterDeviceInput) (*cognitosync.RegisterDeviceOutput, error)

	SetCognitoEventsRequest(*cognitosync.SetCognitoEventsInput) (*request.Request, *cognitosync.SetCognitoEventsOutput)

	SetCognitoEvents(*cognitosync.SetCognitoEventsInput) (*cognitosync.SetCognitoEventsOutput, error)

	SetIdentityPoolConfigurationRequest(*cognitosync.SetIdentityPoolConfigurationInput) (*request.Request, *cognitosync.SetIdentityPoolConfigurationOutput)

	SetIdentityPoolConfiguration(*cognitosync.SetIdentityPoolConfigurationInput) (*cognitosync.SetIdentityPoolConfigurationOutput, error)

	SubscribeToDatasetRequest(*cognitosync.SubscribeToDatasetInput) (*request.Request, *cognitosync.SubscribeToDatasetOutput)

	SubscribeToDataset(*cognitosync.SubscribeToDatasetInput) (*cognitosync.SubscribeToDatasetOutput, error)

	UnsubscribeFromDatasetRequest(*cognitosync.UnsubscribeFromDatasetInput) (*request.Request, *cognitosync.UnsubscribeFromDatasetOutput)

	UnsubscribeFromDataset(*cognitosync.UnsubscribeFromDatasetInput) (*cognitosync.UnsubscribeFromDatasetOutput, error)

	UpdateRecordsRequest(*cognitosync.UpdateRecordsInput) (*request.Request, *cognitosync.UpdateRecordsOutput)

	UpdateRecords(*cognitosync.UpdateRecordsInput) (*cognitosync.UpdateRecordsOutput, error)
}

var _ CognitoSyncAPI = (*cognitosync.CognitoSync)(nil)
