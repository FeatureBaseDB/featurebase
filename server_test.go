// Copyright 2022 Molecula Corp. All rights reserved.
package pilosa_test

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/test"
)

func TestTtlRemoval(t *testing.T) {

	cluster := test.MustRunCluster(t, 1)
	node := cluster.GetNode(0)
	defer cluster.Close()

	// Create a client
	client := node.Client()

	indexName := "i"
	fieldName := "f"

	// Create indexes and field with ttl lasting 24 hours
	if err := client.CreateIndex(context.Background(), indexName, pilosa.IndexOptions{TrackExistence: true}); err != nil && err != pilosa.ErrIndexExists {
		t.Fatalf("creating index, err: %v", err)
	} else if err := client.CreateFieldWithOptions(context.Background(), indexName, fieldName, pilosa.FieldOptions{Ttl: time.Hour * 24, Type: pilosa.FieldTypeTime, TimeQuantum: "YMDH"}); err != nil {
		t.Fatalf("creating field, err: %v", err)
	}

	/* Set sample data 1 using this date: '2001-02-03T04:05', this will create these views:
	- standard
	- standard_2001
	- standard_200102
	- standard_20010203
	- standard_2001020304
	Since the sample date here is over 24 hours, all views except "standard" should get deleted
	*/
	_, err := client.Query(context.Background(), indexName, &pilosa.QueryRequest{Index: indexName, Query: "Set(1, f=1, 2001-02-03T04:05)"})
	if err != nil {
		t.Fatalf("setting sample data 1, err: %v", err)
	}

	dateNow := time.Now()
	dateNowString := fmt.Sprintf("%d-%02d-%02dT%02d:%02d", dateNow.Year(), dateNow.Month(), dateNow.Day(), dateNow.Hour(), dateNow.Minute())
	/* Set sample data 2 using current time.
	For example: current time is 2022-03-03T15:17 (also when the 24 hrs ttl countdown starts) will generate these views:
	- standard_2022			-> gets converted to 2022_01_01, over 24 hours for ttl -> deleted
	- standard_202203		-> gets converted to 2022_03_01, over 24 hours for ttl -> deleted
	- standard_20220303		-> gets converted to 2022_03_03, within 24 hours -> keep
	- standard_2022030315	-> gets converted to 2022_03_03 15:00, within 24 hours -> keep
	*/
	_, err = client.Query(context.Background(), indexName, &pilosa.QueryRequest{Index: indexName, Query: "Set(2, f=2, " + dateNowString + ")"})
	if err != nil {
		t.Fatalf("setting sample data 2, err: %v", err)
	}

	/* Set sample data 3 using yesterday's date
	All views generated from this date should be deleted
	*/
	dateYesterday := time.Now().Add(-24*time.Hour + -1*time.Nanosecond)
	dateYesterdayString := fmt.Sprintf("%d-%02d-%02dT%02d:%02d", dateYesterday.Year(), dateYesterday.Month(), dateYesterday.Day(), dateYesterday.Hour(), dateYesterday.Minute())
	_, err = client.Query(context.Background(), indexName, &pilosa.QueryRequest{Index: indexName, Query: "Set(3, f=3, " + dateYesterdayString + ")"})
	if err != nil {
		t.Fatalf("setting sample data 3, err: %v", err)
	}

	node.Server.TtlRemoval(context.Background())

	// Get all the views for given index + field
	views, err := node.API.Views(context.Background(), indexName, fieldName)
	if err != nil {
		t.Fatal(err)
	}

	expectedViewNames := []string{
		"standard",
		"standard_" + fmt.Sprintf("%d%02d%02d", dateNow.Year(), dateNow.Month(), dateNow.Day()),
		"standard_" + fmt.Sprintf("%d%02d%02d%02d", dateNow.Year(), dateNow.Month(), dateNow.Day(), dateNow.Hour()),
	}

	var viewNames []string
	for _, view := range views {
		viewNames = append(viewNames, view.Name())
	}
	sort.Strings(viewNames)

	if !reflect.DeepEqual(expectedViewNames, viewNames) {
		t.Fatalf("after ttl removal, expected %v, but got %v", expectedViewNames, viewNames)
	}
}
