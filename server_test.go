// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa_test

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/test"
)

func TestViewsRemovalTTL(t *testing.T) {
	cluster := test.MustRunCluster(t, 1)
	node := cluster.GetNode(0)
	defer cluster.Close()

	// Create a client
	client := node.Client()

	indexName := "i"

	// Create indexes and field with ttl lasting 24 hours
	if err := client.CreateIndex(context.Background(), indexName, pilosa.IndexOptions{TrackExistence: true}); err != nil && err != pilosa.ErrIndexExists {
		t.Fatalf("creating index, err: %v", err)
	}

	dateNow := time.Now().UTC()
	dateYesterday := dateNow.Add(-24 * time.Hour)
	dateCurrentMonth := time.Date(dateNow.Year(), dateNow.Month(), 1, 0, 0, 0, 0, time.UTC)
	dateLastDayOfMonth := dateCurrentMonth.AddDate(0, 1, 0).Add(-time.Nanosecond)

	var tests = []struct {
		name     string
		date     string
		expViews []string
	}{
		{
			name:     "date_old",
			date:     "2001-02-03T04:05",
			expViews: []string{"standard"},
			/* date_old (2001-02-03T04:05), this will create these views:
			- standard
			- standard_2001
			- standard_200102
			- standard_20010203
			- standard_2001020304
			Since the sample date here is over 20 years all views except "standard" should get deleted
			*/
		},
		{
			name: "date_now",
			date: fmt.Sprintf("%d-%02d-%02dT%02d:%02d", dateNow.Year(), dateNow.Month(), dateNow.Day(), dateNow.Hour(), dateNow.Minute()),
			expViews: []string{
				"standard",
				"standard_" + fmt.Sprintf("%d", dateNow.Year()),
				"standard_" + fmt.Sprintf("%d%02d", dateNow.Year(), dateNow.Month()),
				"standard_" + fmt.Sprintf("%d%02d%02d", dateNow.Year(), dateNow.Month(), dateNow.Day()),
				"standard_" + fmt.Sprintf("%d%02d%02d%02d", dateNow.Year(), dateNow.Month(), dateNow.Day(), dateNow.Hour()),
			},
			/* For example: current time is 2022-05-11T15:17 (also when the 24 hrs ttl countdown starts) will generate these views:
			- standard_2022			-> end date is 2023_01_01 T00:00, in future -> keep
			- standard_202205		-> end date is 2022_06_01 T00:00, in future -> keep
			- standard_20220511		-> end date is 2022_05_12 T00:00, in future -> keep
			- standard_2022051115	-> end date is 2022_05_11 T18:00, in future -> keep
			*/
		},
		{
			name: "date_yesterday",
			date: fmt.Sprintf("%d-%02d-%02dT%02d:%02d", dateYesterday.Year(), dateYesterday.Month(), dateYesterday.Day(), dateYesterday.Hour(), dateYesterday.Minute()),
			expViews: []string{
				"standard",
				"standard_" + fmt.Sprintf("%d", dateYesterday.Year()),
				"standard_" + fmt.Sprintf("%d%02d", dateYesterday.Year(), dateYesterday.Month()),
				"standard_" + fmt.Sprintf("%d%02d%02d", dateYesterday.Year(), dateYesterday.Month(), dateYesterday.Day()),
				"standard_" + fmt.Sprintf("%d%02d%02d%02d", dateYesterday.Year(), dateYesterday.Month(), dateYesterday.Day(), dateYesterday.Hour()),
			},
			/* Example: current time is 2022-05-11T15:17, date_yesterday (2022-05-10T15:17) will generate these views:
			- standard_2022			-> end date is 2023_01_01 T00:00, in future -> keep
			- standard_202205		-> end date is 2022_06_01 T00:00, in future -> keep
			- standard_20220510		-> end date is 2022_05_11 T00:00, within 24 hrs -> keep
			- standard_2022051015	-> end date is 2022_05_10 T16:00, within 24 hrs -> keep
			*/
		},
		// {
		// 	name: "date_first_of_month",
		// 	date: fmt.Sprintf("%d-%02d-%02dT%02d:%02d", dateCurrentMonth.Year(), dateCurrentMonth.Month(), 1, 0, 0),
		// 	expViews: []string{
		// 		"standard",
		// 		"standard_" + fmt.Sprintf("%d", dateCurrentMonth.Year()),
		// 		"standard_" + fmt.Sprintf("%d%02d", dateCurrentMonth.Year(), dateCurrentMonth.Month()),
		// 	},
		// 	/* Example: current time is 2022-05-11T15:17, date_first_of_month (2022-05-01T00:00) will generate these views:
		// 	- standard_2022			-> end date is 2023_01_01 T00:00, in future -> keep
		// 	- standard_202205		-> end date is 2022_06_01 T00:00, in future -> keep
		// 	- standard_20220501		-> end date is 2022_05_02 T00:00, older than ttl -> delete
		// 	- standard_2022050115	-> end date is 2022_05_01 T01:00, older than ttl -> delete
		// 	!!! commenting this test out for now, there is a special case where if today's date is also same as date_first_of_month
		// 	in that case, the expected views would have all 4, instead of 2 in the above example, since they are not older than the 24 hr ttl
		// 	*/
		// },
		{
			name: "date_last_of_month",
			date: fmt.Sprintf("%d-%02d-%02dT%02d:%02d", dateLastDayOfMonth.Year(), dateLastDayOfMonth.Month(), dateLastDayOfMonth.Day(), dateLastDayOfMonth.Hour(), dateLastDayOfMonth.Minute()),
			expViews: []string{
				"standard",
				"standard_" + fmt.Sprintf("%d", dateLastDayOfMonth.Year()),
				"standard_" + fmt.Sprintf("%d%02d", dateLastDayOfMonth.Year(), dateLastDayOfMonth.Month()),
				"standard_" + fmt.Sprintf("%d%02d%02d", dateNow.Year(), dateNow.Month(), dateLastDayOfMonth.Day()),
				"standard_" + fmt.Sprintf("%d%02d%02d%02d", dateNow.Year(), dateNow.Month(), dateLastDayOfMonth.Day(), dateLastDayOfMonth.Hour()),
			},
			/* Example: current time is 2022-05-11T15:17, date_last_of_month (2022-05-31T23:59) will generate these views:
			- standard_2022			-> end date is 2023_01_01 T00:00, in future -> keep
			- standard_202205		-> end date is 2022_06_01 T00:00, in future -> keep
			- standard_20220531		-> end date is 2022_06_01 T00:00, in future -> keep
			- standard_2022053123	-> end date is 2022_06_01 T00:00, in future -> keep
			*/
		},
		{
			name: "date_first_hour_day",
			date: fmt.Sprintf("%d-%02d-%02dT%02d:%02d", dateNow.Year(), dateNow.Month(), dateNow.Day(), 0, 0),
			expViews: []string{
				"standard",
				"standard_" + fmt.Sprintf("%d", dateNow.Year()),
				"standard_" + fmt.Sprintf("%d%02d", dateNow.Year(), dateNow.Month()),
				"standard_" + fmt.Sprintf("%d%02d%02d", dateNow.Year(), dateNow.Month(), dateNow.Day()),
				"standard_" + fmt.Sprintf("%d%02d%02d%02d", dateNow.Year(), dateNow.Month(), dateNow.Day(), 0),
			},
			/* Example: current time is 2022-05-11T15:17, date_first_hour_day (2022-05-11T00:00) will generate these views:
			- standard_2022			-> end date is 2023_01_01 T00:00, in future -> keep
			- standard_202205		-> end date is 2022_06_01 T00:00, in future -> keep
			- standard_20220511		-> end date is 2022_05_12 T00:00, in future -> keep
			- standard_2022051100	-> end date is 2022_05_01 T01:00, within TTL -> keep
			*/
		},
		{
			name: "date_last_hour_day",
			date: fmt.Sprintf("%d-%02d-%02dT%02d:%02d", dateNow.Year(), dateNow.Month(), dateNow.Day(), 23, 59),
			expViews: []string{
				"standard",
				"standard_" + fmt.Sprintf("%d", dateNow.Year()),
				"standard_" + fmt.Sprintf("%d%02d", dateNow.Year(), dateNow.Month()),
				"standard_" + fmt.Sprintf("%d%02d%02d", dateNow.Year(), dateNow.Month(), dateNow.Day()),
				"standard_" + fmt.Sprintf("%d%02d%02d%02d", dateNow.Year(), dateNow.Month(), dateNow.Day(), 23),
			},
			/* Example: current time is 2022-05-11T15:17, date_last_hour_day (2022-05-11T23:59) will generate these views:
			- standard_2022			-> end date is 2023_01_01 T00:00, in future -> keep
			- standard_202205		-> end date is 2022_06_01 T00:00, in future -> keep
			- standard_20220511		-> end date is 2022_05_12 T00:00, in future -> keep
			- standard_2022051123	-> end date is 2022_05_12 T00:00, in future -> keep
			*/
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := client.CreateFieldWithOptions(context.Background(), indexName, test.name, pilosa.FieldOptions{TTL: time.Hour * 24, Type: pilosa.FieldTypeTime, TimeQuantum: "YMDH"}); err != nil {
				t.Fatalf("creating field, err: %v", err)
			}

			// set data
			_, err := client.Query(context.Background(), indexName, &pilosa.QueryRequest{Index: indexName, Query: "Set(1, " + test.name + "=1, " + test.date + ")"})
			if err != nil {
				t.Fatalf("setting sample data, err: %v", err)
			}

			// run ViewsRemoval
			node.Server.ViewsRemoval(context.Background())

			// Get all the views for given index + field
			views, err := node.API.Views(context.Background(), indexName, test.name)
			if err != nil {
				t.Fatal(err)
			}
			var viewNames []string
			for _, view := range views {
				viewNames = append(viewNames, view.Name())
			}
			sort.Strings(viewNames)

			if !reflect.DeepEqual(test.expViews, viewNames) {
				t.Fatalf("after ttl removal, expected %v, but got %v", test.expViews, viewNames)
			}
		})
	}
}

func TestViewsRemovalStandard(t *testing.T) {
	cluster := test.MustRunCluster(t, 1)
	node := cluster.GetNode(0)
	defer cluster.Close()

	// Create a client
	client := node.Client()

	indexName := "i"

	// Create indexes and field with ttl lasting 24 hours
	if err := client.CreateIndex(context.Background(), indexName, pilosa.IndexOptions{TrackExistence: true}); err != nil && err != pilosa.ErrIndexExists {
		t.Fatalf("creating index, err: %v", err)
	}

	var tests = []struct {
		name           string
		date           string
		noStandardView string
		expViews       []string
	}{
		{
			name:           "t1_delete_standard",
			date:           "2001-02-03T04:05",
			noStandardView: "true",
			expViews:       nil,
			/* date 2001-02-03T04:05, this will create these views:
			- standard
			- standard_2001
			- standard_200102
			- standard_20010203
			- standard_2001020304
			Since the sample date here is over 20 years, all views with dates should get deleted
			Since noStandardView is true, 'standard' view should get deleted
			*/
		},
		{
			name:           "t2_keep_standard",
			date:           "2001-02-03T04:05",
			noStandardView: "false",
			expViews:       []string{"standard"},
			/* date 2001-02-03T04:05, this will create these views:
			- standard
			- standard_2001
			- standard_200102
			- standard_20010203
			- standard_2001020304
			Since the sample date here is over 20 years, all views with dates should get deleted
			Since noStandardView is false, 'standard' view should NOT get deleted
			*/
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := client.CreateFieldWithOptions(context.Background(), indexName, test.name, pilosa.FieldOptions{TTL: time.Hour * 24, Type: pilosa.FieldTypeTime, TimeQuantum: "YMDH"}); err != nil {
				t.Fatalf("creating field, err: %v", err)
			}

			// set data
			_, err := client.Query(context.Background(), indexName, &pilosa.QueryRequest{Index: indexName, Query: "Set(1, " + test.name + "=1, " + test.date + ")"})
			if err != nil {
				t.Fatalf("setting sample data, err: %v", err)
			}

			// update noStandardView value
			err = node.API.UpdateField(context.Background(), indexName, test.name, pilosa.FieldUpdate{Option: "noStandardView", Value: test.noStandardView})
			if err != nil {
				t.Fatalf("updating noStandardView, err: %v", err)
			}

			// run ViewsRemoval
			node.Server.ViewsRemoval(context.Background())

			// get all the views for given index + field
			views, err := node.API.Views(context.Background(), indexName, test.name)
			if err != nil {
				t.Fatal(err)
			}
			var viewNames []string
			for _, view := range views {
				viewNames = append(viewNames, view.Name())
			}
			sort.Strings(viewNames)
			if !reflect.DeepEqual(test.expViews, viewNames) {
				t.Fatalf("after ttl removal, expected %v, but got %v", test.expViews, viewNames)
			}
		})
	}
}
