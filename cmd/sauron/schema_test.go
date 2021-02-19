// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/pilosa/pilosa/v2"
)

func NewSauronTestConfig(t *testing.T) *SauronConfig {

	bind := fmt.Sprintf("127.0.0.1:%d", GetAvailPort())
	bindGRPC := fmt.Sprintf("127.0.0.1:%d", GetAvailPort())
	gossipPort := fmt.Sprintf("%d", GetAvailPort())
	dataDir, err := ioutil.TempDir("", "pilosa-sauron-target-*")
	panicOn(err)

	return &SauronConfig{
		DatabaseName: strings.ToLower(t.Name()),
		Bind:         bind,
		BindGRPC:     bindGRPC,
		GossipPort:   gossipPort,
		DataDir:      dataDir,
	}
}

func TestProxyCanHandlePostSchema(t *testing.T) {

	cfg := NewSauronTestConfig(t)
	proxy := NewSauron(cfg)
	proxy.sql.MustRemove() //start with
	url, err := proxy.Start()
	panicOn(err)
	defer proxy.Stop()

	// prep for test by cleaning up leftovers from any old run.
	//panicOn(proxy.sql.DropTablesWithPrefix(t.Name()))

	//schemaJson := `{ "indexes": [{ "name": "simple", "options": { "trackExistence": true }, "fields": [{ "name": "luminosity", "options": { "type": "int" } }, { "name ": "color", "options ": {} } ] }] }`
	/*
		schemaJson := `{ "indexes": [{ "name": "simple", "options": { "trackExistence": true }, "fields": [{ "name": "luminosity", "options": { "type": "int" } } ] }] }`


		vv("Posting err = '%v'", schemaJson)
		resp, err := client.Post(url+"/schema", "application/json", bytes.NewBufferString(schemaJson))
		if resp.StatusCode != http.StatusNoContent {
			panic(fmt.Sprintf("expecting %v got %v", http.StatusNoContent, resp.StatusCode))
		}
		// verify that we can fetch it back
		resp2, err := client.Get(url + "/schema")
		panicOn(err)
		body, err := ioutil.ReadAll(resp2.Body)
		panicOn(err)
		sbody := string(body)
		vv("body = '%v'", sbody)
		if sbody[:62] != schemaJson[:62] {
			fmt.Printf("did not get posted schema back: observed:\n\n%v\n\nexpected:\n\n%v\n\n", sbody, schemaJson)
			panic("unxpected schema back")
		}
	*/
	// load large schema too
	client := &http.Client{}
	schm, err := ioutil.ReadFile("sample_schema.json")
	panicOn(err)

	resp, err := client.Post(url+"/schema", "application/json", bytes.NewBuffer(schm))
	panicOn(err)
	if resp.StatusCode != http.StatusNoContent {
		panic(fmt.Sprintf("expecting %v got %v", http.StatusNoContent, resp.StatusCode))
	}

	// verify all tables expected are present
	obs := make(map[string]bool)
	for i, table := range proxy.sql.ListIndexFields() {
		_ = i
		obs[table] = true
		if !expectedTables[table] {
			panic(fmt.Sprintf("observed table '%v' but was not expected", table))
		}
	}
	for table := range expectedTables {
		if !obs[table] {
			panic(fmt.Sprintf("expected table '%v' but was not observed", table))
		}
	}

}

func TestProxyCanCreateIndexFieldViaPost(t *testing.T) {

	cfg := NewSauronTestConfig(t)
	proxy := NewSauron(cfg)
	proxy.sql.MustRemove() //start with
	url, err := proxy.Start()
	panicOn(err)
	defer proxy.Stop()
	client := &http.Client{}
	//create non-keyed or "simple" index
	resp, err := client.Post(url+"/index/simple", "application/json", bytes.NewBuffer(nil))
	panicOn(err)
	if resp.StatusCode != http.StatusOK {
		panic(fmt.Sprintf("expecting %v got %v", http.StatusOK, resp.StatusCode))
	}

	fields := []struct {
		name    string
		options []byte
	}{
		{name: "set", options: []byte{}},
		{name: "keyset", options: []byte{}},
		{name: "bsi", options: []byte{}},
	}
	for _, field := range fields {
		//create set field
		resp, err = client.Post(fmt.Sprintf("%v/index/simple/field/%v", url, field.name), "application/json", bytes.NewBuffer(field.options))
		panicOn(err)
		if resp.StatusCode != http.StatusOK {
			panic(fmt.Sprintf("expecting %v got %v", http.StatusOK, resp.StatusCode))
		}
	}
	// verify that we can fetch it back
	resp2, err := client.Get(url + "/schema")
	panicOn(err)
	body, err := ioutil.ReadAll(resp2.Body)
	panicOn(err)
	var schema pilosa.Schema
	err = json.Unmarshal(body, &schema)
	if err != nil {
		panic(fmt.Sprintf("decoding request as JSON Pilosa schema: %v", err))
	}

	expected := map[string]map[string]bool{
		"simple": {"bsi": true, "set": true, "keyset": true},
	}
	for _, i := range schema.Indexes {
		ll, ok := expected[i.Name]
		if !ok {
			panic(fmt.Sprintf("expected index not present: %v", i.Name))
		}
		for _, f := range i.Fields {
			_, ok := ll[f.Name]
			if !ok {
				panic(fmt.Sprintf("expected field '%v' not present in index'%v'", f.Name, i.Name))
			}
		}
	}
}

var expectedTables = map[string]bool{
	"trait_store/aba":                                                                true,
	"trait_store/ach_batch_recency":                                                  true,
	"trait_store/ach_pass_thru_recency":                                              true,
	"trait_store/ach_payment_recency":                                                true,
	"trait_store/bools":                                                              true,
	"trait_store/bools-exists":                                                       true,
	"trait_store/central_group":                                                      true,
	"trait_store/custom_audiences":                                                   true,
	"trait_store/days_since_last_logon":                                              true,
	"trait_store/db":                                                                 true,
	"trait_store/desktop_recency":                                                    true,
	"trait_store/domestic_wire_recency":                                              true,
	"trait_store/external_transfer_recency":                                          true,
	"trait_store/fields":                                                             true,
	"trait_store/funds_transfer_recency":                                             true,
	"trait_store/international_wire_recency":                                         true,
	"trait_store/mobile_remote_deposit_recency":                                      true,
	"trait_store/payroll_recency":                                                    true,
	"trait_store/pfm_category_total_current_balance__401k_investment":                true,
	"trait_store/pfm_category_total_current_balance__403b_investment":                true,
	"trait_store/pfm_category_total_current_balance__529_investment":                 true,
	"trait_store/pfm_category_total_current_balance__any":                            true,
	"trait_store/pfm_category_total_current_balance__auto_loan":                      true,
	"trait_store/pfm_category_total_current_balance__brokerage_account":              true,
	"trait_store/pfm_category_total_current_balance__certificate_of_deposit":         true,
	"trait_store/pfm_category_total_current_balance__checking":                       true,
	"trait_store/pfm_category_total_current_balance__credit_card":                    true,
	"trait_store/pfm_category_total_current_balance__home_equity_loan":               true,
	"trait_store/pfm_category_total_current_balance__ira_investment":                 true,
	"trait_store/pfm_category_total_current_balance__line_of_credit":                 true,
	"trait_store/pfm_category_total_current_balance__loan":                           true,
	"trait_store/pfm_category_total_current_balance__money_market":                   true,
	"trait_store/pfm_category_total_current_balance__mortgage":                       true,
	"trait_store/pfm_category_total_current_balance__personal_loan":                  true,
	"trait_store/pfm_category_total_current_balance__roth_ira_investment":            true,
	"trait_store/pfm_category_total_current_balance__savings":                        true,
	"trait_store/pfm_category_total_current_balance__simple_ira":                     true,
	"trait_store/pfm_category_total_current_balance__student_loan":                   true,
	"trait_store/pfm_category_total_current_balance__taxable_investment":             true,
	"trait_store/phone_recency":                                                      true,
	"trait_store/product_count__brokerage_account":                                   true,
	"trait_store/product_count__commercial_cd_or_share_certificate":                  true,
	"trait_store/product_count__commercial_checking_or_share_draft":                  true,
	"trait_store/product_count__commercial_credit_card":                              true,
	"trait_store/product_count__commercial_indirect_auto_loan":                       true,
	"trait_store/product_count__commercial_line_of_credit":                           true,
	"trait_store/product_count__commercial_loan":                                     true,
	"trait_store/product_count__commercial_money_market":                             true,
	"trait_store/product_count__commercial_new_auto_loan":                            true,
	"trait_store/product_count__commercial_real_estate_loan":                         true,
	"trait_store/product_count__commercial_savings_or_share":                         true,
	"trait_store/product_count__commercial_secured_credit_card":                      true,
	"trait_store/product_count__commercial_secured_loan":                             true,
	"trait_store/product_count__commercial_special_checking_or_share_draft":          true,
	"trait_store/product_count__commercial_special_credit_card":                      true,
	"trait_store/product_count__commercial_special_savings_or_share":                 true,
	"trait_store/product_count__commercial_special_vehicle_loan":                     true,
	"trait_store/product_count__commercial_used_auto_loan":                           true,
	"trait_store/product_count__consumer_basic_checking_or_share_draft":              true,
	"trait_store/product_count__consumer_cd_or_share_certificate":                    true,
	"trait_store/product_count__consumer_credit_card":                                true,
	"trait_store/product_count__consumer_indirect_auto_loan":                         true,
	"trait_store/product_count__consumer_interest_bearing_checking_or_share_draft":   true,
	"trait_store/product_count__consumer_line_of_credit":                             true,
	"trait_store/product_count__consumer_loan":                                       true,
	"trait_store/product_count__consumer_money_market":                               true,
	"trait_store/product_count__consumer_new_auto_loan":                              true,
	"trait_store/product_count__consumer_premium_checking_or_share_draft":            true,
	"trait_store/product_count__consumer_premium_savings_or_share":                   true,
	"trait_store/product_count__consumer_real_estate_loan":                           true,
	"trait_store/product_count__consumer_restricted_checking_or_share_draft":         true,
	"trait_store/product_count__consumer_restricted_savings_or_share":                true,
	"trait_store/product_count__consumer_savings_or_share":                           true,
	"trait_store/product_count__consumer_secured_credit_card":                        true,
	"trait_store/product_count__consumer_secured_loan":                               true,
	"trait_store/product_count__consumer_special_checking_or_share_draft":            true,
	"trait_store/product_count__consumer_special_credit_card":                        true,
	"trait_store/product_count__consumer_special_savings_or_share":                   true,
	"trait_store/product_count__consumer_special_vehicle_loan":                       true,
	"trait_store/product_count__consumer_used_auto_loan":                             true,
	"trait_store/product_count__debit_card":                                          true,
	"trait_store/product_count__education_savings_or_share":                          true,
	"trait_store/product_count__escrow_account":                                      true,
	"trait_store/product_count__health_savings_account":                              true,
	"trait_store/product_count__home_equity_line_of_credit":                          true,
	"trait_store/product_count__home_equity_loan":                                    true,
	"trait_store/product_count__insurance_product":                                   true,
	"trait_store/product_count__interest_only_legal_trust_account":                   true,
	"trait_store/product_count__membership_share":                                    true,
	"trait_store/product_count__other_service":                                       true,
	"trait_store/product_count__plan_401k":                                           true,
	"trait_store/product_count__plan_403b":                                           true,
	"trait_store/product_count__plan_529":                                            true,
	"trait_store/product_count__roth_ira":                                            true,
	"trait_store/product_count__roth_ira_cd_or_share_certificate":                    true,
	"trait_store/product_count__small_business_checking_or_share_draft":              true,
	"trait_store/product_count__special_indirect_vehicle_loan":                       true,
	"trait_store/product_count__student_education_loan":                              true,
	"trait_store/product_count__traditional_ira":                                     true,
	"trait_store/product_count__traditional_ira_cd_or_share_certificate":             true,
	"trait_store/product_count__trust":                                               true,
	"trait_store/product_count__vacation_club_holiday_savings":                       true,
	"trait_store/product_recency__brokerage_account":                                 true,
	"trait_store/product_recency__commercial_cd_or_share_certificate":                true,
	"trait_store/product_recency__commercial_checking_or_share_draft":                true,
	"trait_store/product_recency__commercial_credit_card":                            true,
	"trait_store/product_recency__commercial_indirect_auto_loan":                     true,
	"trait_store/product_recency__commercial_line_of_credit":                         true,
	"trait_store/product_recency__commercial_loan":                                   true,
	"trait_store/product_recency__commercial_money_market":                           true,
	"trait_store/product_recency__commercial_new_auto_loan":                          true,
	"trait_store/product_recency__commercial_real_estate_loan":                       true,
	"trait_store/product_recency__commercial_savings_or_share":                       true,
	"trait_store/product_recency__commercial_secured_credit_card":                    true,
	"trait_store/product_recency__commercial_secured_loan":                           true,
	"trait_store/product_recency__commercial_special_checking_or_share_draft":        true,
	"trait_store/product_recency__commercial_special_credit_card":                    true,
	"trait_store/product_recency__commercial_special_savings_or_share":               true,
	"trait_store/product_recency__commercial_special_vehicle_loan":                   true,
	"trait_store/product_recency__commercial_used_auto_loan":                         true,
	"trait_store/product_recency__consumer_basic_checking_or_share_draft":            true,
	"trait_store/product_recency__consumer_cd_or_share_certificate":                  true,
	"trait_store/product_recency__consumer_credit_card":                              true,
	"trait_store/product_recency__consumer_indirect_auto_loan":                       true,
	"trait_store/product_recency__consumer_interest_bearing_checking_or_share_draft": true,
	"trait_store/product_recency__consumer_line_of_credit":                           true,
	"trait_store/product_recency__consumer_loan":                                     true,
	"trait_store/product_recency__consumer_money_market":                             true,
	"trait_store/product_recency__consumer_new_auto_loan":                            true,
	"trait_store/product_recency__consumer_premium_checking_or_share_draft":          true,
	"trait_store/product_recency__consumer_premium_savings_or_share":                 true,
	"trait_store/product_recency__consumer_real_estate_loan":                         true,
	"trait_store/product_recency__consumer_restricted_checking_or_share_draft":       true,
	"trait_store/product_recency__consumer_restricted_savings_or_share":              true,
	"trait_store/product_recency__consumer_savings_or_share":                         true,
	"trait_store/product_recency__consumer_secured_credit_card":                      true,
	"trait_store/product_recency__consumer_secured_loan":                             true,
	"trait_store/product_recency__consumer_special_checking_or_share_draft":          true,
	"trait_store/product_recency__consumer_special_credit_card":                      true,
	"trait_store/product_recency__consumer_special_savings_or_share":                 true,
	"trait_store/product_recency__consumer_special_vehicle_loan":                     true,
	"trait_store/product_recency__consumer_used_auto_loan":                           true,
	"trait_store/product_recency__debit_card":                                        true,
	"trait_store/product_recency__education_savings_or_share":                        true,
	"trait_store/product_recency__escrow_account":                                    true,
	"trait_store/product_recency__health_savings_account":                            true,
	"trait_store/product_recency__home_equity_line_of_credit":                        true,
	"trait_store/product_recency__home_equity_loan":                                  true,
	"trait_store/product_recency__insurance_product":                                 true,
	"trait_store/product_recency__interest_only_legal_trust_account":                 true,
	"trait_store/product_recency__membership_share":                                  true,
	"trait_store/product_recency__other_service":                                     true,
	"trait_store/product_recency__plan_401k":                                         true,
	"trait_store/product_recency__plan_403b":                                         true,
	"trait_store/product_recency__plan_529":                                          true,
	"trait_store/product_recency__roth_ira":                                          true,
	"trait_store/product_recency__roth_ira_cd_or_share_certificate":                  true,
	"trait_store/product_recency__small_business_checking_or_share_draft":            true,
	"trait_store/product_recency__special_indirect_vehicle_loan":                     true,
	"trait_store/product_recency__student_education_loan":                            true,
	"trait_store/product_recency__traditional_ira":                                   true,
	"trait_store/product_recency__traditional_ira_cd_or_share_certificate":           true,
	"trait_store/product_recency__trust":                                             true,
	"trait_store/product_recency__vacation_club_holiday_savings":                     true,
	"trait_store/stop_payment_recency":                                               true,
	"trait_store/survey_2053c210_adfc_4e14_afd3_9abc68a70719":                        true,
	"trait_store/survey_2679bd7e_cdb8_4c24_a1da_e904799382eb":                        true,
	"trait_store/survey_2f336120_7130_492b_b116_d93d560baa0a":                        true,
	"trait_store/survey_36e229ff_c0a6_48ad_928e_19b5abf69f11":                        true,
	"trait_store/survey_3773950d_a3a6_419a_b9e8_7ed9e33f93aa":                        true,
	"trait_store/survey_38aeaf3f_35ab_4df5_b213_5bd7dc29b796":                        true,
	"trait_store/survey_587ae933_b8e3_49a2_a23d_989df9ad119f":                        true,
	"trait_store/survey_5dfe1a89_29fa_4505_ac00_4cd055b758ab":                        true,
	"trait_store/survey_7bc3b9f0_fb8c_4a79_9a22_14be4ed71949":                        true,
	"trait_store/survey_9cebb459_162e_4518_88f6_bc1cdd630cd0":                        true,
	"trait_store/survey_a05de6d3_6620_4eb2_b9a2_511936680b57":                        true,
	"trait_store/survey_b570363c_ba94_4a7a_9891_141a1befc42b":                        true,
	"trait_store/survey_c358d6e0_4116_4230_84ed_869b315e89a1":                        true,
	"trait_store/survey_cce773ce_14e8_4fae_90cd_213a821a3bc0":                        true,
	"trait_store/survey_e49df858_4903_40ac_bd9f_96f764940f2c":                        true,
	"trait_store/survey_eab7aa49_26e0_4bef_a25f_ddabc9ee2bd9":                        true,
	"trait_store/tablet_recency":                                                     true,
	"trait_store/user_id":                                                            true,
	"trait_store/zip_code":                                                           true,
}
