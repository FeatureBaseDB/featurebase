package datagen

import (
	"io"
	"strconv"
	"time"

	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/idk/datagen/gen"
	"github.com/molecula/featurebase/v3/pql"
)

// Ensure PaloAlto implements interface.
var _ Sourcer = (*PaloAlto)(nil)

// PaloAlto implements Sourcer.
type PaloAlto struct {
	schema []idk.Field
}

// NewPaloAlto returns a new instance of PaloAlto.
func NewPaloAlto(cfg SourceGeneratorConfig) Sourcer {
	return &PaloAlto{
		schema: []idk.Field{
			idk.StringField{NameVal: "vlan_source"},                                   //  0) "unknown"
			idk.StringField{NameVal: "severity_level"},                                //  1) "Normal"
			idk.StringField{NameVal: "display_profile_type"},                          //  2) "IoT"
			idk.BoolField{NameVal: "keep"},                                            //  3) true
			idk.StringField{NameVal: "internal_risk_level"},                           //  4) "Low"
			idk.IntField{NameVal: "ml_progress"},                                      //  5) 100
			idk.StringField{NameVal: "tenantid"},                                      //  6) "gjtu7ldl"
			idk.StringField{NameVal: "display_oscombined"},                            //  7) "Windows CE"
			idk.StringField{NameVal: "profileid"},                                     //  8) "Zebra Technologies Tracking Device"
			idk.StringField{NameVal: "risk_main_contributors"},                        //  9) "Application Usage"
			idk.IntField{NameVal: "profile_date"},                                     // 10) "2019-03-14T06:09:00.000Z"
			idk.StringField{NameVal: "risk_level"},                                    // 11) "low"
			idk.StringArrayField{NameVal: "baseline-learning_category"},               // 12) "payload"
			idk.StringField{NameVal: "baseline-state"},                                // 13) "stable"
			idk.StringField{NameVal: "baseline-protected"},                            // 14) "profile model"
			idk.IntField{NameVal: "baseline-progress"},                                // 15) 100
			idk.IntField{NameVal: "baseline-pi"},                                      // 16) 98
			idk.StringArrayField{NameVal: "baseline-invalid_category"},                // 17) "ext_inbound", "ext_outbound"
			idk.DecimalField{NameVal: "baseline-anomaly_score-application", Scale: 1}, // 18) 0.1
			idk.DecimalField{NameVal: "baseline-anomaly_score-internal", Scale: 4},    // 19) 0.4097
			idk.DecimalField{NameVal: "baseline-anomaly_score-protocol", Scale: 4},    // 20) 0.3736
			idk.DecimalField{NameVal: "baseline-anomaly_score-external", Scale: 4},    // 21) 0.3548
			idk.DecimalField{NameVal: "baseline-anomaly_score-payload", Scale: 4},     // 22) 0.2944
			idk.IntField{NameVal: "profile_confidence"},                               // 23) 86
			idk.StringField{NameVal: "display_osgroup"},                               // 24) "Windows CE"
			idk.IntField{NameVal: "ml_risk-risk_score"},                               // 25) 10
			idk.IntField{NameVal: "ml_risk-progress"},                                 // 26) 100
			idk.StringField{NameVal: "ml_risk-severity_level"},                        // 27) "Low"
			idk.DecimalField{NameVal: "ml_risk-anomaly_score-application", Scale: 2},  // 28) 0.25
			idk.DecimalField{NameVal: "ml_risk-anomaly_score-internal", Scale: 2},     // 29) 0.25
			idk.DecimalField{NameVal: "ml_risk-anomaly_score-protocol", Scale: 2},     // 30) 0.25
			idk.DecimalField{NameVal: "ml_risk-anomaly_score-external", Scale: 2},     // 31) 0.25
			idk.DecimalField{NameVal: "ml_risk-anomaly_score-payload", Scale: 2},      // 32) 0.25
			idk.IntField{NameVal: "ml_risk-date"},                                     // 33) "2020-09-22T00:00:00Z"
			idk.StringField{NameVal: "risk_level_source"},                             // 34) "offline_analytics"
			idk.IntField{NameVal: "risk_score"},                                       // 35) 40
			idk.StringField{NameVal: "source"},                                        // 36) ""
			idk.IntField{NameVal: "internal_risk_score"},                              // 37) 10
			idk.StringField{NameVal: "connect_evtcontent-username"},                   // 38) "unknown"
			idk.StringField{NameVal: "connect_evtcontent-hostname"},                   // 39) "unknown"
			idk.BoolField{NameVal: "connect_evtcontent-monitored"},                    // 40) true
			idk.StringField{NameVal: "connect_evtcontent-ossource"},                   // 41) "inspector"
			idk.StringField{NameVal: "connect_evtcontent-roles"},                      // 42) ""
			idk.StringField{NameVal: "connect_evtcontent-osgroup"},                    // 43) "Embedded"
			idk.StringField{NameVal: "connect_evtcontent-ip"},                         // 44) "10.2.171.9"
			idk.IntField{NameVal: "connect_evtcontent-vlan"},                          // 45) 0
			idk.StringField{NameVal: "connect_evtcontent-useragent"},                  // 46) "unknown"
			idk.StringField{NameVal: "connect_evtcontent-os"},                         // 47) "Palm OS"
			idk.StringField{NameVal: "connect_evtcontent-osver_src"},                  // 48) "offline"
			idk.StringField{NameVal: "display_vendor"},                                // 49) "Zebra Technologies"
			idk.StringField{NameVal: "profile_vendor"},                                // 50) "Zebra Technologies"
			idk.IntField{NameVal: "ml_risk_score"},                                    // 51) 10
			idk.StringField{NameVal: "subnets"},                                       // 52) "10.2.170.0/23"
			idk.StringField{NameVal: "ml_risk_level"},                                 // 53) "Low"
			idk.StringField{NameVal: "profile_type"},                                  // 54) "IoT"
			idk.IntField{NameVal: "profile_type_score"},                               // 55) 100
			idk.StringField{NameVal: "display_hostname"},                              // 56) "unknown"
			idk.StringField{NameVal: "display_os"},                                    // 57) "Palm OS"
			idk.StringField{NameVal: "vendor"},                                        // 58) "Zebra Technologies"
			idk.StringField{NameVal: "siteid"},                                        // 59) "6"
			idk.IntField{NameVal: "progress"},                                         // 60) 100
			idk.IntField{NameVal: "date"},                                             // 61) "2019-02-09T03:31:08.590Z"
			idk.IntField{NameVal: "risk_date"},                                        // 62) "2018-10-28T00:41:17.534012Z"
			idk.StringField{NameVal: "display_vlan"},                                  // 63) "unknown"
			idk.IntField{NameVal: "filter_last_modified"},                             // 64) "2017-08-05T01:35:35.429Z"
			idk.StringField{NameVal: "profile_category"},                              // 65) "Patient Tracking"
			idk.StringField{NameVal: "applianceid"},                                   // 66) "000000000000000000000CC47ADB6556"
			idk.StringField{NameVal: "profile_vertical"},                              // 67) "Medical"
			idk.DecimalField{NameVal: "anomaly_score-application", Scale: 4},          // 68) 0.1442
			idk.DecimalField{NameVal: "anomaly_score-internal", Scale: 4},             // 69) 0.1587
			idk.DecimalField{NameVal: "anomaly_score-protocol", Scale: 4},             // 70) 0.11750000000000001
			idk.DecimalField{NameVal: "anomaly_score-payload", Scale: 1},              // 71) 0.1
			idk.DecimalField{NameVal: "anomaly_score-external", Scale: 1},             // 72) 0.1
			idk.StringField{NameVal: "profile_osgroup"},                               // 73) "Windows CE"
			idk.StringField{NameVal: "user_profile_type"},                             // 74) "IoT"
			idk.StringField{NameVal: "display_dhcp"},                                  // 75) "Yes"
			idk.StringField{NameVal: "profile_type_source"},                           // 76) "profiler"
			idk.IntField{NameVal: "baseline_update_date"},                             // 77) "2019.04.20"
			idk.StringField{NameVal: "deviceid"},                                      // 78) "40:83:de:9e:7d:8c"
			idk.StringField{NameVal: "profile_classifier"},                            // 79) "OuiClassifier"
			idk.StringField{NameVal: "dhcp"},                                          // 80) "Yes"
			idk.StringField{NameVal: "display_profile_category"},                      // 81) "Patient Tracking"
			idk.StringField{NameVal: "display_profileid"},                             // 82) "Macbook Pro"
			idk.BoolField{NameVal: "unique"},                                          // 83) true
			idk.BoolField{NameVal: "pii"},                                             // 84) true
			idk.IntField{NameVal: "pii_ts"},                                           // 85) "2019-01-01T20:15:07.294Z"
			idk.StringField{NameVal: "display_osverfirmwarever"},                      // 86) ""
			idk.StringField{NameVal: "display_wirewireless"},                          // 87) "wireless"
			idk.StringField{NameVal: "wirewireless"},                                  // 88) "wireless"
			idk.StringField{NameVal: "display_epp_safety"},                            // 89) "not_protected"
			idk.StringField{NameVal: "mac"},                                           // 90) "40:83:de:9e:7d:8c"
			idk.StringField{NameVal: "foreignaccess"},                                 // 91) "No"
			idk.DecimalField{NameVal: "ai_vendor_confidence", Scale: 2},               // 92) 98.52
			idk.IntField{NameVal: "subnets_date"},                                     // 93) "2020-09-17T18:42:28.691Z"
			idk.StringField{NameVal: "display_profile_confidence_source"},             // 94) "profiler"
			idk.IntField{NameVal: "display_profile_confidence_date"},                  // 95) "2019-03-14T06:09:00Z"
			idk.IntField{NameVal: "display_profile_confidence"},                       // 96) 86
			idk.StringField{NameVal: "display_profile_confidence_level"},              // 97) "70_Medium"
			idk.BoolField{NameVal: "filtered"},                                        // 98)
			idk.BoolField{NameVal: "routed"},                                          // 99)
			idk.StringField{NameVal: "object_id"},                                     // 100)
			idk.StringField{NameVal: "country_access_country_code"},                   // 101)
			idk.BoolField{NameVal: "country_access_malicious"},                        // 102)
			idk.IntField{NameVal: "country_access_ts"},                                // 103)
		},
	}
}

// Source returns an idk.Source which will generate
// records for a partition of the entire record space,
// determined by the concurrency value. It implements
// the Sourcer interface.
func (k *PaloAlto) Source(cfg SourceConfig) idk.Source {

	src := &PaloAltoSource{
		cur:    cfg.startFrom,
		endAt:  cfg.endAt,
		schema: k.schema,
	}

	src.g = gen.New(gen.OptGenSeed(cfg.seed))
	src.record = make([]interface{}, len(src.schema))
	src.record[17] = []string{}
	src.record[5] = make([]byte, 12)
	src.record[78] = []byte("40:83:de:9e:7d:8c")
	src.record[90] = []byte("40:83:de:9e:7d:8c")

	// add object IDs
	// there'll be 10,000 unique Object IDs
	numUniqueObjectIDs := 10000
	objectIDLen := 10
	charset := "abcdefghijklmnopqrstuvwxyz0123456789"
	objectIDs := make([]string, numUniqueObjectIDs)
	for i := 0; i < numUniqueObjectIDs; i++ {
		randID := make([]byte, objectIDLen)
		for j := range randID {
			randID[j] = charset[src.g.R.Intn(len(charset))]
		}
		objectIDs[i] = string(randID)
	}
	src.objectIDs = objectIDs

	return src
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (k *PaloAlto) PrimaryKeyFields() []string {
	return nil
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (k *PaloAlto) DefaultEndAt() uint64 {
	return 20000
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (k *PaloAlto) Info() string {
	return "Generates data representative of Palo Alto Networks' IoT/medical device security operations. 105 fields of various types, unkeyed."
}

// Ensure PaloAltoSource implements interface.
var _ idk.Source = (*PaloAltoSource)(nil)

// PaloAltoSource is a data generator which generates
// data for all Pilosa field types.
type PaloAltoSource struct {
	g *gen.Gen

	cur, endAt uint64

	schema []idk.Field
	record record

	objectIDs []string
}

func (k *PaloAltoSource) Record() (idk.Record, error) {
	if k.cur >= k.endAt {
		return nil, io.EOF
	}

	company := k.g.StringFromListWeighted(gen.Animals) + " " + k.g.StringFromListWeighted([]string{"Technologies", "Systems", "Associates"})
	os := k.g.StringFromListWeighted([]string{"Palm OS", "OSI", "FreeBSD", "BeOS"})
	baseTime := time.Date(2014, time.January, 1, 0, 0, 0, 0, time.UTC)
	curTime := time.Date(2020, time.November, 1, 0, 0, 0, 0, time.UTC)
	durationSeconds := curTime.Sub(baseTime) / time.Second

	// Increment the ID.
	k.cur++
	k.record[0] = k.g.StringFromListWeighted([]string{"unknown", "switch", "router", "gateway"})
	k.record[1] = k.g.StringFromListWeighted([]string{"Normal", "Low", "Zero", "Elevated", "High", "Critical"})
	k.record[2] = k.g.StringFromListWeighted([]string{"IoT", "Fixed", "Server"})
	k.record[3] = k.g.R.Intn(2) == 0
	k.record[4] = k.g.StringFromListWeighted([]string{"Low", "Normal", "Zero", "Elevated", "High", "Critical"})
	k.record[5] = k.g.R.Int63n(101)
	k.record[6] = k.g.StringFromListWeighted([]string{"gjtu7ldl", "C6RKKn", "9pFooX", "SvNzWU", "jCdISA", "y4fsPI", "ID5qFL", "zMpvSO", "DR4pCh",
		"5BbmPh", "gjTU0F", "DclwrO", "dpsZkt", "5vGgIy", "FQzIDE", "TW8d5P",
		"sxKStY", "1YULKk", "Gpn6r9", "HXy0Ju", "2EVpvj", "3wOt4J", "iXkNkv"})
	k.record[7] = k.g.StringFromListWeighted([]string{"Windows CE", "Windows 2000", "Windows XP", "Windows 95", "Mac OS 8", "Solaris", "MS Bob", "Plan 9", "Windows ME Professional Plus Ultimate Turbo Edition"})
	k.record[8] = company + " " + k.g.StringFromListWeighted([]string{"Tracking Device", "Pointing Device", "Input Device", "Display", ""})
	k.record[9] = k.g.StringFromListWeighted([]string{"Application Usage", "Automated Processes", "Background Scans"})
	k.record[10] = baseTime.Unix() + k.g.R.Int63n(int64(durationSeconds))
	k.record[11] = k.g.StringFromListWeighted([]string{"low", "normal", "zero", "elevated", "high", "critical"})
	k.record[12] = "payload"
	k.record[13] = k.g.StringFromListWeighted([]string{"stable", "unstable"})
	k.record[14] = "profile model"
	k.record[15] = k.g.R.Int63n(101)
	k.record[16] = k.g.R.Int63n(101)
	k.record[17] = k.g.StringSliceFromList(k.record[17].([]string), []string{"ext_inbound", "ext_outbound", "int_over", "int_under", "ext_across", "int_through"})
	k.record[18] = pql.NewDecimal(k.g.R.Int63n(10), 1)
	k.record[19] = pql.NewDecimal(k.g.R.Int63n(10000), 4)
	k.record[20] = pql.NewDecimal(k.g.R.Int63n(10000), 4)
	k.record[21] = pql.NewDecimal(k.g.R.Int63n(10000), 4)
	k.record[22] = pql.NewDecimal(k.g.R.Int63n(10000), 4)
	k.record[23] = k.g.R.Int63n(100)
	k.record[24] = k.g.StringFromListWeighted([]string{"Windows CE", "Windows 2000", "Windows XP", "Windows 95", "Mac OS 8", "Solaris", "MS Bob", "Plan 9", "Windows ME Professional Plus Ultimate Turbo Edition"})
	k.record[25] = k.g.R.Int63n(100)
	k.record[26] = k.g.R.Int63n(101)
	k.record[27] = k.g.StringFromListWeighted([]string{"Low", "Normal", "Zero", "Elevated", "High", "Critical"})
	k.record[28] = pql.NewDecimal(k.g.R.Int63n(100), 2) // 0.25
	k.record[29] = pql.NewDecimal(k.g.R.Int63n(100), 2) // 0.25
	k.record[30] = pql.NewDecimal(k.g.R.Int63n(100), 2) // 0.25
	k.record[31] = pql.NewDecimal(k.g.R.Int63n(100), 2) // 0.25
	k.record[32] = pql.NewDecimal(k.g.R.Int63n(100), 2) // 0.25
	k.record[33] = baseTime.Unix() + k.g.R.Int63n(int64(durationSeconds))
	k.record[34] = "offline_analytics"
	k.record[35] = k.g.R.Int63n(100)
	k.record[36] = k.g.StringFromListWeighted([]string{"snmp", "arp", "wlc", "cmms", "splunk", "bgp", "dhcp", "dns", "ftp", "http", "https",
		"imap", "ldap", "mgcp", "mqtt", "nntp", "ntp", "pop", "ptp", "onc/rpc", "rtp",
		"rtsp", "rip", "sip", "smtp", "ssh", "telnet", "tls/ssl"})
	k.record[37] = k.g.R.Int63n(100)
	k.record[38] = "unknown"
	k.record[39] = k.g.StringFromListWeighted([]string{"macbook.com", "imac.org", "google.com", "youtube.com", "fb.com", "csdn.net", "apple.com", "stackoverflow.com", "aws.com"})
	k.record[40] = k.g.R.Intn(2) == 0
	k.record[41] = k.g.StringFromListWeighted([]string{"offline", "inspector", "online", "SNMP", "nmap"})
	k.record[42] = ""
	k.record[43] = k.g.StringFromListWeighted([]string{"Embedded", "Desktop", "Server", "Workstation", "Handheld", "Hobbyist"})
	k.record[44] = "10.2.171.9"
	k.record[45] = k.g.R.Int63n(10)
	k.record[46] = k.g.StringFromListWeighted([]string{"unknown", "Firefox", "Chrome", "Safari", "Edge", "Internet Explorer", "Netscape Navigator", "Opera", "Iceweasel", "lynx", "IE6"})
	k.record[47] = os
	k.record[48] = k.g.StringFromListWeighted([]string{"offline", "inspector", "online", "SNMP", "nmap"})
	k.record[49] = company
	k.record[50] = company
	k.record[51] = k.g.R.Int63n(101)
	k.record[52] = "10.2.170.0/23"
	k.record[53] = k.g.StringFromListWeighted([]string{"Low", "Normal", "Zero", "Elevated", "High", "Critical"})
	k.record[54] = k.g.StringFromListWeighted([]string{"IoT", "Fixed", "Server"})
	k.record[55] = k.g.R.Int63n(101)
	k.record[56] = "unknown"
	k.record[57] = os
	k.record[58] = company
	k.record[59] = strconv.Itoa(k.g.R.Intn(10)) //"6"
	k.record[60] = k.g.R.Int63n(101)
	k.record[61] = baseTime.Unix() + k.g.R.Int63n(int64(durationSeconds))
	k.record[62] = baseTime.Unix() + k.g.R.Int63n(int64(durationSeconds))
	k.record[63] = "unknown"
	k.record[64] = baseTime.Unix() + k.g.R.Int63n(int64(durationSeconds))
	k.record[65] = k.g.StringFromListWeighted([]string{"Patient Tracking", "Doctor Tracking", "Equipment", "Droids You're Looking For"})
	k.record[66] = "000000000000000000000CC47AD" + strconv.Itoa(10000+k.g.R.Intn(89000))
	k.record[67] = k.g.StringFromListWeighted([]string{"medical", "technology", "retail", "manufacturing", "transportation", "mining", "hospitality"})
	k.record[68] = pql.NewDecimal(k.g.R.Int63n(10000), 4)
	k.record[69] = pql.NewDecimal(k.g.R.Int63n(10000), 4)
	k.record[70] = pql.NewDecimal(k.g.R.Int63n(10000), 4)
	k.record[71] = pql.NewDecimal(k.g.R.Int63n(10), 1)
	k.record[72] = pql.NewDecimal(k.g.R.Int63n(10), 1)
	k.record[73] = k.g.StringFromListWeighted([]string{"Windows CE", "Windows 2000", "Windows XP", "Windows 95", "Mac OS 8", "Solaris", "MS Bob", "Plan 9", "Windows ME Professional Plus Ultimate Turbo Edition"})
	k.record[74] = k.g.StringFromListWeighted([]string{"IoT", "Fixed", "Server"})
	k.record[75] = k.g.StringFromListWeighted([]string{"Yes", "No", "Unknown"})
	k.record[76] = "profiler"
	k.record[77] = baseTime.Unix() + k.g.R.Int63n(int64(durationSeconds))
	// semi-randomize mac address, cardinality 10k
	k.record[78].([]byte)[0] = 48 + byte(k.g.R.Intn(10))
	k.record[78].([]byte)[3] = 48 + byte(k.g.R.Intn(10))
	k.record[78].([]byte)[12] = 48 + byte(k.g.R.Intn(10))
	k.record[78].([]byte)[15] = 48 + byte(k.g.R.Intn(10))
	k.record[79] = "OuiClassifier"
	k.record[80] = k.g.StringFromListWeighted([]string{"Yes", "No", "Unknown"})
	k.record[81] = k.g.StringFromListWeighted([]string{"Patient Tracking", "Doctor Tracking", "Equipment", "Droids You're Looking For"})
	k.record[82] = k.g.StringFromListWeighted([]string{"iPhone", "iPad", "Mac-Macbook", "Mac-MacbookPro", "Mac-iMac",
		"Google Pixel 5", "Samsung Galaxy Note 20", "OnePlus 8 Pro",
		"Mac-MacMini", "Dell G3", "Microsoft Surface Pro", "Thinkpad",
		"HP Pavilion x360", "Dell Inspiron 7577", "HP Notebook", "HP Envy", "Asus Zenbook"})
	k.record[83] = k.g.R.Intn(2) == 0
	k.record[84] = k.g.R.Intn(2) == 0
	k.record[85] = baseTime.Unix() + k.g.R.Int63n(int64(durationSeconds))
	k.record[86] = ""
	k.record[87] = k.g.StringFromListWeighted([]string{"wireless", "wired", "bluetooth", "Qi", "unplugged"})
	k.record[88] = k.record[87]
	k.record[89] = k.g.StringFromListWeighted([]string{"not_protected", "protected", "double_coverage", "sacked"})
	k.record[90] = k.record[78]
	k.record[91] = k.g.StringFromListWeighted([]string{"Yes", "No", "Unknown"})
	k.record[92] = pql.NewDecimal(k.g.R.Int63n(10000), 2) // 98.52
	k.record[93] = baseTime.Unix() + k.g.R.Int63n(int64(durationSeconds))
	k.record[94] = "profiler"
	k.record[95] = baseTime.Unix() + k.g.R.Int63n(int64(durationSeconds))
	k.record[96] = k.g.R.Int63n(100) // 86
	k.record[97] = k.g.StringFromListWeighted([]string{"70_Medium", "50_Medium", "30_Medium", "10_Low", "90_High"})
	k.record[98] = k.g.R.Intn(4) != 0 // weight towards true
	k.record[99] = k.g.R.Intn(4) != 0 // weight towards true
	k.record[100] = k.g.StringFromListWeighted(k.objectIDs)
	k.record[101] = k.g.StringFromListWeighted([]string{"US", "AT", "AU", "BR", "CA", "CH", "CN", "DE",
		"DK", "EC", "EG", "FR", "GB", "IE", "IN", "IT", "JP", "MX", "NL",
		"PL", "SA", "SG", "TR", "UG", "ZM"})
	k.record[102] = k.g.R.Intn(4) == 0 // weight towards false
	// the first value, 15778080, is the timestamp value for 2000-01-01 at
	// the minute resolution. The second value used for the int range, 10999979,
	// is the duration in minutes between 2000-01-01 and 2020-11-30
	k.record[103] = 15778080 + k.g.R.Intn(10999979)

	return k.record, nil
}

func (k *PaloAltoSource) Schema() []idk.Field {
	return k.schema
}

func (k *PaloAltoSource) Close() error {
	return nil
}
