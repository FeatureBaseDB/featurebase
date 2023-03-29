package kafka

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

type configStructFromFileTest struct {
	configFilePath string
	expectedStruct string
}

func TestConfigStructFromFile(t *testing.T) {
	tests := []configStructFromFileTest{
		{ // confirm struct fields are being set in general
			configFilePath: "./config_test_data/config01.toml",
			expectedStruct: `{"Hosts":["kafka:9090"],"Group":"testGroup","Topics":["testTopic"],"BatchSize":35,"BatchMaxStaleness":25000000000,"Timeout":16000000000,"Table":"testTable","Fields":[{"Name":"id","SourceType":"id","SourcePath":null,"PrimaryKey":true},{"Name":"name","SourceType":"string","SourcePath":["test","path"],"PrimaryKey":false},{"Name":"age","SourceType":"int","SourcePath":null,"PrimaryKey":false},{"Name":"hobbies","SourceType":"stringset","SourcePath":null,"PrimaryKey":false}],"Encode":"json"}`,
		},
		{ // confirm defaults are being set
			configFilePath: "./config_test_data/config00.toml",
			expectedStruct: `{"Hosts":["localhost:9092"],"Group":"default-featurebase-group","Topics":["topic00"],"BatchSize":1,"BatchMaxStaleness":5000000000,"Timeout":5000000000,"Table":"table00","Fields":[{"Name":"id","SourceType":"string","SourcePath":null,"PrimaryKey":true},{"Name":"name","SourceType":"string","SourcePath":null,"PrimaryKey":false},{"Name":"age","SourceType":"int","SourcePath":null,"PrimaryKey":false},{"Name":"hobbies","SourceType":"stringset","SourcePath":null,"PrimaryKey":false}],"Encode":"json"}`,
		},
	}

	for _, test := range tests {
		cfg, err := ConfigStructFromFile(test.configFilePath)
		if err != nil {
			t.Fatalf("getting config struct from file: %s", err)
		}
		json, err := json.Marshal(cfg)
		if err != nil {
			t.Fatalf("marshaling config struct: %s", err)
		}
		require.JSONEq(t, test.expectedStruct, string(json))
	}

}
