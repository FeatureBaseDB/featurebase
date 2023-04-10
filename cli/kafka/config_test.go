package kafka

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

type configFromFileTest struct {
	configFilePath string
	expectedStruct string
}

func TestConfigFromFile(t *testing.T) {
	tests := []configFromFileTest{
		{ // confirm json config struct fields are being set
			configFilePath: "./testdata/config/config01.toml",
			expectedStruct: `{"Hosts":["kafka:9090"],"Group":"testGroup","Topics":["testTopic"],"BatchSize":35,"BatchMaxStaleness":25000000000,"Timeout":16000000000,"Table":"testTable","Fields":[{"Name":"id","SourceType":"id","SourcePath":null,"PrimaryKey":true},{"Name":"name","SourceType":"string","SourcePath":["test","path"],"PrimaryKey":false},{"Name":"age","SourceType":"int","SourcePath":null,"PrimaryKey":false},{"Name":"hobbies","SourceType":"stringset","SourcePath":null,"PrimaryKey":false}],"SchemaRegistryURL":"","SchemaRegistryUsername":"","SchemaRegistryPassword":"","Encode":"json","AllowMissingFields":false,"MaxMessages":0,"ConfluentConfig":""}`,
		},
		{ // confirm defaults are being set
			configFilePath: "./testdata/config/config00.toml",
			expectedStruct: `{"Hosts":["localhost:9092"],"Group":"default-featurebase-group","Topics":["topic00"],"BatchSize":1,"BatchMaxStaleness":5000000000,"Timeout":5000000000,"Table":"table00","Fields":[{"Name":"id","SourceType":"string","SourcePath":null,"PrimaryKey":true},{"Name":"name","SourceType":"string","SourcePath":null,"PrimaryKey":false},{"Name":"age","SourceType":"int","SourcePath":null,"PrimaryKey":false},{"Name":"hobbies","SourceType":"stringset","SourcePath":null,"PrimaryKey":false}],"SchemaRegistryURL":"","SchemaRegistryUsername":"","SchemaRegistryPassword":"","Encode":"json","AllowMissingFields":false,"MaxMessages":0,"ConfluentConfig":""}`,
		},
		{ // confirm avro config struct field are being set
			configFilePath: "./testdata/config/config02.toml",
			expectedStruct: `{"Hosts":["kafka:9090"],"Group":"testGroup","Topics":["testTopic"],"BatchSize":35,"BatchMaxStaleness":25000000000,"Timeout":16000000000,"Table":"testTable","Fields":[{"Name":"id","SourceType":"id","SourcePath":null,"PrimaryKey":true},{"Name":"name","SourceType":"string","SourcePath":["test","path"],"PrimaryKey":false},{"Name":"age","SourceType":"int","SourcePath":null,"PrimaryKey":false},{"Name":"hobbies","SourceType":"stringset","SourcePath":null,"PrimaryKey":false}],"SchemaRegistryURL":"","SchemaRegistryUsername":"","SchemaRegistryPassword":"","Encode":"avro","AllowMissingFields":false,"MaxMessages":0,"ConfluentConfig":""}`,
		},
		{ // confirm avro config struct field are being set
			configFilePath: "./testdata/config/config03.toml",
			expectedStruct: `{"Hosts":["kafka:9090"],"Group":"testGroup","Topics":["testTopic"],"BatchSize":35,"BatchMaxStaleness":25000000000,"Timeout":16000000000,"Table":"testTable","Fields":[{"Name":"id","SourceType":"id","SourcePath":null,"PrimaryKey":true},{"Name":"name","SourceType":"string","SourcePath":["test","path"],"PrimaryKey":false},{"Name":"age","SourceType":"int","SourcePath":null,"PrimaryKey":false},{"Name":"hobbies","SourceType":"stringset","SourcePath":null,"PrimaryKey":false}],"SchemaRegistryURL":"","SchemaRegistryUsername":"","SchemaRegistryPassword":"","Encode":"avro","AllowMissingFields":true,"MaxMessages":100,"ConfluentConfig":"./test/confluent/config.json"}`,
		},
	}

	for _, test := range tests {
		cfg, err := ConfigFromFile(test.configFilePath)
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
