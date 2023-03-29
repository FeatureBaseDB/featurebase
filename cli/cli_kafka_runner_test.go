package cli

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/featurebasedb/featurebase/v3/cli/internal"
	"github.com/featurebasedb/featurebase/v3/cli/kafka"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/stretchr/testify/require"
)

// These variables are used throught tests to refer to resouces needed for the
// tests (i.e. where to find the kafka service)
var (
	pilosaHost     string
	pilosaTLSHost  string
	pilosaGrpcHost string
	kafkaHost      string
	registryHost   string
	certPath       string
)

// The variables above are set here. If local is set to true, the tests assume
// all services are running locally. Otherwise, we use the name of the
// containers that these tests will have access to in CI.
func init() {
	local := true
	var ok bool
	if pilosaHost, ok = os.LookupEnv("IDK_TEST_PILOSA_HOST"); !ok {
		if local {
			pilosaHost = "localhost:10101"
		} else {
			pilosaHost = "pilosa:10101"
		}
	}
	if pilosaTLSHost, ok = os.LookupEnv("IDK_TEST_PILOSA_TLS_HOST"); !ok {
		pilosaTLSHost = "https://pilosa-tls:10111"
	}
	if pilosaGrpcHost, ok = os.LookupEnv("IDK_TEST_PILOSA_GRPC_HOST"); !ok {
		if local {
			pilosaGrpcHost = "localhost:20101"
		} else {
			pilosaGrpcHost = "pilosa:20101"
		}
	}
	if kafkaHost, ok = os.LookupEnv("IDK_TEST_KAFKA_HOST"); !ok {
		if local {
			kafkaHost = "localhost:9092"
		} else {
			kafkaHost = "kafka:9092"
		}
	}
	if registryHost, ok = os.LookupEnv("IDK_TEST_REGISTRY_HOST"); !ok {
		if local {
			registryHost = "localhost:8081"
		} else {
			registryHost = "schema-registry:8081"
		}
	}
	if certPath, ok = os.LookupEnv("IDK_TEST_CERT_PATH"); !ok {
		certPath = "/certs"
	}
}

// Struct used for TestRunner which contains the information needed to ingest
// data to kafka, configure the runner, and test that the runner successfully
// ran.
type KafkaRunnerTest struct {
	ConfigFile      string      // path to the configuration file used for the runner
	DataFile        string      // path to the data file to populate kafka with
	Encode          string      // how to encode the data from the data file
	CreateTableStmt string      // statement used to create table prior to ingest
	Tests           []TestQuery // list of test which are 2-tuples of query and expected results

}

// Struct which contains a query to run agaisnt featurebase and the expected
// results from that query.
type TestQuery struct {
	Query        string
	ExpectedResp string
}

// The paths (from the folder that contains this file) to folders with kafka
// runner configurations files and data to be sent and consumed from kafka.
var confPathPrefix string = "./kafka/runner_test_data/config/"
var dataPathPrefix string = "./kafka/runner_test_data/data/"

// A slice of KafkaRunnerTest structs that will be used in TestKafkaRunner test
// function.
var kafkaRunnerTests = []KafkaRunnerTest{
	{ // id keys
		ConfigFile: "config00.toml",
		DataFile:   "data00.json",
		Encode:     kafka.JSON,
		Tests: []TestQuery{
			{
				Query:        "Extract(Sort(All(), field=name), Rows(name), Rows(age), Rows(hobbies))",
				ExpectedResp: `{"results":[{"fields":[{"name":"name","type":"string"},{"name":"age","type":"int64"},{"name":"hobbies","type":"[]string"}],"columns":[{"column":1,"rows":["a",20,["hob2","hob1"]]},{"column":2,"rows":["b",21,["hob2","hob3"]]},{"column":3,"rows":["c",22,["hob3","hob4"]]},{"column":4,"rows":["d",23,["hob4","hob5"]]},{"column":5,"rows":["e",24,["hob5","hob6"]]},{"column":6,"rows":["f",26,["hob6","hob7"]]}]}]}`,
			},
		},
		CreateTableStmt: "(_id ID, name String, age Int, hobbies StringSet)",
	},
	{ // string keys
		ConfigFile: "config01.toml",
		DataFile:   "data00.json",
		Encode:     kafka.JSON,
		Tests: []TestQuery{
			{
				Query:        "Extract(Sort(All(), field=name), Rows(name), Rows(age), Rows(hobbies))",
				ExpectedResp: `{"results":[{"fields":[{"name":"name","type":"string"},{"name":"age","type":"int64"},{"name":"hobbies","type":"[]string"}],"columns":[{"column":"1","rows":["a",20,["hob1","hob2"]]},{"column":"2","rows":["b",21,["hob2","hob3"]]},{"column":"3","rows":["c",22,["hob3","hob4"]]},{"column":"4","rows":["d",23,["hob4","hob5"]]},{"column":"5","rows":["e",24,["hob5","hob6"]]},{"column":"6","rows":["f",26,["hob6","hob7"]]}]}]}`,
			},
		},
		CreateTableStmt: "(_id String, name String, age Int, hobbies StringSet)",
	},
	{ // two string keys
		ConfigFile: "config02.toml",
		DataFile:   "data00.json",
		Encode:     kafka.JSON,
		Tests: []TestQuery{
			{
				Query:        "Extract(Sort(All(), field=name), Rows(id), Rows(name), Rows(age), Rows(hobbies))",
				ExpectedResp: `{"results":[{"fields":[{"name":"id","type":"string"},{"name":"name","type":"string"},{"name":"age","type":"int64"},{"name":"hobbies","type":"[]string"}],"columns":[{"column":"1|a","rows":["1","a",20,["hob1","hob2"]]},{"column":"2|b","rows":["2","b",21,["hob2","hob3"]]},{"column":"3|c","rows":["3","c",22,["hob3","hob4"]]},{"column":"4|d","rows":["4","d",23,["hob4","hob5"]]},{"column":"5|e","rows":["5","e",24,["hob5","hob6"]]},{"column":"6|f","rows":["6","f",26,["hob6","hob7"]]}]}]}`,
			},
		},
		CreateTableStmt: "(_id String, id String, name String, age Int, hobbies StringSet)",
	},
	{ // string, id, and int
		ConfigFile: "config03.toml",
		DataFile:   "data00.json",
		Encode:     kafka.JSON,
		Tests: []TestQuery{
			{
				Query:        "Extract(Sort(All(), field=name), Rows(id), Rows(name), Rows(age), Rows(hobbies))",
				ExpectedResp: `{"results":[{"fields":[{"name":"id","type":"uint64"},{"name":"name","type":"string"},{"name":"age","type":"int64"},{"name":"hobbies","type":"[]string"}],"columns":[{"column":"1|a|20","rows":[1,"a",20,["hob2","hob1"]]},{"column":"2|b|21","rows":[2,"b",21,["hob2","hob3"]]},{"column":"3|c|22","rows":[3,"c",22,["hob3","hob4"]]},{"column":"4|d|23","rows":[4,"d",23,["hob4","hob5"]]},{"column":"5|e|24","rows":[5,"e",24,["hob5","hob6"]]},{"column":"6|f|26","rows":[6,"f",26,["hob6","hob7"]]}]}]}`,
			},
		},
		CreateTableStmt: "(_id String, id id, name String, age Int, hobbies StringSet)",
	},
}

// TestKafkaRunner takes as input a slice of RunnerTest structs
// For each RunnerTest, TestRunner:
//  1. Creates a kafkaRunner based on a config file
//  2. Reads data from a data file
//  3. Encodes that data
//  4. Writes the data to kafka
//  5. Runs the kafkaRunner
//  6. Confirms that the data was written to FeatureBase as expected
func TestKafkaRunner(t *testing.T) {
	for _, test := range kafkaRunnerTests {

		// define final path to config and data files
		var subpath string
		switch encode := test.Encode; encode {
		case kafka.JSON:
			subpath = kafka.JSON
		case kafka.Avro:
			subpath = kafka.Avro
		default:
			t.Fatalf("unsupported encoding type")
		}
		KafkaConfig := confPathPrefix + subpath + "/" + test.ConfigFile
		DataFile := dataPathPrefix + subpath + "/" + test.DataFile

		// create command and kafka runner
		fbsql := NewCommand(logger.StderrLogger)
		setupKafkaRunnerCommand(t, fbsql)

		// read config file so we can build a table before inserting data
		config, err := kafka.ConfigStructFromFile(KafkaConfig)
		if err != nil {
			t.Fatalf("building kafka.Config struct: %s", err)
		}

		// create table to write to (kafka runner does not currently create tables)
		createTable := strings.NewReader(fmt.Sprintf("CREATE TABLE %s %s;", config.Table, test.CreateTableStmt))
		_, err = fbsql.Queryer.Query(fbsql.organizationID, fbsql.databaseID, createTable)
		if err != nil {
			t.Fatalf("creating table: %s", err)
		}

		// delete all tables after test (note this is after all tests not after each test)
		defer func() {
			dropTable := strings.NewReader(fmt.Sprintf("DROP TABLE %s;", config.Table))
			_, err = fbsql.Queryer.Query(fbsql.organizationID, fbsql.databaseID, dropTable)
			if err != nil {
				t.Errorf("dropping table: %s", err)
			}
		}()

		// create runner and hijack hosts based on test env (i.e. not from configuration file or other)
		kafkaRunner, err := fbsql.newKafkaRunner(KafkaConfig)
		if err != nil {
			t.Fatalf("creating runner: %s", err)
		}
		kafkaRunner.FeaturebaseHosts = []string{pilosaHost}
		kafkaRunner.SchemaRegistryURL = registryHost
		kafkaRunner.KafkaBootstrapServers = []string{kafkaHost}
		kafkaRunner.FeaturebaseGRPCHosts = []string{pilosaGrpcHost}

		// read data that will go to kafka
		var records []map[string]interface{}
		records, err = recordsFromFile(DataFile)
		if err != nil {
			t.Errorf("reading raw messages from data file: %s", err)
		}

		// write data to kafka
		var messages [][]byte
		switch encode := test.Encode; encode {
		case kafka.JSON:
			messages, err = encodeJSONMessages(records)
		case kafka.Avro:
			t.Errorf("unsupported encoding type")
		default:
			t.Errorf("unsupported encoding type")
		}
		if err != nil {
			t.Fatal(err)
		}

		err = internal.ProduceMessages(kafkaRunner.Topics[0], strings.Join(kafkaRunner.KafkaBootstrapServers, ","), messages)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			// and delete the topic
			err = internal.DeleteTopic(kafkaRunner.Topics[0], strings.Join(kafkaRunner.KafkaBootstrapServers, ","))
			if err != nil {
				t.Fatal(err)
			}
		}()

		// Run it!!
		kafkaRunner.MaxMsgs = uint64(len(messages))
		err = kafkaRunner.Run()
		if err != nil {
			t.Fatalf("running runner: %s", err)
		}

		// and check the results...
		for _, q := range test.Tests {

			client := &http.Client{}
			var data = strings.NewReader(q.Query)
			req, err := http.NewRequest("POST", fmt.Sprintf("http://localhost:10101/index/%s/query", kafkaRunner.Index), data)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			respBody, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			require.JSONEq(t, q.ExpectedResp, string(respBody))
		}

	}
}

// Reads lines from a file and builds an in memory structure This function
// expects the file formated as new line delimited JSON
func recordsFromFile(pathToRecords string) (records []map[string]interface{}, err error) {
	var data map[string]interface{}

	recordsFile, err := os.Open(pathToRecords)
	if err != nil {
		return nil, fmt.Errorf("opening records file: %s", err)
	}
	defer recordsFile.Close()

	s := bufio.NewScanner(recordsFile)
	for s.Scan() {
		err := json.Unmarshal(s.Bytes(), &data)
		if err != nil {
			return nil, fmt.Errorf("unmarshal json: %s", err)
		}
		records = append(records, data)
		data = make(map[string]interface{})
	}
	return records, nil
}

// Mainly emmulate what happens in cli/cli.go Command.run() Unfortunately we
// cannot just use that function because we need to create our own kafkaRunner
// and update hosts and ports based on the test environment
func setupKafkaRunnerCommand(t *testing.T, fbsql *Command) {
	fbsql.Config.Host = strings.Split(pilosaHost, ":")[0]
	fbsql.Config.Port = strings.Split(pilosaHost, ":")[1]
	if err := fbsql.setupConfig(); err != nil {
		t.Fatalf("setting up config: %s", err)
	}

	// Check to see if Command needs to run in non-interactive mode.
	if len(fbsql.Commands) > 0 ||
		len(fbsql.Files) > 0 ||
		fbsql.Config.KafkaConfig != "" ||
		fbsql.Config.CSV {
		fbsql.nonInteractiveMode = true
	}

	if err := fbsql.setupClient(); err != nil {
		t.Fatalf("setting up client: %s", err)
	}

	// Print the connection info.
	if !fbsql.nonInteractiveMode {
		fbsql.printConnInfo()
	}

	if err := fbsql.connectToDatabase(fbsql.database); err != nil {
		t.Fatalf("connecting to database: %s", err)
		// We intentionally do not return err here.
	}
}

// Take a slice of JSON objects and convert them to a slice of byte slices that
// can be sent to and stored in kafka
func encodeJSONMessages(records []map[string]interface{}) ([][]byte, error) {
	var messages [][]byte
	for _, record := range records {
		encoded, err := json.Marshal(record)
		if err != nil {
			return nil, errors.Errorf("marshaling records to JSON: %s", err)
		}
		messages = append(messages, encoded)
	}
	return messages, nil
}
