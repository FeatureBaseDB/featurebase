package cli_test

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"testing"

	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/cli"
	"github.com/featurebasedb/featurebase/v3/cli/kafka"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/idk/kafka/csrc"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/stretchr/testify/require"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"

	avro "github.com/linkedin/goavro/v2"
)

// Struct that contains the services required to run the kafka runner tests
type KafkaRunnerTestServices struct {
	featurebaseHost     string
	featurebaseGRPCHost string
	kafkaHost           string
	registryHost        string
}

func envOr(envName, defaultVal string) string {
	if val, ok := os.LookupEnv(envName); !ok {
		return defaultVal
	} else {
		return val
	}
}

func getKafkaRunnerTestServices() *KafkaRunnerTestServices {
	return &KafkaRunnerTestServices{
		featurebaseHost:     envOr("KAFKA_RUNNER_TEST_FEATUREBASE_HOST", "localhost:10101"),
		featurebaseGRPCHost: envOr("KAFKA_RUNNER_TEST_FEATUREBASEGRPC_HOST", "localhost:20101"),
		kafkaHost:           envOr("KAFKA_RUNNER_TEST_KAFKA_HOST", "localhost:9092"),
		registryHost:        envOr("KAFKA_RUNNER_TEST_REGISTRY_HOST", "localhost:8081"),
	}
}

// Struct used for TestRunner which contains the information needed to ingest
// data to kafka, configure the runner, and test that the runner successfully
// ran.
type kafkaRunnerTest struct {
	ConfigFile      string // path to the configuration file used for the runner
	DataFile        string // path to the data file to populate kafka with
	CreateTableStmt string // statement used to create table prior to ingest
	SchemaFile      string
	Tests           []testQuery // list of test which are 2-tuples of query and expected results

}

// Struct which contains a query to run agaisnt featurebase and the expected
// results from that query.
type testQuery struct {
	Query        string
	ExpectedResp string
}

// A slice of KafkaRunnerTest structs that will be used in TestKafkaRunner test
// function.
var kafkaRunnerTests = []kafkaRunnerTest{
	{ // id keys
		ConfigFile: "config00.toml",
		DataFile:   "data00.json",
		Tests: []testQuery{
			{
				Query:        "select * from <TABLE> order by name",
				ExpectedResp: `[[1,"a",20,["hob1","hob2"]],[2,"b",21,["hob2","hob3"]],[3,"c",22,["hob3","hob4"]],[4,"d",23,["hob4","hob5"]],[5,"e",24,["hob5","hob6"]],[6,"f",26,["hob6","hob7"]]]`,
			},
		},
		CreateTableStmt: "(_id ID, name String, age Int, hobbies StringSet)",
	},
	{ // string keys
		ConfigFile: "config01.toml",
		DataFile:   "data00.json",
		Tests: []testQuery{
			{
				Query:        "select * from <TABLE> order by name",
				ExpectedResp: `[["1","a",20,["hob1","hob2"]],["2","b",21,["hob2","hob3"]],["3","c",22,["hob3","hob4"]],["4","d",23,["hob4","hob5"]],["5","e",24,["hob5","hob6"]],["6","f",26,["hob6","hob7"]]]`,
			},
		},
		CreateTableStmt: "(_id String, name String, age Int, hobbies StringSet)",
	},
	{ // two string keys
		ConfigFile: "config02.toml",
		DataFile:   "data00.json",
		Tests: []testQuery{
			{
				Query:        "select * from <TABLE> order by name",
				ExpectedResp: `[["1|a","1","a",20,["hob1","hob2"]],["2|b","2","b",21,["hob2","hob3"]],["3|c","3","c",22,["hob3","hob4"]],["4|d","4","d",23,["hob4","hob5"]],["5|e","5","e",24,["hob5","hob6"]],["6|f","6","f",26,["hob6","hob7"]]]`,
			},
		},
		CreateTableStmt: "(_id String, id String, name String, age Int, hobbies StringSet)",
	},
	{ // string, id, and int
		ConfigFile: "config03.toml",
		DataFile:   "data00.json",
		Tests: []testQuery{
			{
				Query:        "select * from <TABLE> order by name",
				ExpectedResp: `[["1|a|20",1,"a",20,["hob1","hob2"]],["2|b|21",2,"b",21,["hob2","hob3"]],["3|c|22",3,"c",22,["hob3","hob4"]],["4|d|23",4,"d",23,["hob4","hob5"]],["5|e|24",5,"e",24,["hob5","hob6"]],["6|f|26",6,"f",26,["hob6","hob7"]]]`,
			},
		},
		CreateTableStmt: "(_id String, id id, name String, age Int, hobbies StringSet)",
	},
	{ // missing values
		ConfigFile: "config05.toml",
		DataFile:   "data02.json",
		Tests: []testQuery{
			{
				Query:        "select * from <TABLE> order by name",
				ExpectedResp: `[["3",null,22,["hob3","hob4"]],["1","a",20,["hob1","hob2"]],["2","b",21,["hob2","hob3"]],["4","d",null,["hob4","hob5"]],["5","e",24,null],["6","f",26,["hob6","hob7"]]]`,
			},
		},
		CreateTableStmt: "(_id String, name String, age Int, hobbies StringSet)",
	},
	/*{ // id keys
		ConfigFile: "config04.toml",
		DataFile:   "data01.json",
		Encode:     "avro",
		SchemaFile: "schema01.json",
		Tests: []testQuery{
			{
				Query:        "Extract(Sort(All(), field=name), Rows(name), Rows(age), Rows(hobbies))",
				ExpectedResp: `{"results":[{"fields":[{"name":"name","type":"string"},{"name":"age","type":"int64"},{"name":"hobbies","type":"[]string"}],"columns":[{"column":1,"rows":["a",20,["hob2","hob1"]]},{"column":2,"rows":["b",21,["hob2","hob3"]]},{"column":3,"rows":["c",22,["hob3","hob4"]]},{"column":4,"rows":["d",23,["hob4","hob5"]]},{"column":5,"rows":["e",24,["hob5","hob6"]]},{"column":6,"rows":["f",26,["hob6","hob7"]]}]}]}`,
			},
		},
		CreateTableStmt: "(_id string, string_string string, string_bytes string, pk2 stringset, stringset_bytes stringset, idset_long idset, decimal_double decimal(2), timestamp_bytes_ts timestamp, idset_longarray idset, dateint_bytes_ts int, bools stringset, stringset_string stringset, stringset_stringarray stringset, idset_int idset, timestamp_bytes_int timestamp, int_long int, id_long id, id_int id, idset_intarray idset, decimal_float decimal(2), bools-exists stringset, stringset_bytesarray stringset, int_int int, decimal_bytes decimal(2), pk1 stringset)",
	},*/
}

// TestKafkaRunner takes as input a slice of kafkaRunnerTest structs.
// For each kafkaRunnerTest, TestKafkaRunner:
//  1. Creates and configues a new cli.Command
//  2. Reads kafka messages from a data file
//  3. Encodes that data
//  4. Writes the data to kafka
//  5. Runs the cli.Command
//  6. Confirms that the data was written to FeatureBase as expected
func TestKafkaRunner(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Get host and port pair for services required for test (e.g. kafka and
	// featurebase)
	services := getKafkaRunnerTestServices()

	for _, test := range kafkaRunnerTests {

		// define path to config and data files
		kafkaConfig := "./kafka/testdata/runner/config/" + test.ConfigFile
		dataFile := "./kafka/testdata/runner/data/" + test.DataFile
		schemaFile := "./kafka/testdata/runner/schema/" + test.SchemaFile

		// read data that will go to kafka
		var records []map[string]interface{}
		records, err := recordsFromFile(dataFile)
		if err != nil {
			t.Errorf("reading raw messages from data file: %s", err)
		}

		// copy config file and replace values as defined in findAndReplace
		var findAndReplace = map[string]string{
			"KAFKA_SERVICE":           services.kafkaHost,
			"SCHEMA_REGISTRY_SERVICE": services.registryHost,
			"MAX_MESSAGES":            strconv.Itoa(len(records)),
		}
		if err := createTempFindAndReplace(kafkaConfig, findAndReplace); err != nil {
			t.Fatalf("creating temp config file: %s", err)
		}
		defer os.Remove(kafkaConfig + ".tmp")

		// create new command
		fbsql := cli.NewCommand(logger.StderrLogger)

		fbsql.Config.Host = strings.Split(services.featurebaseHost, ":")[0]
		fbsql.Config.Port = strings.Split(services.featurebaseHost, ":")[1]
		fbsql.Run(context.Background())                 // creates fbsql's Queryer which we can then use below
		fbsql.Config.KafkaConfig = kafkaConfig + ".tmp" // when fbsql comes back, add kafka config

		config, err := kafka.ConfigFromFile(kafkaConfig + ".tmp")
		if err != nil {
			t.Fatal(err)
		}

		// create table to write to (kafka runner does not currently create tables)
		createTable := strings.NewReader(fmt.Sprintf("CREATE TABLE %s %s;", config.Table, test.CreateTableStmt))
		wqr, err := fbsql.Queryer.Query("", "", createTable)
		if err != nil {
			t.Fatalf("creating table: %s", err)
		}

		if wqr.Error != "" {
			t.Error(wqr.Error)
		}

		// delete all tables after test (note this is after all tests not after each test)
		defer func() {
			dropTable := strings.NewReader(fmt.Sprintf("DROP TABLE %s;", config.Table))
			_, err = fbsql.Queryer.Query("", "", dropTable)
			if err != nil {
				t.Errorf("dropping table: %s", err)
			}
		}()

		// write data to kafka
		var messages [][]byte
		switch encode := config.Encode; encode {
		case "json":
			messages, err = encodeJSONMessages(records)
			if err != nil {
				t.Fatal(err)
			}
		case "avro":
			// get avro schema as string
			schemaBytes, err := os.ReadFile(schemaFile)
			if err != nil {
				t.Fatal(err)
			}
			schema := string(schemaBytes)

			// post the schema to schema registry
			schemaID, err := postSchema(schema, "kafka-runner-subject", services.registryHost)
			if err != nil {
				t.Fatal(err)
			}

			// encode avro messages
			messages, err = encodeAvroMessages(records, schema, schemaID)
			if err != nil {
				t.Fatal(err)
			}
		default:
			t.Errorf("unsupported encoding type")
		}

		if err = createTopic(config.Topics[0], services.kafkaHost); err != nil {
			t.Fatal(err)
		}

		if err = produceMessages(config.Topics[0], services.kafkaHost, messages); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err = deleteTopic(config.Topics[0], services.kafkaHost); err != nil {
				t.Fatal(err)
			}
		}()

		if err = fbsql.Run(context.Background()); err != nil {
			t.Fatal(err)
		}

		// and check the results...
		for _, q := range test.Tests {
			query := strings.Replace(q.Query, "<TABLE>", config.Table, 1)
			if resp, err := fbsql.Queryer.Query("", "", strings.NewReader(query)); err != nil {
				t.Fatal(err)
			} else {
				verifyQueryReponse(t, resp, q.ExpectedResp)
			}
		}
	}
}

// createTempFindAndReplace reads a file (source), replaces substrings specified
// in mapping, and writes the output to the same path as souce, appending
// ".tmp".
func createTempFindAndReplace(source string, mapping map[string]string) error {
	//Read all the contents of the  original file
	bytesRead, err := ioutil.ReadFile(source)
	if err != nil {
		return errors.Errorf("%s", err)
	}

	stringRead := string(bytesRead)

	for find, replace := range mapping {
		stringRead = strings.Replace(stringRead, find, replace, 1)
	}

	//Copy all the contents to the desitination file
	if err = ioutil.WriteFile(source+".tmp", []byte(stringRead), 0755); err != nil {
		return errors.Errorf("%s", err)
	}

	return nil

}

// verifyQueryResponse compares the JSON binary representation of the Data field
// in an WireQueryResponse against some expected string. All lists in the Data
// field of the WireQueryResponse are sorted and then serialized. The test that
// calls this function fails if the query responses are different. In this case,
// the diff is displayed.
func verifyQueryReponse(t *testing.T, wqr *featurebase.WireQueryResponse, expectedQuery string) {
	// we need to sort slices so json compare is accurate
	var data [][]interface{}
	for _, line := range wqr.Data {
		var newline []interface{}
		for _, element := range line {
			switch newElement := element.(type) {
			case featurebase.StringSet:
				newline = append(newline, newElement.SortedStringSlice())
			case featurebase.IDSet:
				newline = append(newline, newElement.SortedInt64Slice())
			default:
				newline = append(newline, element)
			}
		}
		data = append(data, newline)
	}
	js, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	//t.Fatal(string(js))
	require.JSONEq(t, expectedQuery, string(js))
}

// recordsFromFile reads lines from a file and builds an in memory structure.
// The file pointed to by pathToRecords must be formated as new line delimited
// JSON.
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

// encodeJSONMessages takes a slice of JSON objects and convert them to a slice
// of byte slices which are the binary representation of JSON.
func encodeJSONMessages(records []map[string]interface{}) ([][]byte, error) {
	messages := make([][]byte, len(records))
	for i, record := range records {
		encoded, err := json.Marshal(record)
		if err != nil {
			return nil, errors.Errorf("marshaling records to JSON: %s", err)
		}
		messages[i] = encoded
	}
	return messages, nil
}

// encodeAvroMessages takes a slice of JSON objects and convert them to a slice
// of byte slices which are the binary avro encoding based on schema with a
// specific schemaID.
func encodeAvroMessages(records []map[string]interface{}, schema string, schemaID int) ([][]byte, error) {

	// get a thing which can encode a byte slice based on a schema
	avroEncoder, err := avro.NewCodec(schema)
	if err != nil {
		return nil, errors.Errorf("getting avro encoder: %s", err)
	}

	messages := make([][]byte, len(records))
	for i, record := range records {
		buf := make([]byte, 5, 1000)
		buf[0] = 0
		binary.BigEndian.PutUint32(buf[1:], uint32(schemaID))
		buf, err = avroEncoder.BinaryFromNative(buf, record)
		if err != nil {
			return nil, errors.Errorf("avro encoding record: %s", err)
		}
		messages[i] = buf
	}
	return messages, nil
}

// postSchema takes an avro schema and subject and posts it to schema registry
// at a specific url.
func postSchema(schema, subj, schemaRegistryURL string) (schemaID int, err error) {
	schemaClient := csrc.NewClient("http://"+schemaRegistryURL, nil, nil)
	resp, err := schemaClient.PostSubjects(subj, schema)
	if err != nil {
		return 0, errors.Wrap(err, "posting schema")
	}
	return resp.ID, nil
}

// createTopic creates a kafka topic on the kafka server pointed to by
// bootstrapServers
func createTopic(topic string, bootstrapServers string) error {

	// create admin client needed to create topic, defer closing
	ac, err := confluent.NewAdminClient(&confluent.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		return errors.Errorf("creating admin client: %s", err)
	}
	defer ac.Close()

	// create a topic
	var ts = []confluent.TopicSpecification{
		{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}
	_, err = ac.CreateTopics(context.Background(), ts, nil)
	if err != nil {
		return errors.Errorf("creating topics: %s", err)
	}
	// caller must delete if needed

	return nil
}

// produceMessages produces messages to a kafka topic on the kafka server
// pointed to by bootstrapServer. Messages should be byte slices.
func produceMessages(topic string, bootstrapServers string, messages [][]byte) error {

	// create producer, defer closing
	p, err := confluent.NewProducer(&confluent.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		return errors.Errorf("creating producer: %s", err)
	}
	defer p.Close()

	for _, message := range messages {
		err := p.Produce(&confluent.Message{
			TopicPartition: confluent.TopicPartition{Topic: &topic, Partition: confluent.PartitionAny},
			Value:          message,
		}, nil)
		if err != nil {
			return errors.Errorf("producing messages: %s", err)
		}
		p.Flush(3000)
	}

	return nil

}

// deleteTopic deletes a kafka topic on the kafka server pointed to by
// bootstrapServers
func deleteTopic(topic string, bootstrapServers string) error {

	// create admin client needed to create topic, defer closing
	ac, err := confluent.NewAdminClient(&confluent.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		return errors.Errorf("creating admin client: %s", err)
	}
	defer ac.Close()

	// delete a topic, catch any errors
	results, err := ac.DeleteTopics(context.Background(), []string{topic}, nil)
	if err != nil {
		return errors.Errorf("deleting topic: %s", err)
	}

	for _, result := range results {
		if result.Error.Code() != confluent.ErrNoError {
			return errors.Errorf("fatal error deleting topic: %s", result.String())
		}
	}

	return nil

}
