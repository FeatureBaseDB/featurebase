package datagen

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/glycerine/vprint"
	pilosaclient "github.com/molecula/featurebase/v3/client"
	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/pkg/errors"

	"github.com/featurebasedb/featurebase/v3/idk/common"
	"github.com/featurebasedb/featurebase/v3/idk/kafka"
)

const (
	TargetFeaturebase = "featurebase"
	TargetKafka       = "kafka"
	TargetKafkaStatic = "kafkastatic"
	TargetMDS         = "mds"
)

// Main is the top-level datagen struct. It represents datagen-specific
// configuration parameters, as well as sub-level parameteres specific
// to each target.
type Main struct {
	idkMain *idk.Main

	KafkaPut *kafka.PutSource `flag:"-"`

	srcs    []idk.Source
	srcLock sync.Mutex

	cfg SourceGeneratorConfig

	Source string `short:"s" flag:"source" help:"Source generator type. Running datagen with no arguments will list the available source types."`
	Target string `short:"t" flag:"target" help:"Destination for the generated data: [featurebase, kafka, kafkastatic, mds]."`

	Concurrency int `short:"c" flag:"concurrency" help:"Number of concurrent sources and indexing routines to launch."`

	StartFrom uint64 `short:"b" help:"ID at which to start generating records."`
	EndAt     uint64 `short:"e" help:"ID at which to stop generating records."`

	Seed int64 `short:"" help:"Seed to use for any random number generation."`

	TrackProgress bool `short:"" help:"Periodically print status updates on how many records have been sourced."`

	UseShardTransactionalEndpoint bool `flag:"use-shard-transactional-endpoint" help:"Use experimental transactional endpoint"`

	CustomConfig string `short:"" help:"File from which to pull configuration for 'custom' source."`

	// Used strictly for configuration of the targets.
	Pilosa      PilosaConfig
	Kafka       KafkaConfig
	MDS         MDSConfig
	FeatureBase FeatureBaseConfig `flag:"featurebase" help:"qualified featurebase table"`

	DryRun bool `help:"Dry run - just flag parsing."`

	AuthToken string `flag:"auth-token" help:"Authentication token for FeatureBase"`
	Verbose   bool   `flag:"verbose" help:"Enable extended logging for debug"`
	Datadog   bool   `flag:"datadog" help:"Enable datadog profiling"`
}

// PilosaConfig is meant to represent the scoped (pilosa.*) configuration options
// to be used when target = pilosa. These are really just a sub-set of idk.Main,
// containing only those arguments that really apply to datagen.
type PilosaConfig struct {
	Hosts       []string `short:"" help:"Comma separated list of host:port pairs for FeatureBase."`
	Index       string   `short:"" help:"Name of FeatureBase index."`
	BatchSize   int      `short:"" help:"Number of records to read before indexing all of them at once. Generally, larger means better throughput and more memory usage. 1,048,576 might be a good number."`
	CacheLength uint64   `help:"Number of batches of ID mappings to cache."`
}

// FeatureBaseConfig is meant to represent the scoped (featurebase.*)
// configuration options to be used when target = mds. These are really just a
// sub-set of idk.Main, containing only those arguments that really apply to
// datagen.
type FeatureBaseConfig struct {
	OrganizationID string `flag:"org-id" short:"" help:"auto-assigned organization ID"`
	DatabaseID     string `flag:"db-id" short:"" help:"auto-assigned database ID"`
	TableName      string `flag:"table-name" short:"" help:"human friendly table name"`
}

// KafkaConfig is meant to represent the scoped (pilosa.*) configuration options
// to be used when target = kafka. These are really just a sub-set of kafka.PutSource,
// containing only those arguments that really apply to datagen.
type KafkaConfig struct {
	idk.ConfluentCommand
	Topic             string `short:"" help:"Kafka topic to post to."`
	Subject           string `short:"" help:"Kafka schema subject."`
	BatchSize         int    `short:"" help:"Number of records to generate before sending them to Kafka all at once. Generally, larger means better throughput and more memory usage."`
	ReplicationFactor int    `short:"" help:"set replication factor for kafka cluster"`
	NumPartitions     int    `short:"" help:"set partition for kafka cluster"`
}

// MDSConfig represents the configuration options to be used when target = mds.
// These are really just a sub-set of idk.Main, containing only those arguments
// that really apply to datagen.
type MDSConfig struct {
	Address string `short:"" help:"MDS host:port to connect to"`
}

// NewMain returns a new instance of Main.
func NewMain() *Main {
	return &Main{
		idkMain:  &idk.Main{},
		KafkaPut: &kafka.PutSource{},
		Target:   TargetFeaturebase,

		Concurrency: 1,
		Pilosa: PilosaConfig{
			CacheLength: 64,
		},
		Kafka: KafkaConfig{
			BatchSize:         1000,
			ReplicationFactor: 1,
			NumPartitions:     1,
		},
	}
}

// Sourcer is an interface describing a type which can generate sources,
// which in this case are `idk.Source`. It contains a few additional
// methods which are used to configure the source.
type Sourcer interface {
	Source(SourceConfig) idk.Source
	PrimaryKeyFields() []string
	DefaultEndAt() uint64
	Info() string
}

// SourceConfig is the configuration required by a Sourcer when creating
// a new Source.
type SourceConfig struct {
	startFrom uint64
	endAt     uint64
	total     uint64
	seed      int64
}

// sourcerTypes is a map of all available custom Sourcer
// implementations.
var sourcerTypes = map[string]func(SourceGeneratorConfig) Sourcer{
	"all-field-types":         NewAllFieldTypes,
	"bank":                    NewBank,
	"claim":                   NewClaim,
	"customer_segmentation":   NewPerson,
	"customer":                NewCustomer,
	"custom":                  NewCustom,
	"example":                 NewExample,
	"equipment":               NewEquipment,
	"network_ts":              NewNetwork,
	"power_ts":                NewTimeseries,
	"site":                    NewSite,
	"transactions_ts":         NewTransaction,
	"item":                    NewItem,
	"kitchensink":             NewKitchenSink,
	"kitchensink_keyed":       NewKitchenSinkKeyed,
	"sizing":                  NewSizing,
	"stringpk":                NewStringPK,
	"transactions_scenario_1": NewTransaction1,
	"warranty":                NewWarranty,
}

var sourcerTypeGroups = map[string][]string{
	"iot": {
		"equipment",
		"network_ts",
		"power_ts",
		"site",
		"transactions_ts",
	},
}

func (m *Main) info() string {
	// Sort the sourcerTypes map by key.
	keys := make([]string, 0, len(sourcerTypes))
	for k := range sourcerTypes {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	ret := "\n------------------------------------------\n"
	ret += "A source must be specified with --source\n"
	ret += "The following source types are supported:\n\n"

	alreadyPrinted := make(map[string]struct{})

	for group, keys := range sourcerTypeGroups {
		ret += fmt.Sprintf("    %s\n", group)
		for _, k := range keys {
			info, _ := typeInfo(k)
			ret += fmt.Sprintf("        %-21s: %s\n", k, info)
			alreadyPrinted[k] = struct{}{}
		}
	}
	for _, k := range keys {
		if _, ok := alreadyPrinted[k]; ok {
			continue
		}
		info, _ := typeInfo(k)
		ret += fmt.Sprintf("    %-25s: %s\n", k, info)
	}
	ret += "\n"
	return ret
}

func typeInfo(key string) (string, error) {
	srcrFn, ok := sourcerTypes[key]
	if !ok {
		return "", errors.Errorf("invalid key: %s", key)
	}
	src := srcrFn(SourceGeneratorConfig{})
	return src.Info(), nil
}

// Preload configures the Sources based on the concurrency
// and start/end range.
func (m *Main) Preload() error {
	if m.Source == "" {
		return errors.New(m.info())
	}

	cfg := SourceGeneratorConfig{
		StartFrom:    m.StartFrom,
		EndAt:        m.EndAt,
		Concurrency:  m.Concurrency,
		Seed:         m.Seed,
		CustomConfig: m.CustomConfig,
	}

	pks, srcs, err := m.makeSources(m.Source, cfg)
	if err != nil {
		return errors.Wrap(err, "getting sources")
	}
	m.idkMain = idk.NewMain()
	// TODO: this is a little hacky because of the complexity around
	// PrimaryKeyFields and IDField, and the fact that those are
	// currently being set before having a schema.
	// See: https://github.com/featurebasedb/featurebase/v3/idk/issues/147
	switch {
	case len(pks) == 1 && pks[0] == "id":
		m.idkMain.IDField = "id"
	case len(pks) > 0:
		m.idkMain.IDField = ""
		m.idkMain.PrimaryKeyFields = pks
	default:
		m.idkMain.AutoGenerate = true
		m.idkMain.ExternalGenerate = true
	}
	switch m.Target {
	case TargetKafka, TargetKafkaStatic:
		m.KafkaPut, _ = kafka.NewPutSource()
		m.KafkaPut.Concurrency = m.Concurrency
		m.KafkaPut.NewSource = m.newSource
		m.KafkaPut.TrackProgress = m.TrackProgress
		m.KafkaPut.BatchSize = m.Kafka.BatchSize
		m.KafkaPut.ConfluentCommand = m.Kafka.ConfluentCommand
		m.KafkaPut.ConfigMap, err = common.SetupConfluent(&m.Kafka.ConfluentCommand)
		m.KafkaPut.Target = m.Target
		if err != nil {
			return errors.Wrap(err, "setup confluent")
		}
		if m.Kafka.Topic != "" {
			m.KafkaPut.Topic = m.Kafka.Topic
		}
		if m.Kafka.Subject != "" {
			m.KafkaPut.Subject = m.Kafka.Subject
		}
		if m.Verbose {
			m.KafkaPut.Log = logger.NewVerboseLogger(os.Stderr)
		}
		m.KafkaPut.ReplicationFactor = m.Kafka.ReplicationFactor
		m.KafkaPut.NumPartitions = m.Kafka.NumPartitions
		m.KafkaPut.FBPrimaryKeyFields = m.idkMain.PrimaryKeyFields
		m.KafkaPut.FBIDField = m.idkMain.IDField
		m.KafkaPut.FBIndexName = m.Pilosa.Index

	case TargetMDS:
		m.idkMain.Namespace = "ingester_datagen"
		m.idkMain.Concurrency = m.Concurrency
		m.idkMain.CacheLength = m.Pilosa.CacheLength
		m.idkMain.NewSource = m.newSource
		m.idkMain.TrackProgress = m.TrackProgress
		m.idkMain.AuthToken = m.AuthToken
		m.idkMain.UseShardTransactionalEndpoint = m.UseShardTransactionalEndpoint
		if m.Pilosa.BatchSize > 0 {
			m.idkMain.BatchSize = m.Pilosa.BatchSize
		}

		// MDS-specific
		m.idkMain.MDSAddress = m.MDS.Address
		m.idkMain.OrganizationID = dax.OrganizationID(m.FeatureBase.OrganizationID)
		m.idkMain.DatabaseID = dax.DatabaseID(m.FeatureBase.DatabaseID)
		m.idkMain.TableName = dax.TableName(m.FeatureBase.TableName)
		m.idkMain.PackBools = ""

	default:
		m.idkMain.Namespace = "ingester_datagen"
		m.idkMain.Concurrency = m.Concurrency
		m.idkMain.CacheLength = m.Pilosa.CacheLength
		m.idkMain.NewSource = m.newSource
		m.idkMain.TrackProgress = m.TrackProgress
		m.idkMain.AuthToken = m.AuthToken
		m.idkMain.UseShardTransactionalEndpoint = m.UseShardTransactionalEndpoint
		if len(m.Pilosa.Hosts) > 0 {
			m.idkMain.PilosaHosts = m.Pilosa.Hosts
		}
		m.idkMain.Index = m.Pilosa.Index
		if m.Pilosa.BatchSize > 0 {
			m.idkMain.BatchSize = m.Pilosa.BatchSize
		}
	}

	m.srcs = srcs

	return nil
}

// Sources returns the list of sources. This is typically
// called by tests, after the generator has created the sources
// during Preload().
func (m *Main) Sources() []idk.Source {
	return m.srcs
}

// Run generates data for the specified target.
func (m *Main) Run() error {
	switch m.Target {
	case TargetKafka, TargetKafkaStatic:
		return m.KafkaPut.Run()
	default:
		return m.idkMain.Run()
	}
}

func (m *Main) PrintPlan() {
	cfg := m.cfg
	startFrom := cfg.StartFrom
	endAt := cfg.EndAt - 1 // adjust for the implementation detail `endAt++` in generator.Sources
	concurrency := cfg.Concurrency
	RecordCount := endAt - startFrom

	switch m.Target {
	case TargetKafka:
		Topic := m.Kafka.Topic
		NumPartitions := m.Kafka.NumPartitions
		ReplicationFactor := m.Kafka.ReplicationFactor
		Subject := m.Kafka.Subject
		Hosts := m.KafkaPut.KafkaBootstrapServers
		RegURL := m.KafkaPut.SchemaRegistryURL
		if m.Kafka.Topic == "" {
			Topic = m.KafkaPut.Topic
		}
		if m.Kafka.Subject == "" {
			Subject = m.KafkaPut.Subject
		}
		fmt.Printf(`Datagen config:
		hosts:			%s
		start id:		%s
		end id:			%s
		total generated:	%s
		concurrency:            %d
		registry URL:		%s
		topic:			%s
		subject:		%s
		num partitions: %d
		replication factor: %d
`,
			strings.Join(Hosts, ", "),
			AddThousandSep(startFrom),
			AddThousandSep(endAt),
			AddThousandSep(RecordCount),
			concurrency,
			RegURL,
			Topic,
			Subject,
			NumPartitions,
			ReplicationFactor,
		)
	case TargetKafkaStatic:
		Topic := m.Kafka.Topic
		NumPartitions := m.Kafka.NumPartitions
		ReplicationFactor := m.Kafka.ReplicationFactor
		Subject := m.Kafka.Subject
		Hosts := m.KafkaPut.KafkaBootstrapServers
		if m.Kafka.Topic == "" {
			Topic = m.KafkaPut.Topic
		}
		if m.Kafka.Subject == "" {
			Subject = m.KafkaPut.Subject
		}
		fmt.Printf(`Datagen config:
		hosts:			%s
		start id:		%s
		end id:			%s
		total generated:	%s
		concurrency:            %d
		topic:			%s
		subject:		%s
		num partitions: %d
		replication factor: %d
`,
			strings.Join(Hosts, ", "),
			AddThousandSep(startFrom),
			AddThousandSep(endAt),
			AddThousandSep(RecordCount),
			concurrency,
			Topic,
			Subject,
			NumPartitions,
			ReplicationFactor,
		)
	default:
		Hosts := m.Pilosa.Hosts
		BatchSize := m.Pilosa.BatchSize
		if len(m.Pilosa.Hosts) <= 0 {
			Hosts = m.idkMain.PilosaHosts
		}
		if m.Pilosa.BatchSize <= 0 {
			BatchSize = m.idkMain.BatchSize
		}
		BatchCount := uint64(math.Ceil(float64(RecordCount) / float64(BatchSize)))

		if m.Pilosa.Index == "" {
			m.Pilosa.Index = "(not specified)"
		}
		fmt.Printf(`Datagen config:
		hosts:           %s
		index:           %s
		start id:        %s
		end id:          %s
		total generated: %s
		concurrency:     %d
		batch size:      %s
		total batches:   %s
		use shard trans: %v
`,
			strings.Join(Hosts, ", "),
			m.Pilosa.Index,
			AddThousandSep(startFrom),
			AddThousandSep(endAt),
			AddThousandSep(RecordCount),
			concurrency,
			AddThousandSep(uint64(BatchSize)),
			AddThousandSep(BatchCount),
			m.UseShardTransactionalEndpoint,
		)

		fmt.Println("Schema:")
		for _, field := range m.srcs[0].Schema() {
			fmt.Printf("%T %+[1]v\n", field)
		}

	}
}

func AddThousandSep(num uint64) string {
	in := strconv.FormatUint(num, 10)
	out := ""
	for i := 1; i <= len(in); i++ {
		out = string(in[len(in)-i]) + out
		if i != 0 && i != len(in) && i%3 == 0 {
			out = "," + out
		}
	}
	return out
}

// newSource pops the next idk.Source from srcs.
func (m *Main) newSource() (source idk.Source, err error) {
	m.srcLock.Lock()
	defer m.srcLock.Unlock()

	if len(m.srcs) < 1 {
		return nil, errors.New("requested too many sources")
	}
	source = m.srcs[0]
	m.srcs = m.srcs[1:]
	return source, nil
}

// PilosaClient is a helper for tests. The datagen command
// used to embed idk.Main, so datagen tests could call
// Main.PilosaClient(), but idk.Main is no longer
// directly embedded.
func (m *Main) PilosaClient() *pilosaclient.Client {
	if m.idkMain == nil {
		return nil
	}
	return m.idkMain.PilosaClient()
}

// NoStats is a helper for tests.
func (m *Main) NoStats() {
	if m.idkMain != nil {
		m.idkMain.Stats = ""
	}
}

// Sources implements the SourceGenerator interface.
func (m *Main) makeSources(key string, cfg SourceGeneratorConfig) ([]string, []idk.Source, error) {
	srcrFn, ok := sourcerTypes[key]
	if !ok {
		return nil, nil, errors.Errorf("invalid key: %s", key)
	}
	srcr := srcrFn(cfg)

	// Get the primary key fields for the source.
	pks := srcr.PrimaryKeyFields()

	startFrom, endAt := cfg.StartFrom, cfg.EndAt

	if endAt == 0 {
		if dflt := srcr.DefaultEndAt(); dflt > 0 {
			endAt = dflt
		} else {
			endAt = startFrom + 99 // Default to 100 records.
		}
	}
	if startFrom > endAt {
		return nil, nil, errors.Errorf("invalid start/end: %d/%d", startFrom, endAt)
	}
	endAt++ // endAt is used as `a.cur >= a.endAt`, so increment it to get the expected, inclusive, behavior.

	// Calculate the concurrency, total, and start/end boundaries
	// based on the configuration values.
	m.cfg = SourceGeneratorConfig{
		StartFrom:    startFrom,
		EndAt:        endAt,
		Concurrency:  cfg.Concurrency,
		Seed:         cfg.Seed,
		CustomConfig: cfg.CustomConfig,
	}
	concurrency, startEnds, total := startEnds(m.cfg)

	seedGen := rand.New(rand.NewSource(cfg.Seed))

	srcs := make([]idk.Source, concurrency)

	// Distribute the total range of records over a number of
	// Sources equal to the concurrency.
	for i := 0; i < concurrency; i++ {
		srcCfg := SourceConfig{
			startFrom: startEnds[i].start,
			endAt:     startEnds[i].end,
			total:     total,
			seed:      cfg.Seed,
		}
		vprint.VV("source config for i: %d, %#v", i, srcCfg)

		src := srcr.Source(srcCfg)

		// TODO: pull seed from config?
		if s, ok := src.(Seedable); ok {
			s.Seed(seedGen.Int63())
		}

		srcs[i] = src
	}

	return pks, srcs, nil
}

// startEnds returns the start/end boundaries for each source
// run needed based on cfg.StartFrom, cfg.EndAt, and cfg.Concurrency.
// It also returns the calculated total, and the final concurrency
// (in case it had to be adjusted).
// returns (concurrency, startEnds, total)
func startEnds(cfg SourceGeneratorConfig) (int, []startEnd, uint64) {
	var startFrom uint64 = cfg.StartFrom
	var endAt uint64 = cfg.EndAt
	var concurrency int = cfg.Concurrency

	if concurrency == 0 {
		return 0, []startEnd{}, 0
	}

	total := endAt - startFrom
	if uint64(concurrency) > total {
		concurrency = int(total)
	}

	var current uint64

	interval := total / uint64(concurrency)
	if startFrom > current {
		current = startFrom
	}
	leftover := total - (interval * uint64(concurrency))

	ses := make([]startEnd, concurrency)

	// Distribute the total range of records over a number of
	// Sources equal to the concurrency.
	for i := 0; i < concurrency; i++ {
		var start uint64 = current
		var end uint64
		current += interval
		// Include one of the leftovers in each iteration until there
		// are no more.
		if uint64(i) < leftover {
			current++
		}
		// start is inclusive, end is exclusive
		end = current

		ses[i] = startEnd{
			start: start,
			end:   end,
		}
	}

	return concurrency, ses, total
}

type record []interface{}

func (r record) Commit(ctx context.Context) error { return nil }

func (r record) Data() []interface{} {
	return r
}

type startEnd struct {
	start uint64
	end   uint64
}
