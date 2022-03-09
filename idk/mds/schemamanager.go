package mds

import (
	"context"
	"time"

	featurebase "github.com/molecula/featurebase/v3"
	featurebase_client "github.com/molecula/featurebase/v3/client"
	"github.com/molecula/featurebase/v3/dax"
	mdsclient "github.com/molecula/featurebase/v3/dax/mds/client"
	"github.com/molecula/featurebase/v3/errors"
)

// Ensure type implements interface.
// var _ idk.SchemaManager = &schemaManager{}

// schemaManager
type schemaManager struct {
	client *mdsclient.Client
	qual   dax.TableQualifier
}

func NewSchemaManager(mdsAddress dax.Address, qual dax.TableQualifier) *schemaManager {
	return &schemaManager{
		client: mdsclient.New(mdsAddress),
		qual:   qual,
	}
}

func (s *schemaManager) StartTransaction(id string, timeout time.Duration, exclusive bool, requestTimeout time.Duration) (*featurebase.Transaction, error) {
	return nil, nil
}
func (s *schemaManager) FinishTransaction(id string) (*featurebase.Transaction, error) {
	return nil, nil
}
func (s *schemaManager) Schema() (*featurebase_client.Schema, error) {
	// Create a temp schema object to mimic what the FeatureBase client Schema()
	// method returns.
	schema := featurebase_client.NewSchema()

	tables, err := s.client.Tables(context.Background(), s.qual)
	if err != nil {
		return nil, err
	}

	for _, qtbl := range tables {
		idx := schema.Index(string(qtbl.Key()), featurebase_client.OptIndexKeys(qtbl.StringKeys()))
		for _, fld := range qtbl.Fields {
			opts := make([]featurebase_client.FieldOption, 0)

			switch fld.Type {
			case dax.FieldTypeBool:
				opts = append(opts, featurebase_client.OptFieldTypeBool())
			case dax.FieldTypeDecimal:
				opts = append(opts, featurebase_client.OptFieldTypeDecimal(
					fld.Options.Scale,
				))
			case dax.FieldTypeID:
				opts = append(opts, featurebase_client.OptFieldTypeMutex(
					featurebase_client.CacheType(fld.Options.CacheType),
					int(fld.Options.CacheSize),
				))
			case dax.FieldTypeIDSet:
				opts = append(opts, featurebase_client.OptFieldTypeSet(
					featurebase_client.CacheType(fld.Options.CacheType),
					int(fld.Options.CacheSize),
				))
			case dax.FieldTypeInt:
				opts = append(opts, featurebase_client.OptFieldTypeInt(
					fld.Options.Min.ToInt64(0),
					fld.Options.Max.ToInt64(0),
				))
			case dax.FieldTypeString:
				opts = append(opts,
					featurebase_client.OptFieldTypeMutex(
						featurebase_client.CacheType(fld.Options.CacheType),
						int(fld.Options.CacheSize),
					),
					featurebase_client.OptFieldKeys(true),
				)
			case dax.FieldTypeStringSet:
				opts = append(opts,
					featurebase_client.OptFieldTypeSet(
						featurebase_client.CacheType(fld.Options.CacheType),
						int(fld.Options.CacheSize),
					),
					featurebase_client.OptFieldKeys(true),
				)
			case dax.FieldTypeTimestamp:
				opts = append(opts, featurebase_client.OptFieldTypeTimestamp(
					featurebase_client.DefaultEpoch,
					fld.Options.TimeUnit,
				))

			default:
				return nil, errors.Errorf("unsupported field type: %s (%s)", fld.Name, fld.Type)
			}

			_ = idx.Field(string(fld.Name), opts...)
		}
	}

	return schema, nil
}
func (s *schemaManager) SyncIndex(index *featurebase_client.Index) error {
	return nil
}
func (s *schemaManager) DeleteIndex(index *featurebase_client.Index) error {
	return nil
}
func (s *schemaManager) Status() (featurebase_client.Status, error) {
	return featurebase_client.Status{}, nil
}
func (s *schemaManager) SetAuthToken(token string) {}
