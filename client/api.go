package client

import (
	"context"

	featurebase "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/errors"
)

var _ featurebase.SchemaAPI = &schemaAPI{}

// schemaAPI is a featurebase client wrapper which implements the
// featurebase.SchemaAPI interface. This was introduced for use in batch tests
// when we decoupled Batch from the client package. In other words, this is only
// used for those tests, it may not be functionally complete, and should not be
// used otherwise without further testing and review of this code.
type schemaAPI struct {
	*Client
}

func NewSchemaAPI(c *Client) *schemaAPI {
	return &schemaAPI{
		Client: c,
	}
}

func (s *schemaAPI) CreateIndexAndFields(ctx context.Context, indexName string, options featurebase.IndexOptions, fields []featurebase.CreateFieldObj) error {
	schema, err := s.Client.Schema()
	if err != nil {
		return errors.Wrap(err, "getting schema")
	}

	// Add the index.
	idx := schema.Index(indexName,
		OptIndexKeys(options.Keys),
		OptIndexTrackExistence(true),
	)
	if err := s.Client.CreateIndex(idx); err != nil {
		return errors.Wrap(err, "creating index")
	}

	// Now add fields.
	for _, f := range fields {
		fld, err := s.addFieldToIndex(idx, f.Name, f.Options...)
		if err != nil {
			return errors.Wrapf(err, "adding field to index")
		}
		if err := s.Client.CreateField(fld); err != nil {
			return errors.Wrapf(err, "creating field")
		}
	}

	return nil
}

func (s *schemaAPI) CreateField(ctx context.Context, indexName string, fieldName string, opts ...featurebase.FieldOption) (*featurebase.Field, error) {
	schema, err := s.Client.Schema()
	if err != nil {
		return nil, errors.Wrap(err, "getting schema")
	}

	if !schema.HasIndex(indexName) {
		return nil, featurebase.ErrIndexNotFound
	}

	idx := schema.Index(indexName)

	fld, err := s.addFieldToIndex(idx, fieldName, opts...)
	if err != nil {
		return nil, errors.Wrapf(err, "adding field to index")
	}

	if err := s.Client.CreateField(fld); err != nil {
		return nil, errors.Wrapf(err, "creating field")
	}

	return nil, nil
}

func (s *schemaAPI) addFieldToIndex(idx *Index, fieldName string, opts ...featurebase.FieldOption) (*Field, error) {
	ffos := &featurebase.FieldOptions{}
	for _, opt := range opts {
		opt(ffos)
	}

	cfos := []FieldOption{}

	switch ffos.Type {
	case featurebase.FieldTypeBool:
		cfos = append(cfos, OptFieldTypeBool())
	case featurebase.FieldTypeInt:
		cfos = append(cfos, OptFieldTypeInt(ffos.Min.ToInt64(0), ffos.Max.ToInt64(0)))
	case featurebase.FieldTypeSet:
		cfos = append(cfos,
			OptFieldTypeSet(CacheType(ffos.CacheType), int(ffos.CacheSize)),
			OptFieldKeys(ffos.Keys),
		)
	case featurebase.FieldTypeMutex:
		cfos = append(cfos,
			OptFieldTypeMutex(CacheType(ffos.CacheType), int(ffos.CacheSize)),
			OptFieldKeys(ffos.Keys),
		)
	case featurebase.FieldTypeDecimal:
		cfos = append(cfos, OptFieldTypeDecimal(ffos.Scale, ffos.Min, ffos.Max))
	case featurebase.FieldTypeTime:
		cfos = append(cfos,
			OptFieldTypeTime(TimeQuantum(ffos.TimeQuantum), ffos.NoStandardView),
			OptFieldKeys(ffos.Keys),
		)
	case featurebase.FieldTypeTimestamp:
		cfos = append(cfos, OptFieldTypeTimestamp(featurebase.DefaultEpoch, ffos.TimeUnit))
	default:
		return nil, errors.Errorf("unsupported field type: %s", ffos.Type)
	}

	return idx.Field(fieldName, cfos...), nil
}

func (s *schemaAPI) DeleteField(ctx context.Context, indexName string, fieldName string) error {
	schema, err := s.Client.Schema()
	if err != nil {
		return errors.Wrap(err, "getting schema")
	}

	if !schema.HasIndex(indexName) {
		return featurebase.ErrIndexNotFound
	}

	idx := schema.Index(indexName)

	return s.Client.DeleteField(&Field{
		name:  fieldName,
		index: idx,
	})
}

func (s *schemaAPI) DeleteIndex(ctx context.Context, indexName string) error {
	return s.Client.DeleteIndexByName(indexName)
}

func (s *schemaAPI) IndexInfo(ctx context.Context, indexName string) (*featurebase.IndexInfo, error) {
	schema, err := s.Client.Schema()
	if err != nil {
		return nil, errors.Wrap(err, "getting schema")
	}

	if !schema.HasIndex(indexName) {
		return nil, featurebase.ErrIndexNotFound
	}

	idx := schema.Index(indexName)
	return FromClientIndex(idx), nil
}

func (s *schemaAPI) Schema(ctx context.Context, withViews bool) ([]*featurebase.IndexInfo, error) {
	return nil, errors.New("", "schemaAPI.Schema is not implemented")
}

var _ featurebase.QueryAPI = &queryAPI{}

// queryAPI is a featurebase client wrapper which implements the
// featurebase.QueryAPI interface. This was introduced for use in batch tests
// when we decoupled Batch from the client package. In other words, this is only
// used for those tests, it may not be functionally complete, and should not be
// used otherwise without further testing and review of this code.
type queryAPI struct {
	*Client
}

func NewQueryAPI(c *Client) *queryAPI {
	return &queryAPI{
		Client: c,
	}
}

func (q *queryAPI) Query(ctx context.Context, req *featurebase.QueryRequest) (featurebase.QueryResponse, error) {
	schema, err := q.Client.Schema()
	if err != nil {
		return featurebase.QueryResponse{}, errors.Wrap(err, "getting schema")
	}

	if !schema.HasIndex(req.Index) {
		return featurebase.QueryResponse{}, featurebase.ErrIndexNotFound
	}

	idx := schema.Index(req.Index)

	qry := NewPQLBaseQuery(req.Query, idx, nil)
	res, err := q.Client.Query(qry)
	if err != nil {
		return featurebase.QueryResponse{}, errors.Wrap(err, "querying client")
	}

	var rerr error
	if res.ErrorMessage != "" {
		rerr = errors.New("", res.ErrorMessage)
	}

	fbResults := make([]interface{}, 0)

	// Row, PairsField, GroupCounts
	for _, result := range res.ResultList {
		switch result.Type() {
		case QueryResultTypeRow:
			if len(result.Row().Keys) > 0 {
				row := featurebase.NewRow()
				row.Keys = result.Row().Keys
				fbResults = append(fbResults, row)
			} else {
				fbResults = append(fbResults, featurebase.NewRow(result.Row().Columns...))
			}

		case QueryResultTypeUint64:
			fbResults = append(fbResults, uint64(result.Count()))

		case QueryResultTypeBool:
			fbResults = append(fbResults, result.Changed())

		case QueryResultTypePairsField:
			pairs := []featurebase.Pair{}
			for _, ci := range result.CountItems() {
				pairs = append(pairs, featurebase.Pair{
					ID:    ci.ID,
					Key:   ci.Key,
					Count: ci.Count,
				})
			}
			pf := &featurebase.PairsField{
				Pairs: pairs,
				Field: "",
			}
			fbResults = append(fbResults, pf)

		case QueryResultTypeGroupCounts:
			groups := []featurebase.GroupCount{}
			for _, grpCnt := range result.GroupCounts() {
				fieldRows := []featurebase.FieldRow{}
				for _, fr := range grpCnt.Groups {
					fieldRows = append(fieldRows, featurebase.FieldRow{
						Field:  fr.FieldName,
						RowID:  fr.RowID,
						RowKey: fr.RowKey,
						Value:  fr.Value,
					})
				}
				groups = append(groups, featurebase.GroupCount{
					Group: fieldRows,
					Count: uint64(grpCnt.Count),
					Agg:   grpCnt.Agg,
					// DecimalAgg: ??,
				})
			}

			gc := featurebase.NewGroupCounts("", groups...)
			fbResults = append(fbResults, gc)

		case QueryResultTypeValCount:
			vc := featurebase.ValCount{
				Val:   result.Value(),
				Count: result.Count(),
			}
			fbResults = append(fbResults, vc)

		default:
			return featurebase.QueryResponse{}, errors.Errorf("unsupported query result type: %d", result.Type())
		}
	}

	resp := &featurebase.QueryResponse{
		Results: fbResults,
		Err:     rerr,
	}

	return *resp, nil
}
