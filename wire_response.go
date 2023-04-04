package pilosa

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/pkg/errors"
)

// WireQueryResponse is the standard featurebase response type which can be
// serialized and sent over the wire.
type WireQueryResponse struct {
	Schema        WireQuerySchema        `json:"schema"`
	Data          [][]interface{}        `json:"data"`
	Error         string                 `json:"error"`
	Warnings      []string               `json:"warnings"`
	QueryPlan     map[string]interface{} `json:"query-plan"`
	ExecutionTime int64                  `json:"execution-time"`
}

// WireQuerySchema is a list of Fields which map to the data columns in the
// Response.
type WireQuerySchema struct {
	Fields []*WireQueryField `json:"fields"`
}

// WireQueryField is a field name along with a supported BaseType and type
// information.
type WireQueryField struct {
	Name     dax.FieldName          `json:"name"`
	Type     string                 `json:"type"`      // human readable display (e.g. "decimal(2)")
	BaseType dax.BaseType           `json:"base-type"` // for programmatic switching on type (e.g. "decimal")
	TypeInfo map[string]interface{} `json:"type-info"` // type modifiers (like scale), but not constraints (like min/max)
}

// UnmarshalJSON is a custom unmarshaller for the SQLResponse that converts the
// value types in `Data` based on the types in `Schema`.
func (s *WireQueryResponse) UnmarshalJSON(in []byte) error {
	return s.UnmarshalJSONTyped(in, false)
}

// UnmarshalJSONTyped is a temporary until we send typed values back in sql
// responses. At that point, we can get rid of the typed=false path. In order to
// do that, we need sql3 to return typed values, and we need the sql3/test/defs
// to define results as typed values (like `IDSet`) instead of (for example)
// `[]int64`.
func (s *WireQueryResponse) UnmarshalJSONTyped(in []byte, typed bool) error {
	type Alias WireQueryResponse
	var aux Alias

	dec := json.NewDecoder(bytes.NewReader(in))
	dec.UseNumber()
	err := dec.Decode(&aux)
	if err != nil {
		return err
	}
	*s = WireQueryResponse(aux)

	// If the SQLResponse contains an error, don't bother doing any conversions
	// on the data.
	if s.Error != "" {
		return nil
	}

	// Try to convert the types in the TypeInfo map for each field in the
	// schema.
	for _, fld := range s.Schema.Fields {
		// TODO(tlt): we can remove these two "ToLower" calls once sql3 is
		// returning dax.FieldType (i.e. lowercase).
		fld.Type = strings.ToLower(fld.Type)
		fld.BaseType = dax.BaseType(strings.ToLower(string(fld.BaseType)))
		for k, v := range fld.TypeInfo {
			switch k {
			case "scale":
				switch n := v.(type) {
				case float64:
					fld.TypeInfo[k] = int64(n)
				case json.Number:
					fld.TypeInfo[k], _ = n.Int64()
				}
			}
		}
	}

	// Try to convert the data types based on the headers.
	for i := range s.Data {
		for j, hdr := range s.Schema.Fields {
			switch hdr.BaseType {
			case dax.BaseTypeID, dax.BaseTypeInt:
				jn := s.Data[i][j]
				if v, ok := jn.(json.Number); ok {
					if x, err := v.Int64(); err == nil {
						s.Data[i][j] = x
					} else {
						return errors.Wrap(err, "can't be decoded as int64")
					}
				}

			case dax.BaseTypeIDSet:
				if src, ok := s.Data[i][j].([]interface{}); ok {
					if typed {
						val := make(IDSet, len(src))
						for k := range src {
							v := src[k].(json.Number)
							if x, err := v.Int64(); err == nil {
								val[k] = x
							} else {
								return errors.Wrap(err, "can't be decoded as int64")
							}
						}
						s.Data[i][j] = val
					} else {
						val := make([]int64, len(src))
						for k := range src {
							v := src[k].(json.Number)
							if x, err := v.Int64(); err == nil {
								val[k] = x
							} else {
								return errors.Wrap(err, "can't be decoded as int64")
							}
						}
						s.Data[i][j] = val
					}
				}

			case dax.BaseTypeDecimal:
				if jn, ok := s.Data[i][j].(json.Number); ok {
					var scale int64
					if scaleVal, ok := hdr.TypeInfo["scale"]; !ok {
						return errors.New("decimal does not have a scale")
					} else if scaleInt64, ok := scaleVal.(int64); !ok {
						return errors.New("scale can't be cast to int64")
					} else {
						scale = scaleInt64
					}

					format := fmt.Sprintf("%%.%df", scale)
					f, err := jn.Float64()
					if err != nil {
						return errors.Wrap(err, "parsing decimal")
					}
					dec, err := pql.ParseDecimal(fmt.Sprintf(format, f))
					if err != nil {
						return errors.Wrap(err, "parsing decimal")
					}
					if dec.Scale != scale {
						dec = pql.NewDecimal(dec.ToInt64(scale), scale)
					}
					s.Data[i][j] = dec
				}

			case dax.BaseTypeStringSet:
				if src, ok := s.Data[i][j].([]interface{}); ok {
					if typed {
						val := make(StringSet, len(src))
						for k := range src {
							val[k] = src[k].(string)
						}
						s.Data[i][j] = val
					} else {
						val := make([]string, len(src))
						for k := range src {
							val[k] = src[k].(string)
						}
						s.Data[i][j] = val
					}
				}

			case dax.BaseTypeTimestamp:
				if src, ok := s.Data[i][j].(string); ok && src != "" {
					val, err := time.ParseInLocation(time.RFC3339Nano, src, time.UTC)
					if err != nil {
						return errors.Wrap(err, "parsing timestamp")
					}
					s.Data[i][j] = val
				}

			case dax.BaseTypeBool, dax.BaseTypeString:
				// no need to convert

			default:
				log.Printf("WARNING: unimplemented: %s", hdr.BaseType)
			}
		}
	}

	return nil
}

// IDSet is a return type specific to SQLResponse types.
type IDSet []int64

func (ii IDSet) String() string {
	var sb strings.Builder
	sb.WriteString("[")
	for i := range ii {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("%d", ii[i]))
	}
	sb.WriteString("]")

	return sb.String()
}

// SortedInt64Slice returns the values in a IDSet field in a int64 slice that is
// sorted
func (ii IDSet) SortedInt64Slice() []int64 {
	var idSetSlice = make([]int64, len(ii))
	copy(idSetSlice, ii)
	sort.Slice(idSetSlice, func(i, j int) bool { return idSetSlice[i] < idSetSlice[j] })
	return idSetSlice
}

// StringSet is a return type specific to SQLResponse types.
type StringSet []string

func (ss StringSet) String() string {
	var sb strings.Builder
	sb.WriteString("[")
	for i := range ss {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("'" + ss[i] + "'")
	}
	sb.WriteString("]")

	return sb.String()
}

// SortedStringSlice returns the values in a StringSet field in a string slice
// that is sorted
func (ss StringSet) SortedStringSlice() []string {
	var stringSetSlice = make([]string, len(ss))
	copy(stringSetSlice, ss)
	sort.Strings(stringSetSlice)
	return stringSetSlice
}

// ShowColumnsResponse returns a structure which is specific to a `SHOW COLUMNS`
// statement, derived from the results in the WireQueryResponse. This is kind of
// a crude way to unmarshal a WireQueryResponse into a type which is specific to
// the sql operation.
// TODO(tlt): see if we can standardize on this logic, because it would be useful to
// have the same thing for SHOW DATABASES and SHOW TABLES.
// TODO(tlt): the fields unmarshalled in this method are a subset of the actual
// columns available; at the moment, we only handled the ones we need in the
// CLI.
func (s *WireQueryResponse) ShowColumnsResponse() (*ShowColumnsResponse, error) {
	// Make a map of header names to index position. We do this to avoid
	// breaking things if for some reason the format of the SHOW COLUMNS
	// response defined in the sql3 package changes.
	m := make(map[string]int)
	for i, fld := range s.Schema.Fields {
		m[string(fld.Name)] = i
	}

	flds := make([]*dax.Field, 0, len(s.Data))
	for _, row := range s.Data {
		// name
		var name dax.FieldName
		if val, ok := row[m["name"]].(string); ok {
			name = dax.FieldName(val)
		}

		// type
		var typ dax.BaseType
		if val, ok := row[m["type"]].(string); ok {
			typ = dax.BaseType(val)
		}

		// scale
		var scale int64
		if val, ok := row[m["scale"]].(int64); ok {
			scale = val
		}

		flds = append(flds, &dax.Field{
			Name: name,
			Type: typ,
			Options: dax.FieldOptions{
				Scale: scale,
			},
		})
	}
	return &ShowColumnsResponse{
		Fields: flds,
	}, nil
}

// ShowColumnsResponse is a type used to marshal the results of a `SHOW COLUMNS`
// statement.
type ShowColumnsResponse struct {
	Fields []*dax.Field
}

// Field returns the field by name. If the field is not found, nil is returned.
func (s *ShowColumnsResponse) Field(name dax.FieldName) *dax.Field {
	for i := range s.Fields {
		if s.Fields[i].Name == name {
			return s.Fields[i]
		}
	}
	return nil
}
