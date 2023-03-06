package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strconv"

	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/pkg/errors"

	pilosaclient "github.com/featurebasedb/featurebase/v3/client"
	"github.com/featurebasedb/featurebase/v3/pql"
)

func IngestJSON(index *pilosaclient.Index, m idk.Main, in io.Reader) error {
	codec, err := newJSONCodec(in)
	if err != nil {
		return errors.Wrap(err, "reading start of JSON data")
	}

	fields := index.Fields()

	idkSchema := make([]idk.Field, 1, len(index.Fields())+1)
	fieldMappers := make(map[string]mapper, len(fields))
	if index.Opts().Keys() {
		idkSchema[0] = idk.StringField{
			NameVal: "/",
		}
		m.PrimaryKeyFields = []string{"/"}
	} else {
		idkSchema[0] = idk.IDField{
			NameVal: "/",
		}
		m.IDField = "/"
	}

	m.Index = index.Name()

	// TODO: time quantums
	for name, field := range fields {
		i := len(idkSchema)
		fopts := field.Opts()
		switch typ := fopts.Type(); typ {
		case pilosaclient.FieldTypeSet:
			if fopts.Keys() {
				idkSchema = append(idkSchema, idk.StringArrayField{
					NameVal: name,
				})
				fieldMappers[name] = mapper{
					idx: i,
					mapper: func(v interface{}) (interface{}, error) {
						var set []string
						switch v := v.(type) {
						case string:
							set = []string{v}

						case []interface{}:
							set = make([]string, len(v))
							for i, elem := range v {
								str, ok := elem.(string)
								if !ok {
									return nil, errors.Wrapf(TypeError{
										Expected: typeDescriptionString,
										Value:    elem,
									}, "parsing element %d of set", i)
								}

								set[i] = str
							}

							if len(set) > 1 {
								dups := make(map[string]struct{}, len(set))
								for _, elem := range set {
									if _, dup := dups[elem]; dup {
										return nil, ErrDuplicateElement{
											Elem: elem,
										}
									}
									dups[elem] = struct{}{}
								}
							}

						default:
							return nil, TypeError{
								Expected: typeDescriptionStringSet,
								Value:    v,
							}
						}

						for _, v := range set {
							if v == "" {
								return nil, errors.New("empty string in set")
							}
						}

						return set, nil
					},
				}
			} else {
				idkSchema = append(idkSchema, idk.IDArrayField{
					NameVal: name,
				})
				fieldMappers[name] = mapper{
					idx: i,
					mapper: func(v interface{}) (interface{}, error) {
						var set []uint64
						switch v := v.(type) {
						case json.Number:
							id, err := strconv.ParseUint(string(v), 10, 64)
							if err != nil {
								return nil, errors.Wrapf(err, "parsing ID")
							}

							set = []uint64{id}

						case []interface{}:
							set = make([]uint64, len(v))
							for i, elem := range v {
								num, ok := elem.(json.Number)
								if !ok {
									return nil, errors.Wrapf(TypeError{
										Expected: typeDescriptionID,
										Value:    elem,
									}, "parsing element %d of set", i)
								}

								id, err := strconv.ParseUint(string(num), 10, 64)
								if err != nil {
									return nil, errors.Wrapf(err, "parsing element %d of set", i)
								}

								set[i] = id
							}

							if len(set) > 1 {
								dups := make(map[uint64]struct{}, len(set))
								for _, elem := range set {
									if _, dup := dups[elem]; dup {
										return nil, ErrDuplicateElement{
											Elem: elem,
										}
									}
									dups[elem] = struct{}{}
								}
							}

						default:
							return nil, TypeError{
								Expected: typeDescriptionIDSet,
								Value:    v,
							}
						}

						for _, v := range set {
							if v == ^uint64(0) {
								// The client package uses this as a nil sentinel.
								return nil, errors.New("max uint64 is not a valid row ID")
							}
						}

						return set, nil
					},
				}
			}

		case pilosaclient.FieldTypeMutex:
			if fopts.Keys() {
				idkSchema = append(idkSchema, idk.StringField{
					NameVal: name,
					Mutex:   true,
				})
				fieldMappers[name] = mapper{
					idx: i,
					mapper: func(v interface{}) (interface{}, error) {
						str, ok := v.(string)
						if !ok {
							return nil, TypeError{
								Expected: typeDescriptionString,
								Value:    v,
							}
						}

						return str, nil
					},
				}
			} else {
				idkSchema = append(idkSchema, idk.IDField{
					NameVal: name,
					Mutex:   true,
				})
				fieldMappers[name] = mapper{
					idx: i,
					mapper: func(v interface{}) (interface{}, error) {
						num, ok := v.(json.Number)
						if !ok {
							return nil, TypeError{
								Expected: typeDescriptionID,
								Value:    v,
							}
						}

						id, err := strconv.ParseUint(string(num), 10, 64)
						if err != nil {
							return nil, err
						}

						if id == ^uint64(0) {
							// The client package uses this as a nil sentinel.
							return nil, errors.New("max uint64 is not a valid row ID")
						}

						return id, nil
					},
				}
			}

			/*
				case pilosaclient.FieldTypeBool:
					// TODO: not supported by IDK :)
					idkSchema = append(idkSchema, idk.BoolField{
						NameVal: name,
					})
			*/

		case pilosaclient.FieldTypeInt:
			if fopts.Keys() {
				idkSchema = append(idkSchema, idk.StringField{
					NameVal: name,
				})
				fieldMappers[name] = mapper{
					idx: i,
					mapper: func(v interface{}) (interface{}, error) {
						str, ok := v.(string)
						if !ok {
							return nil, TypeError{
								Expected: typeDescriptionString,
								Value:    v,
							}
						}

						return str, nil
					},
				}
				break
			}
			idkSchema = append(idkSchema, idk.IntField{
				NameVal: name,
			})
			fieldMappers[name] = mapper{
				idx: i,
				mapper: func(v interface{}) (interface{}, error) {
					number, ok := v.(json.Number)
					if !ok {
						return nil, TypeError{
							Expected: typeDescriptionInt,
							Value:    v,
						}
					}

					return strconv.ParseInt(string(number), 10, 64)
				},
			}

		case pilosaclient.FieldTypeDecimal:
			scale := fopts.Scale()
			idkSchema = append(idkSchema, idk.DecimalField{
				NameVal: name,
				Scale:   scale,
			})
			fieldMappers[name] = mapper{
				idx: i,
				mapper: func(v interface{}) (interface{}, error) {
					var raw string
					switch v := v.(type) {
					case json.Number:
						raw = string(v)
					case string:
						raw = string(v)
					default:
						return nil, TypeError{
							Expected: typeDescriptionDecimal,
							Value:    v,
						}
					}

					dec, err := pql.ParseDecimal(raw)
					if err != nil {
						return nil, err
					}

					return dec, nil
				},
			}

		/*
				// Pilosa unfortunately does not return the epoch to us.
				// As a result, this does not currently work.
				// Also now we need to parse the timestamp here.
			case pilosaclient.FieldTypeTimestamp:
					idkSchema = append(idkSchema, idk.TimestampField{
						NameVal:     name,
						Epoch:       fopts.MinTimestamp(),
						Granularity: fopts.TimeUnit(),
						Layout:      time.RFC3339Nano,
					})
					fieldMappers[name] = mapper{
						idx: i,
						mapper: func(v interface{}) (interface{}, error) {
							str, ok := v.(string)
							if !ok {
								return nil, TypeError{
									Expected: "time string",
									Value:    v,
								}
							}

							return str, nil
						},
					}*/

		default:
			// Ignore the field for now.
		}
	}

	src := source{
		c:        codec,
		fields:   fieldMappers,
		schema:   idkSchema,
		pkstring: index.Opts().Keys(),
	}
	m.NewSource = func() (idk.Source, error) {
		return &src, nil
	}

	return m.Run()
}

// TypeError is an error indicating that the type of a value was set incorrectly.
type TypeError struct {
	// Expected is a human-readable name of the expected type.
	Expected string

	// Value is the incorrectly-typed value.
	Value interface{}
}

const (
	typeDescriptionID        = "ID"
	typeDescriptionIDSet     = "set of " + typeDescriptionID + "s"
	typeDescriptionString    = "string"
	typeDescriptionStringSet = "set of " + typeDescriptionString + "s"
	typeDescriptionInt       = "integer"
	typeDescriptionDecimal   = "decimal"
)

func (t TypeError) Error() string {
	valType := reflect.TypeOf(t.Value)
	valTypeName := valType.String()
	if name, ok := friendlyTypeNames[valType]; ok {
		valTypeName = name
	}

	return fmt.Sprintf("expected a %s but got %v (a %s)", t.Expected, t.Value, valTypeName)
}

// friendlyTypeNames contains more-easily understood names for some common types.
var friendlyTypeNames = map[reflect.Type]string{
	reflect.TypeOf(json.Number("")):          "number",
	reflect.TypeOf([]interface{}{}):          "array",
	reflect.TypeOf(map[string]interface{}{}): "object",
}

// ErrDuplicateElement is an error indicating that an element was included in a set multiple times.
type ErrDuplicateElement struct {
	// Elem is the duplicated element.
	Elem interface{}
}

func (err ErrDuplicateElement) Error() string {
	return fmt.Sprintf("found duplicate element %v in set", err.Elem)
}

type source struct {
	c        *jsonCodec
	pkstring bool
	fields   map[string]mapper
	schema   []idk.Field
}

type mapper struct {
	idx    int
	mapper func(interface{}) (interface{}, error)
}

func (s *source) Record() (idk.Record, error) {
	// Decode the next record.
	raw, err := s.c.Next()
	if err != nil {
		return nil, err
	}

	// Parse the record into IDK-format.
	out := make(idkRec, len(s.schema))
	switch pk := raw.PrimaryKey.(type) {
	case string:
		if !s.pkstring {
			return nil, errors.Wrap(TypeError{
				Expected: typeDescriptionID,
				Value:    pk,
			}, "parsing primary key")
		}

		out[0] = pk

	case json.Number:
		if s.pkstring {
			return nil, errors.Wrap(TypeError{
				Expected: typeDescriptionString,
				Value:    pk,
			}, "parsing primary key")
		}

		id, err := strconv.ParseUint(string(pk), 10, 64)
		if err != nil {
			return nil, errors.Wrap(err, "parsing primary key")
		}

		out[0] = id

	default:
		expect := typeDescriptionID
		if s.pkstring {
			expect = typeDescriptionString
		}
		return nil, errors.Wrap(TypeError{
			Expected: expect,
			Value:    pk,
		}, "parsing primary key")
	}
	for field, value := range raw.Values {
		m, ok := s.fields[field]
		if !ok {
			return nil, errors.Errorf("field %q missing from schema", field)
		}

		v, err := m.mapper(value)
		if err != nil {
			return nil, errors.Wrapf(err, "parsing record field %q", field)
		}

		out[m.idx] = v
	}

	return out, nil
}

func (s *source) Schema() []idk.Field {
	return s.schema
}

func (s *source) Close() error {
	return nil
}

type idkRec []interface{}

func (r idkRec) Commit(ctx context.Context) error {
	return nil
}

func (r idkRec) Data() []interface{} {
	return r
}

func (r idkRec) Schema() interface{} (
	return nil
)