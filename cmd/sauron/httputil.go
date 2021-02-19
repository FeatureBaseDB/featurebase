// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/json"
	"log"
	"mime"
	"net/http"
	"strings"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/pql"
	"github.com/pkg/errors"
)

// validHeaderAcceptJSON returns false if one or more Accept
// headers are present, but none of them are "application/json"
// (or any matching wildcard). Otherwise returns true.
func validHeaderAcceptJSON(header http.Header) bool {
	return validHeaderAcceptType(header, "application", "json")
}

func validHeaderAcceptType(header http.Header, typ, subtyp string) bool {
	if v, found := header["Accept"]; found {
		for _, v := range v {
			t, _, err := mime.ParseMediaType(v)
			if err != nil {
				switch err {
				case mime.ErrInvalidMediaParameter:
					// This is an optional feature, so we can keep going anyway.
				default:
					continue
				}
			}
			spl := strings.SplitN(t, "/", 2)
			if len(spl) < 2 {
				continue
			}
			switch {
			case spl[0] == typ && spl[1] == subtyp:
				return true
			case spl[0] == "*" && spl[1] == subtyp:
				return true
			case spl[0] == typ && spl[1] == "*":
				return true
			case spl[0] == "*" && spl[1] == "*":
				return true
			}
		}
		return false
	}
	return true
}

// successResponse is a general success/error struct for http responses.
type successResponse struct {
	//h         *Handler
	Success   bool   `json:"success"`
	Name      string `json:"name,omitempty"`
	CreatedAt int64  `json:"createdAt,omitempty"`
	//Error     *Error `json:"error,omitempty"`
	Error error
}

// check determines success or failure based on the error.
// It also returns the corresponding http status code.
func (r *successResponse) check(err error) (statusCode int) {
	if err == nil {
		r.Success = true
		return 0
	}

	cause := errors.Cause(err)

	// Determine HTTP status code based on the error type.
	switch cause.(type) {
	case pilosa.BadRequestError:
		statusCode = http.StatusBadRequest
	case pilosa.ConflictError:
		statusCode = http.StatusConflict
	case pilosa.NotFoundError:
		statusCode = http.StatusNotFound
	default:
		statusCode = http.StatusInternalServerError
	}

	r.Success = false
	r.Error = err // = &Error{Message: err.Error()}

	return statusCode
}

// write sends a response to the http.ResponseWriter based on the success
// status and the error.
func (r *successResponse) write(w http.ResponseWriter, err error) {
	// Apply the error and get the status code.
	statusCode := r.check(err)

	// Marshal the json response.
	msg, err := json.Marshal(r)
	if err != nil {
		http.Error(w, string(msg), http.StatusInternalServerError)
		return
	}

	// Write the response.
	if statusCode == 0 {
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write(msg)
		if err != nil {
			log.Printf("error writing response: %v", err)
			return
		}
		_, err = w.Write([]byte("\n"))
		if err != nil {
			log.Printf("error writing newline after response: %v", err)
			return
		}
	} else {
		http.Error(w, string(msg), statusCode)
	}
}

type postFieldRequest struct {
	Options fieldOptions `json:"options"`
}

// fieldOptions tracks pilosa.FieldOptions. It is made up of pointers to values,
// and used for input validation.
type fieldOptions struct {
	Type           string              `json:"type,omitempty"`
	CacheType      *string             `json:"cacheType,omitempty"`
	CacheSize      *uint32             `json:"cacheSize,omitempty"`
	Min            *pql.Decimal        `json:"min,omitempty"`
	Max            *pql.Decimal        `json:"max,omitempty"`
	Scale          *int64              `json:"scale,omitempty"`
	TimeQuantum    *pilosa.TimeQuantum `json:"timeQuantum,omitempty"`
	Keys           *bool               `json:"keys,omitempty"`
	NoStandardView bool                `json:"noStandardView,omitempty"`
	ForeignIndex   *string             `json:"foreignIndex,omitempty"`
}

func (o *fieldOptions) ToPilosaFieldOptions() *pilosa.FieldOptions {

	fo := &pilosa.FieldOptions{
		Type: o.Type,

		/*
			//Base           int64       `json:"base,omitempty"`
			//BitDepth       uint        `json:"bitDepth,omitempty"`
			Min:            *o.Min,
			Max:            *o.Max,
			Scale:          *o.Scale,
			NoStandardView: o.NoStandardView,
			CacheSize:      *o.CacheSize,
			CacheType:      *o.CacheType,
			TimeQuantum:    *o.TimeQuantum,
			ForeignIndex:   *o.ForeignIndex,
		*/
	}
	if o.Keys != nil {
		fo.Keys = *o.Keys
	}
	return fo
}

func (o *fieldOptions) validate() error {
	// Pointers to default values.
	defaultCacheType := pilosa.DefaultCacheType
	defaultCacheSize := uint32(pilosa.DefaultCacheSize)

	switch o.Type {
	case pilosa.FieldTypeSet, "":
		// Because FieldTypeSet is the default, its arguments are
		// not required. Instead, the defaults are applied whenever
		// a value does not exist.
		if o.Type == "" {
			o.Type = pilosa.FieldTypeSet
		}
		if o.CacheType == nil {
			o.CacheType = &defaultCacheType
		}
		if o.CacheSize == nil {
			o.CacheSize = &defaultCacheSize
		}
		if o.Min != nil {
			return pilosa.NewBadRequestError(errors.New("min does not apply to field type set"))
		} else if o.Max != nil {
			return pilosa.NewBadRequestError(errors.New("max does not apply to field type set"))
		} else if o.TimeQuantum != nil {
			return pilosa.NewBadRequestError(errors.New("timeQuantum does not apply to field type set"))
		}
	case pilosa.FieldTypeInt:
		if o.CacheType != nil {
			return pilosa.NewBadRequestError(errors.New("cacheType does not apply to field type int"))
		} else if o.CacheSize != nil {
			return pilosa.NewBadRequestError(errors.New("cacheSize does not apply to field type int"))
		} else if o.TimeQuantum != nil {
			return pilosa.NewBadRequestError(errors.New("timeQuantum does not apply to field type int"))
		} else if o.ForeignIndex != nil && o.Type == pilosa.FieldTypeDecimal {
			return pilosa.NewBadRequestError(errors.New("decimal field cannot be a foreign key"))
		}
	case pilosa.FieldTypeDecimal:
		if o.Scale == nil {
			return pilosa.NewBadRequestError(errors.New("decimal field requires a scale argument"))
		} else if o.CacheType != nil {
			return pilosa.NewBadRequestError(errors.New("cacheType does not apply to field type int"))
		} else if o.CacheSize != nil {
			return pilosa.NewBadRequestError(errors.New("cacheSize does not apply to field type int"))
		} else if o.TimeQuantum != nil {
			return pilosa.NewBadRequestError(errors.New("timeQuantum does not apply to field type int"))
		} else if o.ForeignIndex != nil && o.Type == pilosa.FieldTypeDecimal {
			return pilosa.NewBadRequestError(errors.New("decimal field cannot be a foreign key"))
		}
	case pilosa.FieldTypeTime:
		if o.CacheType != nil {
			return pilosa.NewBadRequestError(errors.New("cacheType does not apply to field type time"))
		} else if o.CacheSize != nil {
			return pilosa.NewBadRequestError(errors.New("cacheSize does not apply to field type time"))
		} else if o.Min != nil {
			return pilosa.NewBadRequestError(errors.New("min does not apply to field type time"))
		} else if o.Max != nil {
			return pilosa.NewBadRequestError(errors.New("max does not apply to field type time"))
		} else if o.TimeQuantum == nil {
			return pilosa.NewBadRequestError(errors.New("timeQuantum is required for field type time"))
		}
	case pilosa.FieldTypeMutex:
		if o.CacheType == nil {
			o.CacheType = &defaultCacheType
		}
		if o.CacheSize == nil {
			o.CacheSize = &defaultCacheSize
		}
		if o.Min != nil {
			return pilosa.NewBadRequestError(errors.New("min does not apply to field type mutex"))
		} else if o.Max != nil {
			return pilosa.NewBadRequestError(errors.New("max does not apply to field type mutex"))
		} else if o.TimeQuantum != nil {
			return pilosa.NewBadRequestError(errors.New("timeQuantum does not apply to field type mutex"))
		}
	case pilosa.FieldTypeBool:
		if o.CacheType != nil {
			return pilosa.NewBadRequestError(errors.New("cacheType does not apply to field type bool"))
		} else if o.CacheSize != nil {
			return pilosa.NewBadRequestError(errors.New("cacheSize does not apply to field type bool"))
		} else if o.Min != nil {
			return pilosa.NewBadRequestError(errors.New("min does not apply to field type bool"))
		} else if o.Max != nil {
			return pilosa.NewBadRequestError(errors.New("max does not apply to field type bool"))
		} else if o.TimeQuantum != nil {
			return pilosa.NewBadRequestError(errors.New("timeQuantum does not apply to field type bool"))
		} else if o.Keys != nil {
			return pilosa.NewBadRequestError(errors.New("keys does not apply to field type bool"))
		} else if o.ForeignIndex != nil {
			return pilosa.NewBadRequestError(errors.New("bool field cannot be a foreign key"))
		}
	default:
		return errors.Errorf("invalid field type: %s", o.Type)
	}
	return nil
}
