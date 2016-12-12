package bench

import (
	"time"
)

// wrapper type to force human-readable JSON output
type PrettyDuration time.Duration

func (d PrettyDuration) MarshalJSON() ([]byte, error) {
	s := time.Duration(d).String()
	return []byte("\"" + s + "\""), nil
}

// Recursively replaces elements of ugly types with their pretty wrappers
func Prettify(m map[string]interface{}) map[string]interface{} {
	newmap := make(map[string]interface{})
	for k, v := range m {
		switch v.(type) {
		case time.Duration:
			newmap[k] = PrettyDuration(v.(time.Duration))
		default:
			newmap[k] = Prettify(v.(map[string]interface{}))
		}
	}
	return newmap
}
