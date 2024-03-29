package pinot

import (
	"bytes"
	"encoding/json"
)

// decodeJSONWithNumber use the UseNumber option in std json, which works
// by first decode number into string, then back to converted type
// see implementation of json.Number in std
func decodeJSONWithNumber(bodyBytes []byte, out interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(bodyBytes))
	decoder.UseNumber()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	return nil
}
