package json

import "encoding/json"

type codec struct {
}

func (c *codec) Encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (c *codec) Decode(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
