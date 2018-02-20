package codecs

import (
	"encoding/json"

	"github.com/devimteam/observer"
)

const contentTypeAppJSON = "application/json"

type JSONCodec struct {
}

type codecRequest struct {
	request observer.Request
}

type codecResponse struct {
	data interface{}
}

func (c *JSONCodec) NewRequest(r observer.Request) observer.CodecRequest {
	return &codecRequest{request: r}
}

func (c *JSONCodec) NewResponse(data interface{}) observer.CodecResponse {
	return &codecResponse{data: data}
}

func (c *JSONCodec) ContentType() string {
	return contentTypeAppJSON
}

func (c *codecResponse) Body() ([]byte, error) {
	return json.Marshal(c.data)
}

func (c *codecRequest) ReadRequest(args interface{}) error {
	return json.Unmarshal(c.request.Body(), args)
}
