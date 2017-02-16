package json

import (
	"encoding/json"

	"github.com/l-vitaly/observer"
)

const contentType = "application/json"

type Codec struct {
}

func NewCodec() *Codec {
	return &Codec{}
}

type codecRequest struct {
	request observer.Request
}

type codecResponse struct {
	data interface{}
}

func (c *Codec) NewRequest(r observer.Request) observer.CodecRequest {
	return &codecRequest{request: r}
}

func (c *Codec) NewResponse(data interface{}) observer.CodecResponse {
	return &codecResponse{data: data}
}

func (c *Codec) ContentType() string {
	return contentType
}

func (c *codecResponse) Body() ([]byte, error) {
	return json.Marshal(c.data)
}

func (c *codecRequest) ReadRequest(args interface{}) error {
	return json.Unmarshal(c.request.Body(), args)
}
