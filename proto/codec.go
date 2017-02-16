package proto

import (
	"github.com/gogo/protobuf/proto"
	"github.com/l-vitaly/observer"
)

const contentType = "application/protobuf"

type Codec struct {
}

func NewCodec() *Codec {
	return &Codec{}
}

type codecRequest struct {
	request observer.Request
}

type codecResponse struct {
	data proto.Message
}

func (c *Codec) NewRequest(r observer.Request) observer.CodecRequest {
	return &codecRequest{request: r}
}

func (c *Codec) NewResponse(data interface{}) observer.CodecResponse {
	return &codecResponse{data: data.(proto.Message)}
}

func (c *Codec) ContentType() string {
	return contentType
}

func (c *codecResponse) Body() ([]byte, error) {
	return proto.Marshal(c.data)
}

func (c *codecRequest) ReadRequest(i interface{}) error {
	args := i.(proto.Message)
	return proto.Unmarshal(c.request.Body(), args)
}
