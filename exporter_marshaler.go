package otelnats

import (
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type marshaler interface {
	marshal(v interface{}) ([]byte, error)
}

type marshalerJSON struct{}

func (marshalerJSON) marshal(v any) ([]byte, error) {
	return protojson.Marshal(v.(proto.Message))
}

type marshalerProtobuf struct{}

func (marshalerProtobuf) marshal(v any) ([]byte, error) {
	return proto.Marshal(v.(proto.Message))
}
