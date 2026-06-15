package server

import (
	"fmt"

	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

type standardProtoCodec struct{}

func init() {
	// Vitess v0.24 globally registers a codec named "proto" for VTProto.
	// This connector serves Fivetran protobuf RPCs, so restore gRPC's standard
	// protobuf codec for this package after Vitess package init has run.
	encoding.RegisterCodecV2(standardProtoCodec{})
}

func (standardProtoCodec) Name() string {
	return "proto"
}

func (standardProtoCodec) Marshal(v any) (mem.BufferSlice, error) {
	msg := protoMessage(v)
	if msg == nil {
		return nil, fmt.Errorf("proto: failed to marshal, message is %T, want proto.Message", v)
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return mem.BufferSlice{mem.SliceBuffer(data)}, nil
}

func (standardProtoCodec) Unmarshal(data mem.BufferSlice, v any) error {
	msg := protoMessage(v)
	if msg == nil {
		return fmt.Errorf("proto: failed to unmarshal, message is %T, want proto.Message", v)
	}

	return proto.Unmarshal(data.Materialize(), msg)
}

func protoMessage(v any) proto.Message {
	switch v := v.(type) {
	case protoadapt.MessageV1:
		return protoadapt.MessageV2Of(v)
	case protoadapt.MessageV2:
		return v
	default:
		return nil
	}
}
