// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.23.0
// 	protoc        v3.7.1
// source: github.com/iznauy/BTrDB/grpcinterface/btrdb.proto

package grpcinterface

import (
	context "context"
	proto "github.com/golang/protobuf/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// This is a compile-time assertion that a sufficiently up-to-date version
// of the legacy proto package is being used.
const _ = proto.ProtoPackageIsVersion4

type TestConnectionRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *TestConnectionRequest) Reset() {
	*x = TestConnectionRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TestConnectionRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TestConnectionRequest) ProtoMessage() {}

func (x *TestConnectionRequest) ProtoReflect() protoreflect.Message {
	mi := &file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TestConnectionRequest.ProtoReflect.Descriptor instead.
func (*TestConnectionRequest) Descriptor() ([]byte, []int) {
	return file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_rawDescGZIP(), []int{0}
}

type TestConnectionResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Success bool `protobuf:"varint,1,opt,name=success,proto3" json:"success,omitempty"`
}

func (x *TestConnectionResponse) Reset() {
	*x = TestConnectionResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TestConnectionResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TestConnectionResponse) ProtoMessage() {}

func (x *TestConnectionResponse) ProtoReflect() protoreflect.Message {
	mi := &file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TestConnectionResponse.ProtoReflect.Descriptor instead.
func (*TestConnectionResponse) Descriptor() ([]byte, []int) {
	return file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_rawDescGZIP(), []int{1}
}

func (x *TestConnectionResponse) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

var File_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto protoreflect.FileDescriptor

var file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_rawDesc = []byte{
	0x0a, 0x31, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x69, 0x7a, 0x6e,
	0x61, 0x75, 0x79, 0x2f, 0x42, 0x54, 0x72, 0x44, 0x42, 0x2f, 0x67, 0x72, 0x70, 0x63, 0x69, 0x6e,
	0x74, 0x65, 0x72, 0x66, 0x61, 0x63, 0x65, 0x2f, 0x62, 0x74, 0x72, 0x64, 0x62, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x12, 0x0d, 0x67, 0x72, 0x70, 0x63, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x66, 0x61,
	0x63, 0x65, 0x22, 0x17, 0x0a, 0x15, 0x54, 0x65, 0x73, 0x74, 0x43, 0x6f, 0x6e, 0x6e, 0x65, 0x63,
	0x74, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x22, 0x32, 0x0a, 0x16, 0x54,
	0x65, 0x73, 0x74, 0x43, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x73, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x08, 0x52, 0x07, 0x73, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x32,
	0x68, 0x0a, 0x05, 0x42, 0x54, 0x72, 0x44, 0x42, 0x12, 0x5f, 0x0a, 0x0e, 0x54, 0x65, 0x73, 0x74,
	0x43, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x24, 0x2e, 0x67, 0x72, 0x70,
	0x63, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x66, 0x61, 0x63, 0x65, 0x2e, 0x54, 0x65, 0x73, 0x74, 0x43,
	0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x1a, 0x25, 0x2e, 0x67, 0x72, 0x70, 0x63, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x66, 0x61, 0x63, 0x65,
	0x2e, 0x54, 0x65, 0x73, 0x74, 0x43, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x52,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x27, 0x5a, 0x25, 0x67, 0x69, 0x74,
	0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x69, 0x7a, 0x6e, 0x61, 0x75, 0x79, 0x2f, 0x42,
	0x54, 0x72, 0x44, 0x42, 0x2f, 0x67, 0x72, 0x70, 0x63, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x66, 0x61,
	0x63, 0x65, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_rawDescOnce sync.Once
	file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_rawDescData = file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_rawDesc
)

func file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_rawDescGZIP() []byte {
	file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_rawDescOnce.Do(func() {
		file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_rawDescData = protoimpl.X.CompressGZIP(file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_rawDescData)
	})
	return file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_rawDescData
}

var file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_goTypes = []interface{}{
	(*TestConnectionRequest)(nil),  // 0: grpcinterface.TestConnectionRequest
	(*TestConnectionResponse)(nil), // 1: grpcinterface.TestConnectionResponse
}
var file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_depIdxs = []int32{
	0, // 0: grpcinterface.BTrDB.TestConnection:input_type -> grpcinterface.TestConnectionRequest
	1, // 1: grpcinterface.BTrDB.TestConnection:output_type -> grpcinterface.TestConnectionResponse
	1, // [1:2] is the sub-list for method output_type
	0, // [0:1] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_init() }
func file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_init() {
	if File_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TestConnectionRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TestConnectionResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_goTypes,
		DependencyIndexes: file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_depIdxs,
		MessageInfos:      file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_msgTypes,
	}.Build()
	File_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto = out.File
	file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_rawDesc = nil
	file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_goTypes = nil
	file_github_com_iznauy_BTrDB_grpcinterface_btrdb_proto_depIdxs = nil
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConnInterface

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion6

// BTrDBClient is the client API for BTrDB service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type BTrDBClient interface {
	TestConnection(ctx context.Context, in *TestConnectionRequest, opts ...grpc.CallOption) (*TestConnectionResponse, error)
}

type bTrDBClient struct {
	cc grpc.ClientConnInterface
}

func NewBTrDBClient(cc grpc.ClientConnInterface) BTrDBClient {
	return &bTrDBClient{cc}
}

func (c *bTrDBClient) TestConnection(ctx context.Context, in *TestConnectionRequest, opts ...grpc.CallOption) (*TestConnectionResponse, error) {
	out := new(TestConnectionResponse)
	err := c.cc.Invoke(ctx, "/grpcinterface.BTrDB/TestConnection", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// BTrDBServer is the server API for BTrDB service.
type BTrDBServer interface {
	TestConnection(context.Context, *TestConnectionRequest) (*TestConnectionResponse, error)
}

// UnimplementedBTrDBServer can be embedded to have forward compatible implementations.
type UnimplementedBTrDBServer struct {
}

func (*UnimplementedBTrDBServer) TestConnection(context.Context, *TestConnectionRequest) (*TestConnectionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method TestConnection not implemented")
}

func RegisterBTrDBServer(s *grpc.Server, srv BTrDBServer) {
	s.RegisterService(&_BTrDB_serviceDesc, srv)
}

func _BTrDB_TestConnection_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TestConnectionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BTrDBServer).TestConnection(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/grpcinterface.BTrDB/TestConnection",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BTrDBServer).TestConnection(ctx, req.(*TestConnectionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _BTrDB_serviceDesc = grpc.ServiceDesc{
	ServiceName: "grpcinterface.BTrDB",
	HandlerType: (*BTrDBServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "TestConnection",
			Handler:    _BTrDB_TestConnection_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "github.com/iznauy/BTrDB/grpcinterface/btrdb.proto",
}
