// Copyright (c) 2020 Graphcore Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.26.0
// 	protoc        v3.21.12
// source: vipu-protos-1.18.0/version.proto

package protos

import (
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

// The version service is used on the client side to identify server version
type VersionRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *VersionRequest) Reset() {
	*x = VersionRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_vipu_protos_1_18_0_version_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *VersionRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*VersionRequest) ProtoMessage() {}

func (x *VersionRequest) ProtoReflect() protoreflect.Message {
	mi := &file_vipu_protos_1_18_0_version_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use VersionRequest.ProtoReflect.Descriptor instead.
func (*VersionRequest) Descriptor() ([]byte, []int) {
	return file_vipu_protos_1_18_0_version_proto_rawDescGZIP(), []int{0}
}

type VersionReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Version string `protobuf:"bytes,1,opt,name=version,proto3" json:"version,omitempty"`
}

func (x *VersionReply) Reset() {
	*x = VersionReply{}
	if protoimpl.UnsafeEnabled {
		mi := &file_vipu_protos_1_18_0_version_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *VersionReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*VersionReply) ProtoMessage() {}

func (x *VersionReply) ProtoReflect() protoreflect.Message {
	mi := &file_vipu_protos_1_18_0_version_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use VersionReply.ProtoReflect.Descriptor instead.
func (*VersionReply) Descriptor() ([]byte, []int) {
	return file_vipu_protos_1_18_0_version_proto_rawDescGZIP(), []int{1}
}

func (x *VersionReply) GetVersion() string {
	if x != nil {
		return x.Version
	}
	return ""
}

var File_vipu_protos_1_18_0_version_proto protoreflect.FileDescriptor

var file_vipu_protos_1_18_0_version_proto_rawDesc = []byte{
	0x0a, 0x20, 0x76, 0x69, 0x70, 0x75, 0x2d, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x2d, 0x31, 0x2e,
	0x31, 0x38, 0x2e, 0x30, 0x2f, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x12, 0x08, 0x76, 0x69, 0x72, 0x6d, 0x67, 0x72, 0x70, 0x63, 0x22, 0x10, 0x0a, 0x0e,
	0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x22, 0x28,
	0x0a, 0x0c, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x12, 0x18,
	0x0a, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x32, 0x52, 0x0a, 0x0e, 0x56, 0x65, 0x72, 0x73,
	0x69, 0x6f, 0x6e, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x40, 0x0a, 0x0a, 0x47, 0x65,
	0x74, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x18, 0x2e, 0x76, 0x69, 0x72, 0x6d, 0x67,
	0x72, 0x70, 0x63, 0x2e, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x1a, 0x16, 0x2e, 0x76, 0x69, 0x72, 0x6d, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x56, 0x65,
	0x72, 0x73, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x22, 0x00, 0x42, 0x0a, 0x5a, 0x08,
	0x2e, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_vipu_protos_1_18_0_version_proto_rawDescOnce sync.Once
	file_vipu_protos_1_18_0_version_proto_rawDescData = file_vipu_protos_1_18_0_version_proto_rawDesc
)

func file_vipu_protos_1_18_0_version_proto_rawDescGZIP() []byte {
	file_vipu_protos_1_18_0_version_proto_rawDescOnce.Do(func() {
		file_vipu_protos_1_18_0_version_proto_rawDescData = protoimpl.X.CompressGZIP(file_vipu_protos_1_18_0_version_proto_rawDescData)
	})
	return file_vipu_protos_1_18_0_version_proto_rawDescData
}

var file_vipu_protos_1_18_0_version_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_vipu_protos_1_18_0_version_proto_goTypes = []interface{}{
	(*VersionRequest)(nil), // 0: virmgrpc.VersionRequest
	(*VersionReply)(nil),   // 1: virmgrpc.VersionReply
}
var file_vipu_protos_1_18_0_version_proto_depIdxs = []int32{
	0, // 0: virmgrpc.VersionService.GetVersion:input_type -> virmgrpc.VersionRequest
	1, // 1: virmgrpc.VersionService.GetVersion:output_type -> virmgrpc.VersionReply
	1, // [1:2] is the sub-list for method output_type
	0, // [0:1] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_vipu_protos_1_18_0_version_proto_init() }
func file_vipu_protos_1_18_0_version_proto_init() {
	if File_vipu_protos_1_18_0_version_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_vipu_protos_1_18_0_version_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*VersionRequest); i {
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
		file_vipu_protos_1_18_0_version_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*VersionReply); i {
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
			RawDescriptor: file_vipu_protos_1_18_0_version_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_vipu_protos_1_18_0_version_proto_goTypes,
		DependencyIndexes: file_vipu_protos_1_18_0_version_proto_depIdxs,
		MessageInfos:      file_vipu_protos_1_18_0_version_proto_msgTypes,
	}.Build()
	File_vipu_protos_1_18_0_version_proto = out.File
	file_vipu_protos_1_18_0_version_proto_rawDesc = nil
	file_vipu_protos_1_18_0_version_proto_goTypes = nil
	file_vipu_protos_1_18_0_version_proto_depIdxs = nil
}
