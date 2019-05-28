// Code generated by protoc-gen-go. DO NOT EDIT.
// source: pb/manifest.proto

package pb

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type Slice struct {
	Checksum             []byte   `protobuf:"bytes,1,opt,name=Checksum,proto3" json:"Checksum,omitempty"`
	Offset               int64    `protobuf:"varint,2,opt,name=Offset,proto3" json:"Offset,omitempty"`
	Length               int64    `protobuf:"varint,3,opt,name=Length,proto3" json:"Length,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Slice) Reset()         { *m = Slice{} }
func (m *Slice) String() string { return proto.CompactTextString(m) }
func (*Slice) ProtoMessage()    {}
func (*Slice) Descriptor() ([]byte, []int) {
	return fileDescriptor_2539f5857e72bfb4, []int{0}
}

func (m *Slice) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Slice.Unmarshal(m, b)
}
func (m *Slice) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Slice.Marshal(b, m, deterministic)
}
func (m *Slice) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Slice.Merge(m, src)
}
func (m *Slice) XXX_Size() int {
	return xxx_messageInfo_Slice.Size(m)
}
func (m *Slice) XXX_DiscardUnknown() {
	xxx_messageInfo_Slice.DiscardUnknown(m)
}

var xxx_messageInfo_Slice proto.InternalMessageInfo

func (m *Slice) GetChecksum() []byte {
	if m != nil {
		return m.Checksum
	}
	return nil
}

func (m *Slice) GetOffset() int64 {
	if m != nil {
		return m.Offset
	}
	return 0
}

func (m *Slice) GetLength() int64 {
	if m != nil {
		return m.Length
	}
	return 0
}

type ManifestV1 struct {
	Size                 int64    `protobuf:"varint,1,opt,name=Size,proto3" json:"Size,omitempty"`
	Slices               []*Slice `protobuf:"bytes,2,rep,name=Slices,proto3" json:"Slices,omitempty"`
	Comment              string   `protobuf:"bytes,3,opt,name=Comment,proto3" json:"Comment,omitempty"`
	ChunkMaxSize         int64    `protobuf:"varint,4,opt,name=ChunkMaxSize,proto3" json:"ChunkMaxSize,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ManifestV1) Reset()         { *m = ManifestV1{} }
func (m *ManifestV1) String() string { return proto.CompactTextString(m) }
func (*ManifestV1) ProtoMessage()    {}
func (*ManifestV1) Descriptor() ([]byte, []int) {
	return fileDescriptor_2539f5857e72bfb4, []int{1}
}

func (m *ManifestV1) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ManifestV1.Unmarshal(m, b)
}
func (m *ManifestV1) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ManifestV1.Marshal(b, m, deterministic)
}
func (m *ManifestV1) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ManifestV1.Merge(m, src)
}
func (m *ManifestV1) XXX_Size() int {
	return xxx_messageInfo_ManifestV1.Size(m)
}
func (m *ManifestV1) XXX_DiscardUnknown() {
	xxx_messageInfo_ManifestV1.DiscardUnknown(m)
}

var xxx_messageInfo_ManifestV1 proto.InternalMessageInfo

func (m *ManifestV1) GetSize() int64 {
	if m != nil {
		return m.Size
	}
	return 0
}

func (m *ManifestV1) GetSlices() []*Slice {
	if m != nil {
		return m.Slices
	}
	return nil
}

func (m *ManifestV1) GetComment() string {
	if m != nil {
		return m.Comment
	}
	return ""
}

func (m *ManifestV1) GetChunkMaxSize() int64 {
	if m != nil {
		return m.ChunkMaxSize
	}
	return 0
}

func init() {
	proto.RegisterType((*Slice)(nil), "pb.Slice")
	proto.RegisterType((*ManifestV1)(nil), "pb.ManifestV1")
}

func init() { proto.RegisterFile("pb/manifest.proto", fileDescriptor_2539f5857e72bfb4) }

var fileDescriptor_2539f5857e72bfb4 = []byte{
	// 198 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x12, 0x2c, 0x48, 0xd2, 0xcf,
	0x4d, 0xcc, 0xcb, 0x4c, 0x4b, 0x2d, 0x2e, 0xd1, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0x2a,
	0x48, 0x52, 0x0a, 0xe6, 0x62, 0x0d, 0xce, 0xc9, 0x4c, 0x4e, 0x15, 0x92, 0xe2, 0xe2, 0x70, 0xce,
	0x48, 0x4d, 0xce, 0x2e, 0x2e, 0xcd, 0x95, 0x60, 0x54, 0x60, 0xd4, 0xe0, 0x09, 0x82, 0xf3, 0x85,
	0xc4, 0xb8, 0xd8, 0xfc, 0xd3, 0xd2, 0x8a, 0x53, 0x4b, 0x24, 0x98, 0x14, 0x18, 0x35, 0x98, 0x83,
	0xa0, 0x3c, 0x90, 0xb8, 0x4f, 0x6a, 0x5e, 0x7a, 0x49, 0x86, 0x04, 0x33, 0x44, 0x1c, 0xc2, 0x53,
	0x6a, 0x64, 0xe4, 0xe2, 0xf2, 0x85, 0xda, 0x15, 0x66, 0x28, 0x24, 0xc4, 0xc5, 0x12, 0x9c, 0x59,
	0x95, 0x0a, 0x36, 0x96, 0x39, 0x08, 0xcc, 0x16, 0x52, 0xe4, 0x62, 0x03, 0xdb, 0x5b, 0x2c, 0xc1,
	0xa4, 0xc0, 0xac, 0xc1, 0x6d, 0xc4, 0xa9, 0x57, 0x90, 0xa4, 0x07, 0x16, 0x09, 0x82, 0x4a, 0x08,
	0x49, 0x70, 0xb1, 0x3b, 0xe7, 0xe7, 0xe6, 0xa6, 0xe6, 0x95, 0x80, 0x8d, 0xe7, 0x0c, 0x82, 0x71,
	0x85, 0x94, 0xb8, 0x78, 0x9c, 0x33, 0x4a, 0xf3, 0xb2, 0x7d, 0x13, 0x2b, 0xc0, 0x06, 0xb3, 0x80,
	0x0d, 0x46, 0x11, 0x4b, 0x62, 0x03, 0xfb, 0xd1, 0x18, 0x10, 0x00, 0x00, 0xff, 0xff, 0xa6, 0xf1,
	0x61, 0x63, 0xf8, 0x00, 0x00, 0x00,
}
