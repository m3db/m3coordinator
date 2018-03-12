// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: placement.proto

/*
	Package admin is a generated protocol buffer package.

	It is generated from these files:
		placement.proto

	It has these top-level messages:
		PlacementInitRequest
		PlacementGetResponse
*/
package admin

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"
import placementpb "github.com/m3db/m3cluster/generated/proto/placementpb"

import io "io"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type PlacementInitRequest struct {
	Instances         []*placementpb.Instance `protobuf:"bytes,1,rep,name=instances" json:"instances,omitempty"`
	NumShards         int32                   `protobuf:"varint,2,opt,name=num_shards,json=numShards,proto3" json:"num_shards,omitempty"`
	ReplicationFactor int32                   `protobuf:"varint,3,opt,name=replication_factor,json=replicationFactor,proto3" json:"replication_factor,omitempty"`
}

func (m *PlacementInitRequest) Reset()                    { *m = PlacementInitRequest{} }
func (m *PlacementInitRequest) String() string            { return proto.CompactTextString(m) }
func (*PlacementInitRequest) ProtoMessage()               {}
func (*PlacementInitRequest) Descriptor() ([]byte, []int) { return fileDescriptorPlacement, []int{0} }

func (m *PlacementInitRequest) GetInstances() []*placementpb.Instance {
	if m != nil {
		return m.Instances
	}
	return nil
}

func (m *PlacementInitRequest) GetNumShards() int32 {
	if m != nil {
		return m.NumShards
	}
	return 0
}

func (m *PlacementInitRequest) GetReplicationFactor() int32 {
	if m != nil {
		return m.ReplicationFactor
	}
	return 0
}

type PlacementGetResponse struct {
	Placement *placementpb.Placement `protobuf:"bytes,1,opt,name=placement" json:"placement,omitempty"`
	Version   int32                  `protobuf:"varint,2,opt,name=version,proto3" json:"version,omitempty"`
}

func (m *PlacementGetResponse) Reset()                    { *m = PlacementGetResponse{} }
func (m *PlacementGetResponse) String() string            { return proto.CompactTextString(m) }
func (*PlacementGetResponse) ProtoMessage()               {}
func (*PlacementGetResponse) Descriptor() ([]byte, []int) { return fileDescriptorPlacement, []int{1} }

func (m *PlacementGetResponse) GetPlacement() *placementpb.Placement {
	if m != nil {
		return m.Placement
	}
	return nil
}

func (m *PlacementGetResponse) GetVersion() int32 {
	if m != nil {
		return m.Version
	}
	return 0
}

func init() {
	proto.RegisterType((*PlacementInitRequest)(nil), "admin.PlacementInitRequest")
	proto.RegisterType((*PlacementGetResponse)(nil), "admin.PlacementGetResponse")
}
func (m *PlacementInitRequest) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *PlacementInitRequest) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Instances) > 0 {
		for _, msg := range m.Instances {
			dAtA[i] = 0xa
			i++
			i = encodeVarintPlacement(dAtA, i, uint64(msg.Size()))
			n, err := msg.MarshalTo(dAtA[i:])
			if err != nil {
				return 0, err
			}
			i += n
		}
	}
	if m.NumShards != 0 {
		dAtA[i] = 0x10
		i++
		i = encodeVarintPlacement(dAtA, i, uint64(m.NumShards))
	}
	if m.ReplicationFactor != 0 {
		dAtA[i] = 0x18
		i++
		i = encodeVarintPlacement(dAtA, i, uint64(m.ReplicationFactor))
	}
	return i, nil
}

func (m *PlacementGetResponse) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *PlacementGetResponse) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Placement != nil {
		dAtA[i] = 0xa
		i++
		i = encodeVarintPlacement(dAtA, i, uint64(m.Placement.Size()))
		n1, err := m.Placement.MarshalTo(dAtA[i:])
		if err != nil {
			return 0, err
		}
		i += n1
	}
	if m.Version != 0 {
		dAtA[i] = 0x10
		i++
		i = encodeVarintPlacement(dAtA, i, uint64(m.Version))
	}
	return i, nil
}

func encodeVarintPlacement(dAtA []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return offset + 1
}
func (m *PlacementInitRequest) Size() (n int) {
	var l int
	_ = l
	if len(m.Instances) > 0 {
		for _, e := range m.Instances {
			l = e.Size()
			n += 1 + l + sovPlacement(uint64(l))
		}
	}
	if m.NumShards != 0 {
		n += 1 + sovPlacement(uint64(m.NumShards))
	}
	if m.ReplicationFactor != 0 {
		n += 1 + sovPlacement(uint64(m.ReplicationFactor))
	}
	return n
}

func (m *PlacementGetResponse) Size() (n int) {
	var l int
	_ = l
	if m.Placement != nil {
		l = m.Placement.Size()
		n += 1 + l + sovPlacement(uint64(l))
	}
	if m.Version != 0 {
		n += 1 + sovPlacement(uint64(m.Version))
	}
	return n
}

func sovPlacement(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozPlacement(x uint64) (n int) {
	return sovPlacement(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *PlacementInitRequest) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowPlacement
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: PlacementInitRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: PlacementInitRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Instances", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPlacement
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthPlacement
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Instances = append(m.Instances, &placementpb.Instance{})
			if err := m.Instances[len(m.Instances)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field NumShards", wireType)
			}
			m.NumShards = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPlacement
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.NumShards |= (int32(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field ReplicationFactor", wireType)
			}
			m.ReplicationFactor = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPlacement
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.ReplicationFactor |= (int32(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipPlacement(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthPlacement
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *PlacementGetResponse) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowPlacement
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: PlacementGetResponse: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: PlacementGetResponse: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Placement", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPlacement
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthPlacement
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Placement == nil {
				m.Placement = &placementpb.Placement{}
			}
			if err := m.Placement.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Version", wireType)
			}
			m.Version = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPlacement
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Version |= (int32(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipPlacement(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthPlacement
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipPlacement(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowPlacement
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowPlacement
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
			return iNdEx, nil
		case 1:
			iNdEx += 8
			return iNdEx, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowPlacement
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			iNdEx += length
			if length < 0 {
				return 0, ErrInvalidLengthPlacement
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowPlacement
					}
					if iNdEx >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipPlacement(dAtA[start:])
				if err != nil {
					return 0, err
				}
				iNdEx = start + next
			}
			return iNdEx, nil
		case 4:
			return iNdEx, nil
		case 5:
			iNdEx += 4
			return iNdEx, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}

var (
	ErrInvalidLengthPlacement = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowPlacement   = fmt.Errorf("proto: integer overflow")
)

func init() { proto.RegisterFile("placement.proto", fileDescriptorPlacement) }

var fileDescriptorPlacement = []byte{
	// 272 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x5c, 0x90, 0x4f, 0x4a, 0xfb, 0x40,
	0x14, 0xc7, 0x7f, 0xf3, 0x2b, 0x55, 0x3a, 0x5d, 0xa8, 0x83, 0x4a, 0x10, 0x0c, 0xa5, 0xab, 0x6c,
	0xcc, 0x40, 0xe3, 0x09, 0x04, 0x95, 0xee, 0x24, 0x1e, 0xa0, 0x4c, 0x26, 0xaf, 0xed, 0x40, 0xe6,
	0x4d, 0x9c, 0x79, 0xf1, 0x2c, 0x7a, 0x23, 0x97, 0x1e, 0x41, 0xe2, 0x45, 0x84, 0xd0, 0xa6, 0xd1,
	0xe5, 0xfb, 0xfe, 0xe3, 0xc3, 0xe3, 0x27, 0x75, 0xa5, 0x34, 0x58, 0x40, 0x4a, 0x6b, 0xef, 0xc8,
	0x89, 0xb1, 0x2a, 0xad, 0xc1, 0xab, 0xfb, 0x8d, 0xa1, 0x6d, 0x53, 0xa4, 0xda, 0x59, 0x69, 0xb3,
	0xb2, 0x90, 0x36, 0xd3, 0x55, 0x13, 0x08, 0xbc, 0xdc, 0x00, 0x82, 0x57, 0x04, 0xa5, 0xec, 0x1a,
	0xb2, 0x5f, 0xa8, 0x0b, 0xf9, 0x67, 0x6d, 0xfe, 0xce, 0xf8, 0xf9, 0xd3, 0x5e, 0x5b, 0xa2, 0xa1,
	0x1c, 0x5e, 0x1a, 0x08, 0x24, 0x32, 0x3e, 0x31, 0x18, 0x48, 0xa1, 0x86, 0x10, 0xb1, 0xd9, 0x28,
	0x99, 0x2e, 0x2e, 0xd2, 0xc1, 0x52, 0xba, 0xdc, 0xb9, 0xf9, 0x21, 0x27, 0xae, 0x39, 0xc7, 0xc6,
	0xae, 0xc2, 0x56, 0xf9, 0x32, 0x44, 0xff, 0x67, 0x2c, 0x19, 0xe7, 0x13, 0x6c, 0xec, 0x73, 0x27,
	0x88, 0x1b, 0x2e, 0x3c, 0xd4, 0x95, 0xd1, 0x8a, 0x8c, 0xc3, 0xd5, 0x5a, 0x69, 0x72, 0x3e, 0x1a,
	0x75, 0xb1, 0xb3, 0x81, 0xf3, 0xd0, 0x19, 0xf3, 0xf5, 0x00, 0xed, 0x11, 0x28, 0x87, 0x50, 0x3b,
	0x0c, 0x20, 0x6e, 0xf9, 0xa4, 0x07, 0x89, 0xd8, 0x8c, 0x25, 0xd3, 0xc5, 0xe5, 0x2f, 0xb4, 0xbe,
	0x95, 0x1f, 0x82, 0x22, 0xe2, 0xc7, 0xaf, 0xe0, 0x83, 0x71, 0xb8, 0x03, 0xdb, 0x9f, 0x77, 0xa7,
	0x1f, 0x6d, 0xcc, 0x3e, 0xdb, 0x98, 0x7d, 0xb5, 0x31, 0x7b, 0xfb, 0x8e, 0xff, 0x15, 0x47, 0xdd,
	0x73, 0xb2, 0x9f, 0x00, 0x00, 0x00, 0xff, 0xff, 0x53, 0x6f, 0xc4, 0x2f, 0x7d, 0x01, 0x00, 0x00,
}
