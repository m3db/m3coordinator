// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: types.proto

package prompb

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"
import _ "github.com/gogo/protobuf/gogoproto"

import binary "encoding/binary"

import io "io"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

type LabelMatcher_Type int32

const (
	LabelMatcher_EQ  LabelMatcher_Type = 0
	LabelMatcher_NEQ LabelMatcher_Type = 1
	LabelMatcher_RE  LabelMatcher_Type = 2
	LabelMatcher_NRE LabelMatcher_Type = 3
)

var LabelMatcher_Type_name = map[int32]string{
	0: "EQ",
	1: "NEQ",
	2: "RE",
	3: "NRE",
}
var LabelMatcher_Type_value = map[string]int32{
	"EQ":  0,
	"NEQ": 1,
	"RE":  2,
	"NRE": 3,
}

func (x LabelMatcher_Type) String() string {
	return proto.EnumName(LabelMatcher_Type_name, int32(x))
}
func (LabelMatcher_Type) EnumDescriptor() ([]byte, []int) { return fileDescriptorTypes, []int{4, 0} }

type Sample struct {
	Value     float64 `protobuf:"fixed64,1,opt,name=value,proto3" json:"value,omitempty"`
	Timestamp int64   `protobuf:"varint,2,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
}

func (m *Sample) Reset()                    { *m = Sample{} }
func (m *Sample) String() string            { return proto.CompactTextString(m) }
func (*Sample) ProtoMessage()               {}
func (*Sample) Descriptor() ([]byte, []int) { return fileDescriptorTypes, []int{0} }

func (m *Sample) GetValue() float64 {
	if m != nil {
		return m.Value
	}
	return 0
}

func (m *Sample) GetTimestamp() int64 {
	if m != nil {
		return m.Timestamp
	}
	return 0
}

type TimeSeries struct {
	Labels  []*Label  `protobuf:"bytes,1,rep,name=labels" json:"labels,omitempty"`
	Samples []*Sample `protobuf:"bytes,2,rep,name=samples" json:"samples,omitempty"`
}

func (m *TimeSeries) Reset()                    { *m = TimeSeries{} }
func (m *TimeSeries) String() string            { return proto.CompactTextString(m) }
func (*TimeSeries) ProtoMessage()               {}
func (*TimeSeries) Descriptor() ([]byte, []int) { return fileDescriptorTypes, []int{1} }

func (m *TimeSeries) GetLabels() []*Label {
	if m != nil {
		return m.Labels
	}
	return nil
}

func (m *TimeSeries) GetSamples() []*Sample {
	if m != nil {
		return m.Samples
	}
	return nil
}

type Label struct {
	Name  string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Value string `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (m *Label) Reset()                    { *m = Label{} }
func (m *Label) String() string            { return proto.CompactTextString(m) }
func (*Label) ProtoMessage()               {}
func (*Label) Descriptor() ([]byte, []int) { return fileDescriptorTypes, []int{2} }

func (m *Label) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *Label) GetValue() string {
	if m != nil {
		return m.Value
	}
	return ""
}

type Labels struct {
	Labels []Label `protobuf:"bytes,1,rep,name=labels" json:"labels"`
}

func (m *Labels) Reset()                    { *m = Labels{} }
func (m *Labels) String() string            { return proto.CompactTextString(m) }
func (*Labels) ProtoMessage()               {}
func (*Labels) Descriptor() ([]byte, []int) { return fileDescriptorTypes, []int{3} }

func (m *Labels) GetLabels() []Label {
	if m != nil {
		return m.Labels
	}
	return nil
}

// Matcher specifies a rule, which can match or set of labels or not.
type LabelMatcher struct {
	Type  LabelMatcher_Type `protobuf:"varint,1,opt,name=type,proto3,enum=prometheus.LabelMatcher_Type" json:"type,omitempty"`
	Name  string            `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	Value string            `protobuf:"bytes,3,opt,name=value,proto3" json:"value,omitempty"`
}

func (m *LabelMatcher) Reset()                    { *m = LabelMatcher{} }
func (m *LabelMatcher) String() string            { return proto.CompactTextString(m) }
func (*LabelMatcher) ProtoMessage()               {}
func (*LabelMatcher) Descriptor() ([]byte, []int) { return fileDescriptorTypes, []int{4} }

func (m *LabelMatcher) GetType() LabelMatcher_Type {
	if m != nil {
		return m.Type
	}
	return LabelMatcher_EQ
}

func (m *LabelMatcher) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *LabelMatcher) GetValue() string {
	if m != nil {
		return m.Value
	}
	return ""
}

func init() {
	proto.RegisterType((*Sample)(nil), "prometheus.Sample")
	proto.RegisterType((*TimeSeries)(nil), "prometheus.TimeSeries")
	proto.RegisterType((*Label)(nil), "prometheus.Label")
	proto.RegisterType((*Labels)(nil), "prometheus.Labels")
	proto.RegisterType((*LabelMatcher)(nil), "prometheus.LabelMatcher")
	proto.RegisterEnum("prometheus.LabelMatcher_Type", LabelMatcher_Type_name, LabelMatcher_Type_value)
}
func (m *Sample) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Sample) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Value != 0 {
		dAtA[i] = 0x9
		i++
		binary.LittleEndian.PutUint64(dAtA[i:], uint64(math.Float64bits(float64(m.Value))))
		i += 8
	}
	if m.Timestamp != 0 {
		dAtA[i] = 0x10
		i++
		i = encodeVarintTypes(dAtA, i, uint64(m.Timestamp))
	}
	return i, nil
}

func (m *TimeSeries) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *TimeSeries) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Labels) > 0 {
		for _, msg := range m.Labels {
			dAtA[i] = 0xa
			i++
			i = encodeVarintTypes(dAtA, i, uint64(msg.Size()))
			n, err := msg.MarshalTo(dAtA[i:])
			if err != nil {
				return 0, err
			}
			i += n
		}
	}
	if len(m.Samples) > 0 {
		for _, msg := range m.Samples {
			dAtA[i] = 0x12
			i++
			i = encodeVarintTypes(dAtA, i, uint64(msg.Size()))
			n, err := msg.MarshalTo(dAtA[i:])
			if err != nil {
				return 0, err
			}
			i += n
		}
	}
	return i, nil
}

func (m *Label) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Label) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Name) > 0 {
		dAtA[i] = 0xa
		i++
		i = encodeVarintTypes(dAtA, i, uint64(len(m.Name)))
		i += copy(dAtA[i:], m.Name)
	}
	if len(m.Value) > 0 {
		dAtA[i] = 0x12
		i++
		i = encodeVarintTypes(dAtA, i, uint64(len(m.Value)))
		i += copy(dAtA[i:], m.Value)
	}
	return i, nil
}

func (m *Labels) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Labels) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Labels) > 0 {
		for _, msg := range m.Labels {
			dAtA[i] = 0xa
			i++
			i = encodeVarintTypes(dAtA, i, uint64(msg.Size()))
			n, err := msg.MarshalTo(dAtA[i:])
			if err != nil {
				return 0, err
			}
			i += n
		}
	}
	return i, nil
}

func (m *LabelMatcher) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *LabelMatcher) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Type != 0 {
		dAtA[i] = 0x8
		i++
		i = encodeVarintTypes(dAtA, i, uint64(m.Type))
	}
	if len(m.Name) > 0 {
		dAtA[i] = 0x12
		i++
		i = encodeVarintTypes(dAtA, i, uint64(len(m.Name)))
		i += copy(dAtA[i:], m.Name)
	}
	if len(m.Value) > 0 {
		dAtA[i] = 0x1a
		i++
		i = encodeVarintTypes(dAtA, i, uint64(len(m.Value)))
		i += copy(dAtA[i:], m.Value)
	}
	return i, nil
}

func encodeVarintTypes(dAtA []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return offset + 1
}
func (m *Sample) Size() (n int) {
	var l int
	_ = l
	if m.Value != 0 {
		n += 9
	}
	if m.Timestamp != 0 {
		n += 1 + sovTypes(uint64(m.Timestamp))
	}
	return n
}

func (m *TimeSeries) Size() (n int) {
	var l int
	_ = l
	if len(m.Labels) > 0 {
		for _, e := range m.Labels {
			l = e.Size()
			n += 1 + l + sovTypes(uint64(l))
		}
	}
	if len(m.Samples) > 0 {
		for _, e := range m.Samples {
			l = e.Size()
			n += 1 + l + sovTypes(uint64(l))
		}
	}
	return n
}

func (m *Label) Size() (n int) {
	var l int
	_ = l
	l = len(m.Name)
	if l > 0 {
		n += 1 + l + sovTypes(uint64(l))
	}
	l = len(m.Value)
	if l > 0 {
		n += 1 + l + sovTypes(uint64(l))
	}
	return n
}

func (m *Labels) Size() (n int) {
	var l int
	_ = l
	if len(m.Labels) > 0 {
		for _, e := range m.Labels {
			l = e.Size()
			n += 1 + l + sovTypes(uint64(l))
		}
	}
	return n
}

func (m *LabelMatcher) Size() (n int) {
	var l int
	_ = l
	if m.Type != 0 {
		n += 1 + sovTypes(uint64(m.Type))
	}
	l = len(m.Name)
	if l > 0 {
		n += 1 + l + sovTypes(uint64(l))
	}
	l = len(m.Value)
	if l > 0 {
		n += 1 + l + sovTypes(uint64(l))
	}
	return n
}

func sovTypes(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozTypes(x uint64) (n int) {
	return sovTypes(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *Sample) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTypes
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
			return fmt.Errorf("proto: Sample: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Sample: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 1 {
				return fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
			}
			var v uint64
			if (iNdEx + 8) > l {
				return io.ErrUnexpectedEOF
			}
			v = uint64(binary.LittleEndian.Uint64(dAtA[iNdEx:]))
			iNdEx += 8
			m.Value = float64(math.Float64frombits(v))
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Timestamp", wireType)
			}
			m.Timestamp = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Timestamp |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipTypes(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthTypes
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
func (m *TimeSeries) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTypes
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
			return fmt.Errorf("proto: TimeSeries: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: TimeSeries: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Labels", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
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
				return ErrInvalidLengthTypes
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Labels = append(m.Labels, &Label{})
			if err := m.Labels[len(m.Labels)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Samples", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
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
				return ErrInvalidLengthTypes
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Samples = append(m.Samples, &Sample{})
			if err := m.Samples[len(m.Samples)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipTypes(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthTypes
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
func (m *Label) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTypes
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
			return fmt.Errorf("proto: Label: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Label: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Name", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthTypes
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Name = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthTypes
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Value = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipTypes(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthTypes
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
func (m *Labels) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTypes
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
			return fmt.Errorf("proto: Labels: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Labels: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Labels", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
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
				return ErrInvalidLengthTypes
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Labels = append(m.Labels, Label{})
			if err := m.Labels[len(m.Labels)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipTypes(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthTypes
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
func (m *LabelMatcher) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTypes
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
			return fmt.Errorf("proto: LabelMatcher: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: LabelMatcher: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Type", wireType)
			}
			m.Type = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Type |= (LabelMatcher_Type(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Name", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthTypes
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Name = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthTypes
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Value = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipTypes(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthTypes
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
func skipTypes(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowTypes
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
					return 0, ErrIntOverflowTypes
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
					return 0, ErrIntOverflowTypes
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
				return 0, ErrInvalidLengthTypes
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowTypes
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
				next, err := skipTypes(dAtA[start:])
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
	ErrInvalidLengthTypes = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowTypes   = fmt.Errorf("proto: integer overflow")
)

func init() { proto.RegisterFile("types.proto", fileDescriptorTypes) }

var fileDescriptorTypes = []byte{
	// 335 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x84, 0x91, 0xc1, 0x4a, 0xeb, 0x40,
	0x14, 0x86, 0x3b, 0x49, 0x9a, 0xd2, 0xd3, 0xcb, 0x25, 0x0e, 0x2e, 0x82, 0x68, 0x2c, 0x59, 0x45,
	0xd0, 0x94, 0xd6, 0x95, 0xe0, 0xaa, 0x90, 0x5d, 0x15, 0x3a, 0xed, 0xca, 0x5d, 0x52, 0xc6, 0x36,
	0x90, 0x31, 0x43, 0x66, 0x22, 0xf4, 0x41, 0x04, 0x1f, 0xa9, 0x4b, 0x9f, 0x40, 0xa4, 0xbe, 0x88,
	0xe4, 0xa4, 0x35, 0x05, 0x05, 0x77, 0xe7, 0xfc, 0xf9, 0x7e, 0xce, 0x47, 0x06, 0x7a, 0x7a, 0x2d,
	0xb9, 0x0a, 0x65, 0x91, 0xeb, 0x9c, 0x82, 0x2c, 0x72, 0xc1, 0xf5, 0x8a, 0x97, 0xea, 0xe4, 0x6a,
	0x99, 0xea, 0x55, 0x99, 0x84, 0x8b, 0x5c, 0x0c, 0x96, 0xf9, 0x32, 0x1f, 0x20, 0x92, 0x94, 0x8f,
	0xb8, 0xe1, 0x82, 0x53, 0x5d, 0xf5, 0x6f, 0xc1, 0x9e, 0xc5, 0x42, 0x66, 0x9c, 0x1e, 0x43, 0xfb,
	0x39, 0xce, 0x4a, 0xee, 0x92, 0x3e, 0x09, 0x08, 0xab, 0x17, 0x7a, 0x0a, 0x5d, 0x9d, 0x0a, 0xae,
	0x74, 0x2c, 0xa4, 0x6b, 0xf4, 0x49, 0x60, 0xb2, 0x26, 0xf0, 0x39, 0xc0, 0x3c, 0x15, 0x7c, 0xc6,
	0x8b, 0x94, 0x2b, 0x7a, 0x01, 0x76, 0x16, 0x27, 0x3c, 0x53, 0x2e, 0xe9, 0x9b, 0x41, 0x6f, 0x74,
	0x14, 0x36, 0x5e, 0xe1, 0xa4, 0xfa, 0xc2, 0x76, 0x00, 0xbd, 0x84, 0x8e, 0xc2, 0xb3, 0xca, 0x35,
	0x90, 0xa5, 0x87, 0x6c, 0x6d, 0xc4, 0xf6, 0x88, 0x3f, 0x84, 0x36, 0xd6, 0x29, 0x05, 0xeb, 0x29,
	0x16, 0xb5, 0x62, 0x97, 0xe1, 0xdc, 0x78, 0x1b, 0x18, 0xd6, 0x8b, 0x7f, 0x03, 0xf6, 0xa4, 0x3e,
	0x35, 0xf8, 0xd3, 0x6a, 0x6c, 0x6d, 0xde, 0xcf, 0x5b, 0x7b, 0x37, 0xff, 0x85, 0xc0, 0x3f, 0xcc,
	0xef, 0x62, 0xbd, 0x58, 0xf1, 0x82, 0x0e, 0xc1, 0xaa, 0xfe, 0x36, 0x5e, 0xfd, 0x3f, 0x3a, 0xfb,
	0xd1, 0xdf, 0x71, 0xe1, 0x7c, 0x2d, 0x39, 0x43, 0xf4, 0x5b, 0xd4, 0xf8, 0x4d, 0xd4, 0x3c, 0x14,
	0x0d, 0xc0, 0xaa, 0x7a, 0xd4, 0x06, 0x23, 0x9a, 0x3a, 0x2d, 0xda, 0x01, 0xf3, 0x3e, 0x9a, 0x3a,
	0xa4, 0x0a, 0x58, 0xe4, 0x18, 0x18, 0xb0, 0xc8, 0x31, 0xc7, 0xee, 0x66, 0xeb, 0x91, 0xb7, 0xad,
	0x47, 0x3e, 0xb6, 0x1e, 0x79, 0xfd, 0xf4, 0x5a, 0x0f, 0x76, 0x65, 0x22, 0x93, 0xc4, 0xc6, 0xb7,
	0xbc, 0xfe, 0x0a, 0x00, 0x00, 0xff, 0xff, 0x6c, 0x41, 0x68, 0xb3, 0x15, 0x02, 0x00, 0x00,
}
