// Package vec32 provides operations on float32 vectors.
package vec32

import (
	"bytes"
	"encoding/binary"
	"math"
)

// Add adds b to a.
// Add panics if the vector lengths are unequal.
func Add(a, b []float32) {
	if len(a) != len(b) {
		panic("vector lengths not equal")
	}
	for i, v := range b {
		a[i] += v
	}
}

// Scale scales a by n.
func Scale(a []float32, n float32) {
	for i := range a {
		a[i] *= n
	}
}

// Dot returns the dot product of a and b.
// Dot panics if the vector lengths are unequal.
func Dot(a, b []float32) float32 {
	if len(a) != len(b) {
		panic("vector lengths not equal")
	}
	s := float32(0)
	for i := range a {
		s += a[i] * b[i]
	}
	return s
}

// Norm returns the Euclidean norm of a.
func Norm(a []float32) float32 {
	s := float32(0)
	for _, v := range a {
		s += v * v
	}
	return float32(math.Sqrt(float64(s)))
}

// Normalize converts a to its unit vector.
func Normalize(a []float32) {
	Scale(a, 1/Norm(a))
}

// Bytes serializes vec into a byte slice.
func Bytes(vec []float32) []byte {
	buf := new(bytes.Buffer)
	for _, v := range vec {
		binary.Write(buf, binary.BigEndian, v)
	}
	return buf.Bytes()
}

// FromBytes converts a byte slice into slice of float32.
func FromBytes(data []byte) ([]float32, error) {
	vec := make([]float32, len(data)/4)
	buf := bytes.NewReader(data)
	var v float32
	for i := range vec {
		if err := binary.Read(buf, binary.BigEndian, &v); err != nil {
			return nil, err
		}
		vec[i] = v
	}
	return vec, nil
}
