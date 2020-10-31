// Package vec32 provides operations on float32 vectors.
package vec32

import (
	"bytes"
	"encoding/binary"
	"math"
)

// Add adds v2 to v1.
func Add(v1, v2 []float32) {
	for i, v := range v2 {
		v1[i] += v
	}
}

// Scale scales vec by n.
func Scale(vec []float32, n float32) {
	for i := range vec {
		vec[i] *= n
	}
}

// Norm returns the Euclidean norm of vec.
func Norm(vec []float32) float32 {
	s := float32(0)
	for _, v := range vec {
		s += v * v
	}
	return float32(math.Sqrt(float64(s)))
}

// Normalize converts vec to its unit vector.
func Normalize(vec []float32) {
	Scale(vec, 1/Norm(vec))
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
