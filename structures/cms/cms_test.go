package cms

import (
	"testing"
)

func TestCountMinSketch(t *testing.T) {
	sketch := MakeCountMinSketch(0.01, 0.01)
	sketch.Add([]byte("test"))
	sketch.Add([]byte("test"))
	sketch.Add([]byte("test"))
	sketch.Add([]byte("test"))
	sketch.Add([]byte("test"))
	sketch.Add([]byte("hello"))
	sketch.Add([]byte("hello"))
	sketch.Add([]byte("world"))
	sketch.Add([]byte("world"))

	if sketch.Read([]byte("test")) != 5 {
		t.Errorf("Expected 5, got %d", sketch.Read([]byte("test")))
	}
	if sketch.Read([]byte("hello")) != 2 {
		t.Errorf("Expected 2, got %d", sketch.Read([]byte("hello")))
	}
	if sketch.Read([]byte("world")) != 2 {
		t.Errorf("Expected 2, got %d", sketch.Read([]byte("world")))
	}
	if sketch.Read([]byte("random")) != 0 {
		t.Errorf("Expected 0, got %d", sketch.Read([]byte("random")))
	}
}

func TestCountMinSketchSerialization(t *testing.T) {
	sketch := MakeCountMinSketch(0.01, 0.01)
	sketch.Add([]byte("test"))
	sketch.Add([]byte("test"))
	sketch.Add([]byte("test"))
	sketch.Add([]byte("test"))
	sketch.Add([]byte("test"))
	sketch.Add([]byte("hello"))
	sketch.Add([]byte("hello"))
	sketch.Add([]byte("world"))
	sketch.Add([]byte("world"))
	cms2 := MakeCountMinSketch(0.01, 0.01)
	cms2.Add([]byte("test"))
	cms2.Add([]byte("test"))
	cms2.Add([]byte("hello"))

	serialized1 := sketch.Serialize()
	serialized2 := cms2.Serialize()
	deserialized := Deserialize(append(serialized1, serialized2...))
	cms3 := deserialized[0]
	cms4 := deserialized[1]

	if cms3.Read([]byte("test")) != 5 {
		t.Errorf("Expected 5, got %d", cms2.Read([]byte("test")))
	}
	if cms3.Read([]byte("hello")) != 2 {
		t.Errorf("Expected 2, got %d", cms3.Read([]byte("hello")))
	}
	if cms3.Read([]byte("world")) != 2 {
		t.Errorf("Expected 2, got %d", cms2.Read([]byte("world")))
	}
	if cms3.Read([]byte("random")) != 0 {
		t.Errorf("Expected 0, got %d", cms2.Read([]byte("random")))
	}
	if cms4.Read([]byte("test")) != 2 {
		t.Errorf("Expected 2, got %d", cms4.Read([]byte("test")))
	}
	if cms4.Read([]byte("hello")) != 1 {
		t.Errorf("Expected 1, got %d", cms4.Read([]byte("hello")))
	}
}
