package cms

import (
	"testing"
)

func TestCountMinSketch(t *testing.T) {
	sketch := MakeCountMinSketch(0.01, 0.01)
	sketch.Add("test")
	sketch.Add("test")
	sketch.Add("test")
	sketch.Add("test")
	sketch.Add("test")
	sketch.Add("hello")
	sketch.Add("hello")
	sketch.Add("hello")
	sketch.Add("world")
	sketch.Add("world")

	if sketch.Read("test") != 5 {
		t.Errorf("Expected 5, got %d", sketch.Read("test"))
	}
	if sketch.Read("hello") != 3 {
		t.Errorf("Expected 3, got %d", sketch.Read("hello"))
	}
	if sketch.Read("world") != 2 {
		t.Errorf("Expected 3, got %d", sketch.Read("world"))
	}
	if sketch.Read("random") != 0 {
		t.Errorf("Expected 0, got %d", sketch.Read("random"))
	}
}

func TestCountMinSketchSerialization(t *testing.T) {
	sketch := MakeCountMinSketch(0.01, 0.01)
	sketch.Add("test")
	sketch.Add("test")
	sketch.Add("test")
	sketch.Add("test")
	sketch.Add("test")
	sketch.Add("hello")
	sketch.Add("hello")
	sketch.Add("hello")
	sketch.Add("world")
	sketch.Add("world")
	cms2 := MakeCountMinSketch(0.01, 0.01)
	cms2.Add("test")
	cms2.Add("test")
	cms2.Add("hello")

	serialized1 := sketch.Serialize()
	serialized2 := cms2.Serialize()
	deserialized := Deserialize(append(serialized1, serialized2...))
	cms3 := deserialized[0]
	cms4 := deserialized[1]

	if cms3.Read("test") != 5 {
		t.Errorf("Expected 5, got %d", cms2.Read("test"))
	}
	if cms3.Read("hello") != 3 {
		t.Errorf("Expected 3, got %d", cms2.Read("hello"))
	}
	if cms3.Read("world") != 2 {
		t.Errorf("Expected 3, got %d", cms2.Read("world"))
	}
	if cms3.Read("random") != 0 {
		t.Errorf("Expected 0, got %d", cms2.Read("random"))
	}
	if cms4.Read("test") != 2 {
		t.Errorf("Expected 2, got %d", cms2.Read("test"))
	}
	if cms4.Read("hello") != 1 {
		t.Errorf("Expected 1, got %d", cms2.Read("hello"))
	}
}
