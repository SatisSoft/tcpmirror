package main

import (
	"reflect"
	"testing"
)

func TestFormEGTS(t *testing.T) {
	packetOrig := []byte{1, 0, 0, 11, 0, 35, 0, 0, 0, 1, 153,
		24, 0, 0, 0, 1, 239, 0, 0, 0, 2, 2,
		16, 21, 0, 210, 49, 43, 16, 79, 186, 58, 158, 210, 39, 188, 53, 3, 0, 0, 178, 0, 0, 0, 0, 0, 106, 141}
	data := rnisData{1533570258, 37.782409656276556, 55.62752532903746, 178, 0, false, 239, "data"}
	packetForm, _ := formEGTS(data)

	if !reflect.DeepEqual(packetForm, packetOrig) {
		t.Error("For rnisData", data,
			"expected", packetOrig,
			"got", packetForm)
	}
}
