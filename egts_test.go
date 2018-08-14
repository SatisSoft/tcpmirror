package main

import (
	"reflect"
	"testing"
)

func TestFormEGTS(t *testing.T) {
	packetOrig := []byte{1, 0, 0, 11, 0, 35, 0, 0, 0, 1, 153,
		24, 0, 0, 0, 1, 239, 0, 0, 0, 2, 2,
		16, 21, 0, 210, 49, 43, 16, 79, 186, 58, 158, 210, 39, 188, 53, 3, 0, 0, 178, 0, 0, 0, 0, 0, 106, 141}
	data := rnisData{1533570258, 37.782409656276556, 55.62752532903746, 178, 0, false, 239, "data", 0, 0, false, true, true}
	packetForm := formEGTS(data, 0, 0)

	if !reflect.DeepEqual(packetForm, packetOrig) {
		t.Error("For rnisData", data, "\n",
			"expected: ", packetOrig, "\n",
			"got:      ", packetForm)
	}
}

func TestParserEGTS(t *testing.T) {
	packet := []byte{1, 0, 3, 11, 0, 16, 0, 1, 0, 0, 245, 1, 0, 0, 6, 0, 1, 0, 24, 2, 2, 0, 3, 0, 1, 0, 0, 170, 217,
		1, 0, 3, 11, 0, 16, 0, 1, 0, 0, 245, 2, 0, 0, 6, 0, 1, 0, 24, 2, 2, 0, 3, 0, 1, 0, 0, 231, 49,
		1, 0, 3, 11, 0, 28, 0, 1, 0, 0, 212, 3, 0, 0, 18, 0, 1, 0, 24, 2, 2, 0, 3, 0, 3, 0, 0, 0, 3, 0, 2, 0, 0, 0, 3, 0, 1, 0, 0, 58, 7}
	reqIdsOrig := []byte{1, 2, 3}
	reqIdsForm, err := parseEGTS(packet)

	if !reflect.DeepEqual(reqIdsOrig, reqIdsForm) {
		t.Error("For rnisData", packet, "\n",
			"expected: ", reqIdsOrig, "\n",
			"got:      ", reqIdsForm)
	}
}
