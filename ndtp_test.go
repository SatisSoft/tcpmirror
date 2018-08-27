package main

import (
	"reflect"
	"testing"
)

func TestParserNDTP(t *testing.T) {
	packet := []byte{0, 80, 86, 161, 44, 216, 192, 140, 96, 196, 138, 54, 8, 0, 69, 0, 0, 129, 102, 160, 64, 0, 125, 6,
		18, 51, 10, 68, 41, 150, 10, 176, 70, 26, 236, 153, 35, 56, 151, 147, 73, 96, 98, 94, 76, 40, 80,
		24, 1, 2, 190, 27, 0, 0, 126, 126, 74, 0, 2, 0, 107, 210, 2, 0, 0, 0, 0, 0, 0, 1, 0, 101, 0, 1, 0, 171,
		20, 0, 0, 0, 0, 36, 141, 198, 90, 87, 110, 119, 22, 201, 186, 64, 33, 224, 203, 0, 0, 0, 0, 83, 1, 0,
		0, 220, 0, 4, 0, 2, 0, 22, 0, 67, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 167, 97, 0, 0, 31, 6, 0, 0, 8,
		0, 2, 0, 0, 0, 0, 0}
	rnis := rnisData{1522961700, 37.692578, 55.7890249, 339, 0, false, 0, "", 0, 0, true, true, true}
	nph := nphData{1, 2, 0, true, 0, false, 0}
	dataOrig := ndtpData{0x02, 0x00, true, nph, rnis}
	dataForm, _, _, _ := parseNDTP(packet)
	if !reflect.DeepEqual(dataForm, dataOrig) {
		t.Error("For packet", packet, "\n",
			"expected: ", dataOrig, "\n",
			"got:      ", dataForm)
	}
}
