package util

import (
	"github.com/egorban/navprot/pkg/convertation"
	"github.com/egorban/navprot/pkg/ndtp"
)

// Ndtp2Egts converts binary NDTP packet to bynary EGTS packet
func Ndtp2Egts(packet []byte, id uint32, packID, recID uint16) (res []byte, err error) {
	ndtpData := new(ndtp.Packet)
	if _, err = ndtpData.Parse(packet); err != nil {
		return
	}
	egtsData, err := convertation.ToEGTS(ndtpData, id, packID, recID)
	if err != nil {
		return
	}
	res, err = egtsData.Form()
	return
}
