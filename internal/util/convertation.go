package util

import (
	"github.com/ashirko/navprot/pkg/convertation"
	"github.com/ashirko/navprot/pkg/ndtp"
)

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
