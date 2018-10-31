package main

import (
	"github.com/gomodule/redigo/redis"
	"log"
	"reflect"
	"testing"
)

var packet = []byte{126, 126, 90, 1, 2, 0, 33, 134, 2, 0, 4, 0, 0, 144, 7, 5, 0, 100, 0, 0, 0, 1, 0, 0, 0, 18, 0, 0, 128, 0, 0, 0, 0, 1, 0, 0, 0, 60, 78, 65, 86, 83, 67, 82, 32, 118, 101, 114, 61, 49, 46, 48, 62, 60, 73, 68, 62, 49, 56, 60, 47, 73, 68, 62, 60, 70, 82, 79, 77, 62, 83, 69, 82, 86, 69, 82, 60, 47, 70, 82, 79, 77, 62, 60, 84, 79, 62, 85, 83, 69, 82, 60, 47, 84, 79, 62, 60, 84, 89, 80, 69, 62, 81, 85, 69, 82, 89, 60, 47, 84, 89, 80, 69, 62, 60, 77, 83, 71, 32, 116, 105, 109, 101, 61, 54, 48, 32, 98, 101, 101, 112, 61, 49, 32, 116, 121, 112, 101, 61, 98, 97, 99, 107, 103, 114, 111, 117, 110, 100, 62, 60, 98, 114, 47, 62, 60, 98, 114, 47, 62, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 194, 251, 32, 236, 229, 237, 255, 32, 241, 235, 251, 248, 232, 242, 229, 63, 60, 98, 114, 47, 62, 60, 98, 114, 47, 62, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 60, 98, 116, 110, 49, 62, 196, 224, 60, 47, 98, 116, 110, 49, 62, 60, 98, 114, 47, 62, 60, 98, 114, 47, 62, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 38, 110, 98, 115, 112, 59, 60, 98, 116, 110, 50, 62, 205, 229, 242, 60, 47, 98, 116, 110, 50, 62, 60, 98, 114, 47, 62, 60, 47, 77, 83, 71, 62, 60, 47, 78, 65, 86, 83, 67, 82, 62}

func TestExtServ1(t *testing.T) {
	id := 11
	var mesID uint16 = 1001
	mill := getMill()
	var cR redis.Conn
	for {
		var err error
		cR, err = redis.Dial("tcp", ":6379")
		if err != nil {
			log.Printf("serverSession: error connecting to redis in serverSession: %s\n", err)
		} else {
			break
		}
	}
	//close redis connection
	defer cR.Close()
	err := writeExtServ(cR, id, packet, mill, mesID)
	if err != nil {
		t.Errorf("writeExtServ error: %v", err)
	}
	res, mill1, flag, mesID1, err := getServExt(cR, id)
	if err != nil {
		t.Errorf("getServExt error: %v", err)
	}
	if mesID1 != uint64(mesID) {
		t.Errorf("incorrect mesID: %d", mesID)
	}
	if !reflect.DeepEqual(res, packet) {
		t.Errorf("incorrect res: %v", res)
	}
	if mill != mill1 {
		t.Errorf("incorrect mill: %d; must be %d", mill1, mill)
	}
	if flag != "0" {
		t.Errorf("incorrect flag: %s", flag)
	}
}

func TestExtServ2(t *testing.T) {
	id := 12
	var mesID uint16 = 1002
	mill := getMill()
	var cR redis.Conn
	for {
		var err error
		cR, err = redis.Dial("tcp", ":6379")
		if err != nil {
			log.Printf("serverSession: error connecting to redis in serverSession: %s\n", err)
		} else {
			break
		}
	}
	//close redis connection
	defer cR.Close()
	err := writeExtServ(cR, id, packet, mill, mesID)
	if err != nil {
		t.Errorf("writeExtServ error: %v", err)
	}
	err = setFlagServerExt(cR, id, "1")
	res, mill1, flag, mesID1, err := getServExt(cR, id)
	if err != nil {
		t.Errorf("getServExt error: %v", err)
	}
	if mesID1 != uint64(mesID) {
		t.Errorf("incorrect mesID: %d", mesID)
	}
	if !reflect.DeepEqual(res, packet) {
		t.Errorf("incorrect res: %v", res)
	}
	if mill != mill1 {
		t.Errorf("incorrect mill: %d; must be %d", mill1, mill)
	}
	if flag != "1" {
		t.Errorf("incorrect flag: %s", flag)
	}
}

func TestExtServ3(t *testing.T) {
	id := 13
	var mesID uint16 = 1003
	mill := getMill()
	var cR redis.Conn
	for {
		var err error
		cR, err = redis.Dial("tcp", ":6379")
		if err != nil {
			log.Printf("serverSession: error connecting to redis in serverSession: %s\n", err)
		} else {
			break
		}
	}
	//close redis connection
	defer cR.Close()
	err := writeExtServ(cR, id, packet, mill, mesID)
	if err != nil {
		t.Errorf("writeExtServ error: %v", err)
	}
	res, mill1, flag, mesID1, err := getServExt(cR, id)
	if err != nil {
		t.Errorf("getServExt error: %v", err)
	}
	if mesID1 != uint64(mesID) {
		t.Errorf("incorrect mesID: %d", mesID)
	}
	if !reflect.DeepEqual(res, packet) {
		t.Errorf("incorrect res: %v", res)
	}
	if mill != mill1 {
		t.Errorf("incorrect mill: %d; must be %d", mill1, mill)
	}
	if flag != "0" {
		t.Errorf("incorrect flag: %s", flag)
	}
	err = removeServerExt(cR, id)
	if err != nil {
		t.Errorf("removeServerExt error: %v", err)
	}
	_, mill1, _, _, err = getServExt(cR, id)
	if err != nil {
		t.Errorf("removeServerExt error 1: %v", err)
	}
	if mill1 != 0 {
		t.Errorf("incorrect mill: %d; must be %d", mill1, 0)
	}

}
