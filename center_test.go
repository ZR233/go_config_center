package go_config_centor

import (
	"testing"
)

func TestConfig_Save(t *testing.T) {
	ce := NewCenter([]string{"bsw-ubuntu:2181"}, "/test1/test2/test3", "detect_config")
	err := ce.Open()
	if err != nil {
		t.Error(err)
		return
	}
	err = ce.Sync()
	if err != nil {
		t.Error(err)
		return
	}
}

func TestCenter_GetKafkaAddresses(t *testing.T) {

	ce := NewCenter([]string{"bsw-ubuntu:2181"}, "/test1/test2/test3", "detect_config")
	err := ce.Open()
	if err != nil {
		t.Error(err)
		return
	}
	addr, err := ce.GetKafkaAddresses()
	if err != nil {
		t.Error(err)
		return
	}

	t.Log(addr)
}
