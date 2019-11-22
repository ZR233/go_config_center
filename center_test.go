package go_config_centor

import "testing"

func TestConfig_Save(t *testing.T) {
	ce, err := NewCenter([]string{"bsw-ubuntu:2181"}, "/test1/test2/test3", "config")
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
