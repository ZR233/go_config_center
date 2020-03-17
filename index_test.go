package go_config_centor

import (
	"testing"
)

func TestOpen(t *testing.T) {
	config, err := Open("/digger", nil)
	println(err)
	common := config.GetCommon()

	println(common)

}
