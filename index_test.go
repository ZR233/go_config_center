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

func TestConfig_Set(t *testing.T) {
	config, err := Open("test/test1/test2", nil)
	println(err)
	var test struct {
		Hello  string
		Hello2 string
	}
	test.Hello = "123"
	test.Hello2 = "321"

	err = config.Set(test)
	println(err)
}
