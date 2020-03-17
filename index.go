package go_config_centor

import (
	"github.com/ZR233/go_config_center/consul"
	"github.com/ZR233/go_config_center/model"
	"github.com/go-yaml/yaml"
)

type ConfigType int

const (
	ConfigTypeFile ConfigType = iota
	ConfigTypeZookeeper
	ConfigTypeConsul
)

type Config struct {
	options *Options
	client  model.Client
	path    string
}
type Options struct {
	Type  ConfigType
	Hosts []string
}

func DefaultOptions() (options *Options) {
	options = &Options{
		Type: ConfigTypeConsul,
	}

	return
}

func Open(path string, options *Options) (config *Config, err error) {
	if options == nil {
		options = DefaultOptions()
	}

	config = &Config{
		options: options,
		path:    path,
	}

	switch options.Type {
	case ConfigTypeConsul:
		address := "127.0.0.1:8500"
		if len(options.Hosts) > 0 {
			address = options.Hosts[0]
		}

		config.client = consul.NewClient(address)
	case ConfigTypeFile:
	case ConfigTypeZookeeper:
	}

	return
}

func (c *Config) Close() error {

	return nil
}

func (c *Config) GetCommon() *model.CommonConfig {
	common := &model.CommonConfig{}
	err := c.unmarshal("/common/redis", &common.Redis)
	if err != nil {
		panic(err)
	}
	err = c.unmarshal("/common/postgresql", &common.Postgresql)
	if err != nil {
		panic(err)
	}
	err = c.unmarshal("/common/zookeeper", &common.Zookeeper)
	if err != nil {
		panic(err)
	}

	return common
}
func (c *Config) unmarshal(path string, out interface{}) (err error) {
	data := c.client.Get(path)
	err = yaml.Unmarshal(data, out)
	return
}
func (c *Config) Unmarshal(out interface{}) (err error) {
	err = c.unmarshal(c.path, out)
	return
}

func (c *Config) set(path string, in interface{}) (err error) {
	data, err := yaml.Marshal(in)
	if err != nil {
		return
	}

	err = c.client.Set(path, data)
	return
}

func (c *Config) Set(in interface{}) (err error) {
	data, err := yaml.Marshal(in)
	if err != nil {
		return
	}

	err = c.client.Set(c.path, data)
	return
}

func (c *Config) SetCommon(common *model.CommonConfig) (err error) {
	data, err := yaml.Marshal(common)
	if err != nil {
		return
	}

	err = c.client.Set(c.path, data)
	return
}
