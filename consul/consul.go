package consul

import "github.com/hashicorp/consul/api"

type Client struct {
	consul *api.Client
}

func (c *Client) Get(path string) []byte {
	pair, _, err := c.consul.KV().Get(path, nil)
	if err != nil {
		panic(err)
	}
	return pair.Value
}

func (c Client) Close() error {
	return nil
}
func (c *Client) Set(path string, value []byte) (err error) {

	_, err = c.consul.KV().Put(&api.KVPair{Key: path, Flags: 0, Value: value}, nil)
	return
}

func NewClient(Address string) *Client {
	c := &Client{}
	config := api.DefaultConfig()
	config.Address = Address
	consul, err := api.NewClient(config)
	if err != nil {
		panic(err)
	}
	c.consul = consul

	return c
}
