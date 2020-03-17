package model

type Client interface {
	Get(path string) []byte
	Close() error
	Set(path string, value []byte) error
}
