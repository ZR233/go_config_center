package go_config_centor

import (
	"github.com/samuel/go-zookeeper/zk"
	"testing"
	"time"
)

func TestConfig_Save(t *testing.T) {
	ce := NewCenter("/test1/test2/test3", "detect_config")
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

	ce := NewCenter("/test1/test2/test3", "detect_config")
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

func TestCenter_GetPublicPostgres(t *testing.T) {
	ce := NewCenter("/test1/test2/test3", "detect_config")
	err := ce.Open()
	if err != nil {
		t.Error(err)
		return
	}
	p := ce.GetPublicPostgres()

	t.Log(p)
}
func TestZK_Auth(t *testing.T) {

	conn, _, err := zk.Connect([]string{
		"192.168.0.3:2181",
	}, time.Second*20)
	if err != nil {
		t.Fatal(err)
	}

	// permission
	//var acls = zk.DigestACL(zk.PermAll, "user", "password")
	////
	////// create
	//var flags int32 = 0

	Path := "/gozk-digest-test"

	//_, err = conn.Create(Path, []byte("zxc"), flags, acls)

	//t.Log(err)
	//
	//data , _, err := conn.Get(Path)
	//t.Log(data)
	//err  = conn.AddAuth("digest", []byte("user:password"))
	err = conn.Delete(Path, -1)

	a, _, err := conn.GetACL(Path)
	t.Log(a)

	data, _, err := conn.Get(Path)
	t.Log(data)

}

func TestAuth(t *testing.T) {

}
