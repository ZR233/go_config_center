package go_config_centor

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"
)

const (
	centerPrefix     = "/config_center"
	filePrefix       = "temp_"
	fileType         = ".json"
	publicConfigName = "public.json"
)

type Center struct {
	zkHosts            []string
	zkConn             *zk.Conn
	RemotePath         string
	localPath          string
	name               string
	viper              *viper.Viper
	publicViper        *viper.Viper
	onlineMode         bool
	enablePublicConfig bool
	updateSuccess      bool
}

func (c *Center) SetOnlineMode(b bool) {
	c.onlineMode = b
}
func (c *Center) EnablePublicConfig(b bool) {
	c.enablePublicConfig = b
}

func (c *Center) publicLocalPathName() string {
	return path.Join(c.localPath, filePrefix+publicConfigName)
}

func (c *Center) localPathName() string {
	return path.Join(c.localPath, filePrefix+c.name)
}

func NewCenter(zkHosts []string, Path, Name string) (center *Center) {
	Path = path.Join(centerPrefix, Path)
	Name += fileType
	center = &Center{
		onlineMode:         true,
		enablePublicConfig: true,
		updateSuccess:      true,
	}
	center.zkHosts = zkHosts
	center.name = Name
	center.RemotePath = Path
	center.localPath = "./"
	center.viper = viper.New()
	center.publicViper = viper.New()
	return
}

func (c *Center) prepareConfig(viper2 *viper.Viper, remotePathName, localPathName string) (err error) {
	ifFileNotExistThenCreate(localPathName)

	if c.onlineMode {

		err = downloadConfig(c.zkConn, remotePathName, localPathName)
		if err != nil {
			err = fmt.Errorf("[config center]sync public config fail:\n%w", err)
			logrus.Error(err)
			c.updateSuccess = false
		}
	}
	viper2.SetConfigFile(localPathName)

	return
}

func (c *Center) Open() (err error) {
	conn, _, err := zk.Connect(c.zkHosts, time.Second*5)
	if err != nil {
		return
	}
	c.zkConn = conn

	err = c.prepareConfig(c.viper, path.Join(c.RemotePath, c.name), c.localPathName())

	if c.enablePublicConfig {
		err = c.prepareConfig(c.publicViper, path.Join(centerPrefix, publicConfigName), c.publicLocalPathName())
		c.SetPublicDefault()

		//同步线上公共配置
		err = c.syncPublic()
		if err != nil {
			err = fmt.Errorf("[config center]sync public config fail:\n%w", err)
			logrus.Error(err)
		}

	}

	return
}

func (c *Center) Update() (err error) {

	conn, _, err := zk.Connect(c.zkHosts, time.Second*5)
	if err != nil {
		return
	}
	c.zkConn = conn
	if c.enablePublicConfig {
		err = c.downloadPublic()
		if err != nil {
			logrus.Error("[config center]download public config fail")
		}
	}

	err = c.download()
	if err != nil {
		logrus.Error("[config center]download config fail")
	}

	return
}

type RedisConfig struct {
	// A seed list of host:port addresses of cluster nodes.
	Addrs []string

	// The maximum number of retries before giving up. Command is retried
	// on network errors and MOVED/ASK redirects.
	// Default is 8 retries.
	MaxRedirects int

	// Enables read-only commands on slave nodes.
	ReadOnly bool
	// Allows routing read-only commands to the closest master or slave node.
	// It automatically enables ReadOnly.
	RouteByLatency bool
	// Allows routing read-only commands to the random master or slave node.
	// It automatically enables ReadOnly.
	RouteRandomly bool

	Password string

	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	// PoolSize applies per cluster node and not for the whole cluster.
	PoolSize           int
	MinIdleConns       int
	MaxConnAge         time.Duration
	PoolTimeout        time.Duration
	IdleTimeout        time.Duration
	IdleCheckFrequency time.Duration
}

type SQLConfig struct {
	Host     string
	Port     int
	User     string
	Password string
}

type PostgreSQLConfig struct {
	Write SQLConfig
	Read  []SQLConfig
}

func (c *Center) SetPublicDefault() {
	redis := RedisConfig{}
	redis.Addrs = []string{"192.168.0.3:6379"}
	redis.Password = "asdf*123"
	c.publicViper.SetDefault("redis", redis)
	c.publicViper.SetDefault("hbase.thrift", "192.168.0.3:9090")
	c.publicViper.SetDefault("hbase.thrift2", "192.168.0.3:9090")

	pgcfgOne := SQLConfig{
		"192.168.0.3",
		5432,
		"sa",
		"asdf*123",
	}
	pgcfg := PostgreSQLConfig{
		Write: pgcfgOne,
		Read: []SQLConfig{
			pgcfgOne,
		},
	}
	c.publicViper.SetDefault("sql.postgres", pgcfg)

}

func (c *Center) SetDefault(key string, value interface{}) {
	c.viper.SetDefault(key, value)
}

func ifFileNotExistThenCreate(path string) {
	// 若配置文件不存在，则创建
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			file, err := os.Create(path)
			if err != nil {
				logrus.Panic(err)
			}
			_ = file.Close()
		} else {
			logrus.Panic(err)
		}
	}
}

func (c *Center) download() (err error) {
	remotePath := path.Join(c.RemotePath, c.name)
	localPathName := c.localPathName()
	err = downloadConfig(c.zkConn, remotePath, localPathName)
	return
}
func (c *Center) downloadPublic() (err error) {
	remotePath := path.Join(centerPrefix, publicConfigName)
	localPathName := c.publicLocalPathName()
	err = downloadConfig(c.zkConn, remotePath, localPathName)
	return
}

func downloadConfig(conn *zk.Conn, remotePathName, localPathName string) (err error) {
	var data []byte
	exist, _, err := conn.Exists(remotePathName)
	if err != nil {
		return
	}
	if !exist {
		logrus.Warn("[config center]config path not exist, will create:\n", remotePathName)
		return
	}
	data, _, err = conn.Get(remotePathName)
	if err != nil {
		return
	}
	err = ioutil.WriteFile(localPathName, data, os.ModePerm)
	return
}

func (c *Center) Sync() (err error) {
	err = c.sync(c.viper, c.RemotePath, c.localPathName(), c.name)
	return
}
func (c *Center) syncPublic() (err error) {
	err = c.sync(c.publicViper, centerPrefix, c.publicLocalPathName(), publicConfigName)
	return
}

func (c *Center) sync(viper2 *viper.Viper, remotePath, localPathName, configName string) (err error) {
	err = viper2.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		logrus.Warn(fmt.Sprintf("[config center]config file is empty:\n%s", err))
	}

	err = viper2.WriteConfig()
	if err != nil { // Handle errors reading the config file
		err = fmt.Errorf("writer config fail:\r%w", err)
		return
	}
	err = viper2.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		err = fmt.Errorf("read config fail:\r%w", err)
		return
	}

	if c.onlineMode && c.updateSuccess {
		err = upload(c.zkConn, remotePath, localPathName, configName)
		if err != nil { // Handle errors reading the config file
			err = fmt.Errorf("upload config fail:\r%w", err)
		}
	}

	return
}

func upload(conn *zk.Conn, remotePath, localPathName, configName string) (err error) {
	if remotePath == "" {
		err = errors.New("path is empty")
		return
	}
	Path := strings.TrimLeft(remotePath, "/")
	pathSlice := strings.Split(Path, "/")
	pathSlice = append(pathSlice, configName)

	Path = ""
	pathLayLen := len(pathSlice)
	data := []byte("")

	for i := 0; i < pathLayLen; i++ {
		Path += "/" + pathSlice[i]
		exist := false
		exist, _, err = conn.Exists(Path)
		if err != nil {
			return
		}
		if !exist {
			// permission
			var acls = zk.WorldACL(zk.PermAll)
			// create
			var flags int32 = 0

			_, err = conn.Create(Path, data, flags, acls)
			if err != nil {
				return
			}
		}
	}
	data, err = ioutil.ReadFile(localPathName)
	_, stat, err := conn.Get(Path)
	if err != nil {
		return
	}
	_, err = conn.Set(Path, data, stat.Version)

	return
}

func (c *Center) GetKafkaAddresses() (addrArr []string, err error) {
	brokersPath := "/brokers/ids"
	Children, _, err := c.zkConn.Children(brokersPath)
	if err != nil {
		err = fmt.Errorf("kafka not exist\n%w", err)
	}
	type KafkaAddress struct {
		Host string
		Port int
	}

	var data []byte
	for _, child := range Children {
		data, _, err = c.zkConn.Get(path.Join(brokersPath, child))
		if err != nil {
			logrus.Warn(fmt.Sprintf("[config center]kafka broker (%s) lost", child))
			continue
		}
		var addr KafkaAddress
		err = json.Unmarshal(data, &addr)
		if err != nil {
			return
		}

		addrArr = append(addrArr, fmt.Sprintf("%s:%d", addr.Host, addr.Port))
	}

	return
}

func (c *Center) GetHBaseThrift() string  { return c.publicViper.GetString("hbase.thrift") }
func (c *Center) GetHBaseThrift2() string { return c.publicViper.GetString("hbase.thrift2") }

func (c *Center) GetString(key string) string                    { return c.viper.GetString(key) }
func (c *Center) GetBool(key string) bool                        { return c.viper.GetBool(key) }
func (c *Center) GetInt(key string) int                          { return c.viper.GetInt(key) }
func (c *Center) GetInt32(key string) int32                      { return c.viper.GetInt32(key) }
func (c *Center) GetInt64(key string) int64                      { return c.viper.GetInt64(key) }
func (c *Center) GetUint(key string) uint                        { return c.viper.GetUint(key) }
func (c *Center) GetUint32(key string) uint32                    { return c.viper.GetUint32(key) }
func (c *Center) GetUint64(key string) uint64                    { return c.viper.GetUint64(key) }
func (c *Center) GetFloat64(key string) float64                  { return c.viper.GetFloat64(key) }
func (c *Center) GetTime(key string) time.Time                   { return c.viper.GetTime(key) }
func (c *Center) GetDuration(key string) time.Duration           { return c.viper.GetDuration(key) }
func (c *Center) GetIntSlice(key string) []int                   { return c.viper.GetIntSlice(key) }
func (c *Center) GetStringSlice(key string) []string             { return c.viper.GetStringSlice(key) }
func (c *Center) GetStringMap(key string) map[string]interface{} { return c.viper.GetStringMap(key) }
func (c *Center) GetStringMapString(key string) map[string]string {
	return c.viper.GetStringMapString(key)
}
func (c *Center) GetStringMapStringSlice(key string) map[string][]string {
	return c.viper.GetStringMapStringSlice(key)
}
func (c *Center) GetSizeInBytes(key string) uint { return c.viper.GetSizeInBytes(key) }
