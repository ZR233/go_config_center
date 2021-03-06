package go_config_centor

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ZR233/go_config_center/log"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/spf13/viper"
	"io/ioutil"
	"os"
	"path"
	"time"
)

const (
	centerPrefix     = "/config_center"
	filePrefix       = "temp_"
	fileType         = ".yaml"
	publicConfigName = "public.yaml"
)

type Center struct {
	localPath string
	online    *OptionOnlineMode
	name      string
	*viper.Viper
}

func (c *Center) publicLocalPathName() string {
	return path.Join(c.localPath, filePrefix+publicConfigName)
}

func (c *Center) localPathName() string {
	return path.Join(c.localPath, filePrefix+c.name)
}

type Option interface {
	set(center *Center) (err error)
}

//开启在线配置中心
type OptionOnlineMode struct {
	zkConfigPath string
	remoteDir    string
	zkViper      *viper.Viper
	zkConn       *zk.Conn
	center       *Center
}

func NewOptionOnlineMode(remoteDir, zkConfigPath string) (o *OptionOnlineMode) {
	o = &OptionOnlineMode{}
	o.remoteDir = path.Join(centerPrefix, remoteDir)
	o.zkConfigPath = zkConfigPath
	o.zkViper = viper.New()
	return
}

func (o *OptionOnlineMode) set(center *Center) (err error) {
	o.center = center
	center.online = o
	return
}

//配置文件存储位置
type OptionLocalConfigPath string

func (o OptionLocalConfigPath) set(center *Center) (err error) {
	center.localPath = string(o)
	return
}

type OptionZKConfigPath string

func (o OptionZKConfigPath) set(center *Center) (err error) {
	center.localPath = string(o)
	return
}

func NewCenter(Name string) (center *Center) {
	//Path = path.Join(centerPrefix, Path)
	Name += fileType
	center = &Center{
		//enablePublicConfig: true,
		//updateSuccess:      true,
	}
	center.name = Name
	//center.RemotePath = Path
	center.localPath = "./"
	center.Viper = viper.New()

	return
}
func (c *Center) Open(options ...Option) (msg []string, err error) {

	for _, option := range options {
		err = option.set(c)
		if err != nil {
			return
		}
	}

	if c.online != nil {
		err = c.online.open()
		//加载线上配置失败，读取上次配置
		if err != nil {
			err = fmt.Errorf("online mode open fail, read last config\n%w", err)
			msg = append(msg, err.Error())
		}
	}
	c.SetConfigFile(c.localPathName())
	err = c.ReadInConfig()
	if err != nil {
		err = fmt.Errorf("private config read fail\n%w", err)
		return
	}

	return
}

func (o *OptionOnlineMode) open() (err error) {
	err = o.readZkConfig()
	if err != nil {
		err = fmt.Errorf("read zk config fail\n%w", err)
		return
	}

	err = o.zkConnect()
	if err != nil {
		err = fmt.Errorf("zk connect\n%w", err)
		return
	}

	err = o.download(o.center.Viper, path.Join(o.remoteDir, o.center.name), o.center.localPathName())
	if err != nil {
		err = fmt.Errorf("download private config fail\n%w", err)
		return
	}

	//if o.center.enablePublicConfig{
	//	err = o.download(o.center.publicViper, path.Join(centerPrefix, publicConfigName), o.center.publicLocalPathName())
	//	if err !=nil{
	//		err = fmt.Errorf("download public config fail\n%w", err)
	//		return
	//	}
	//}

	return
}
func (o *OptionOnlineMode) readZkConfig() (err error) {
	o.zkViper.SetConfigFile(o.zkConfigPath)
	err = o.zkViper.ReadInConfig()
	if err != nil {
		return
	}
	return
}
func (o *OptionOnlineMode) zkConnect() (err error) {
	zkHosts := o.zkViper.GetStringSlice("hosts")
	o.zkConn, _, err = zk.Connect(zkHosts, time.Second*5)
	if err != nil {
		return
	}
	return
}
func (o *OptionOnlineMode) download(viperCase *viper.Viper, remotePathName, localPathName string) (err error) {
	err = downloadConfig(o.zkConn, remotePathName, localPathName)
	if err != nil {
		err = fmt.Errorf("download config fail:\n%w", err)
		return
	}
	viperCase.SetConfigFile(localPathName)
	return
}

//func (c *Center) SetLocalConfigPath(localPath string) {
//	c.localPath = localPath
//}

//func (c *Center) prepareConfig(viper2 *viper.Viper, remotePathName, localPathName string) (err error) {
//	err = ifFileNotExistThenCreate(localPathName)
//	if err != nil {
//		return
//	}
//
//	if c.onlineMode {
//
//		err = downloadConfig(c.zkConn, remotePathName, localPathName)
//		if err != nil {
//			err = fmt.Errorf("[config center]sync public config fail:\n%w", err)
//			log.Error(err)
//			c.updateSuccess = false
//		}
//	}
//	viper2.SetConfigFile(localPathName)
//
//	return
//}

//func (c *Center) GetFileConfig() *FileConfig {
//	return c.fileConfig
//}

//func (c *Center) Update() (err error) {
//	err = c.prepareConfig(c.viper, path.Join(c.RemotePath, c.name), c.localPathName())
//
//	if c.enablePublicConfig {
//		err = c.prepareConfig(c.publicViper, path.Join(centerPrefix, publicConfigName), c.publicLocalPathName())
//		c.SetPublicDefault()
//
//		//同步线上公共配置
//		err = c.syncPublic()
//		if err != nil {
//			err = fmt.Errorf("[config center]sync public config fail:\n%w", err)
//			log.Error(err)
//		}
//	}
//
//	return
//}
type PublicConfig struct {
	Redis RedisConfig
}
type RedisConfig struct {
	Type       string
	MasterName string
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

//func (c *Center) SetPublicDefault() {
//	redis := RedisConfig{}
//	redis.Type = "single"
//	redis.MasterName = "mymaster"
//	redis.Addrs = []string{"192.168.0.3:6379"}
//	redis.Password = "asdf*123"
//	c.publicViper.SetDefault("redis", redis)
//	c.publicViper.SetDefault("hbase.thrift", "192.168.0.3:9090")
//	c.publicViper.SetDefault("hbase.thrift2", "192.168.0.3:9090")
//
//	pgcfgOne := SQLConfig{
//		"192.168.0.3",
//		5432,
//		"sa",
//		"asdf*123",
//	}
//	pgcfg := PostgreSQLConfig{
//		Write: pgcfgOne,
//		Read: []SQLConfig{
//			pgcfgOne,
//		},
//	}
//	c.publicViper.SetDefault("sql.postgres", pgcfg)
//
//}

//func ifFileNotExistThenCreate(path string) (err error) {
//	defer func() {
//		if err != nil {
//			err = fmt.Errorf("create file fail (%s):\n%w", path, err)
//		}
//	}()
//
//	// 若配置文件不存在，则创建
//	if _, err = os.Stat(path); err != nil {
//		if os.IsNotExist(err) {
//			var file *os.File
//			file, err = os.Create(path)
//			if err != nil {
//				return
//			}
//			_ = file.Close()
//		} else {
//			return
//		}
//	}
//	return
//}

//func (c *Center) download() (err error) {
//	remotePath := path.Join(c.RemotePath, c.name)
//	localPathName := c.localPathName()
//	err = downloadConfig(c.zkConn, remotePath, localPathName)
//	return
//}
//func (c *Center) downloadPublic() (err error) {
//	remotePath := path.Join(centerPrefix, publicConfigName)
//	localPathName := c.publicLocalPathName()
//	err = downloadConfig(c.zkConn, remotePath, localPathName)
//	return
//}

func downloadConfig(conn *zk.Conn, remotePathName, localPathName string) (err error) {
	var data []byte
	exist, _, err := conn.Exists(remotePathName)
	if err != nil {
		return
	}
	if !exist {
		log.Warn("[config center]config path not exist, will create:\n", remotePathName)
		return
	}
	data, _, err = conn.Get(remotePathName)
	if err != nil {
		return
	}
	err = ioutil.WriteFile(localPathName, data, os.ModePerm)
	return
}

//func (c *Center) Sync() (err error) {
//	err = c.sync(c.viper, c.RemotePath, c.localPathName(), c.name)
//	return
//}
//func (c *Center) syncPublic() (err error) {
//	err = c.sync(c.publicViper, centerPrefix, c.publicLocalPathName(), publicConfigName)
//	return
//}
//
//func syncViperFile(viper2 *viper.Viper) (err error) {
//	defer func() {
//		if err != nil {
//			err = fmt.Errorf("sync file fail:\n%w", err)
//		}
//	}()
//
//	err = viper2.ReadInConfig() // Find and read the config file
//	if err != nil {             // Handle errors reading the config file
//		log.Warn(fmt.Sprintf("[config center]config file is empty:\n%s", err))
//	}
//
//	err = viper2.WriteConfig()
//	if err != nil { // Handle errors reading the config file
//		err = fmt.Errorf("writer config fail:\r%w", err)
//		return
//	}
//	err = viper2.ReadInConfig() // Find and read the config file
//	if err != nil {             // Handle errors reading the config file
//		err = fmt.Errorf("read config fail:\r%w", err)
//		return
//	}
//
//	return
//}
//
//func (c *Center) sync(viper2 *viper.Viper, remotePath, localPathName, configName string) (err error) {
//	err = syncViperFile(viper2)
//	if err != nil {
//		return
//	}
//
//	if c.onlineMode && c.updateSuccess {
//		err = upload(c.zkConn, remotePath, localPathName, configName)
//		if err != nil { // Handle errors reading the config file
//			err = fmt.Errorf("upload config fail:\r%w", err)
//		}
//	}
//
//	return
//}
//
//func upload(conn *zk.Conn, remotePath, localPathName, configName string) (err error) {
//	if remotePath == "" {
//		err = errors.New("path is empty")
//		return
//	}
//	Path := strings.TrimLeft(remotePath, "/")
//	pathSlice := strings.Split(Path, "/")
//	pathSlice = append(pathSlice, configName)
//
//	Path = ""
//	pathLayLen := len(pathSlice)
//	data := []byte("")
//
//	for i := 0; i < pathLayLen; i++ {
//		Path += "/" + pathSlice[i]
//		exist := false
//		exist, _, err = conn.Exists(Path)
//		if err != nil {
//			return
//		}
//		if !exist {
//			// permission
//			var acls = zk.WorldACL(zk.PermAll)
//
//			// create
//			var flags int32 = 0
//
//			_, err = conn.Create(Path, data, flags, acls)
//			if err != nil {
//				return
//			}
//		}
//	}
//	data, err = ioutil.ReadFile(localPathName)
//	_, stat, err := conn.Get(Path)
//	if err != nil {
//		return
//	}
//	_, err = conn.Set(Path, data, stat.Version)
//
//	return
//}

func (c *Center) GetKafkaAddresses() (addrArr []string, err error) {
	brokersPath := "/brokers/ids"
	Children, _, err := c.online.zkConn.Children(brokersPath)
	if err != nil {
		err = fmt.Errorf("kafka not exist\n%w", err)
	}
	type KafkaAddress struct {
		Host string
		Port int
	}

	var data []byte
	for _, child := range Children {
		data, _, err = c.online.zkConn.Get(path.Join(brokersPath, child))
		if err != nil {
			log.Warn(fmt.Sprintf("[config center]kafka broker (%s) lost", child))
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

func (c *Center) GetZKHosts() (addrArr []string, err error) {
	if c.online == nil {
		err = errors.New("online mode off")
		return
	}
	addrArr = c.online.zkViper.GetStringSlice("hosts")
	return
}
