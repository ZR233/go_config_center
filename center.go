package go_config_centor

import (
	"errors"
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
	centerPrefix = "/config_center"
	filePrefix   = "temp_"
	fileType     = ".json"
)

type Center struct {
	zkConn     *zk.Conn
	RemotePath string
	localPath  string
	name       string
	viper      *viper.Viper
}

func (c *Center) localPathName() string {
	return path.Join(c.localPath, filePrefix+c.name)
}

func NewCenter(zkHosts []string, Path, Name string) (center *Center, err error) {
	Path = path.Join(centerPrefix, Path)
	Name += fileType
	center = &Center{}
	conn, _, err := zk.Connect(zkHosts, time.Second*5)
	if err != nil {
		return
	}
	center.name = Name
	center.zkConn = conn
	center.RemotePath = Path
	center.localPath = "./"
	center.viper = viper.New()

	localPathName := center.localPathName()
	ifFileNotExistThenCreate(localPathName)

	err = center.download()
	if err != nil {
		logrus.Error("[config center]download config fail")
	}

	center.viper.SetConfigFile(localPathName)
	return
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
	var data []byte

	remotePath := path.Join(c.RemotePath, c.name)

	exist, _, err := c.zkConn.Exists(remotePath)
	if err != nil {
		return
	}
	if !exist {
		logrus.Warn("[config center]config path not exist, will create:\n", remotePath)
		return
	}
	data, _, err = c.zkConn.Get(remotePath)
	if err != nil {
		return
	}
	err = ioutil.WriteFile(c.localPathName(), data, os.ModePerm)
	return
}

func (c *Center) Sync() (err error) {
	err = c.viper.ReadInConfig() // Find and read the config file
	if err != nil {              // Handle errors reading the config file
		logrus.Warn("config file is empty")
	}

	err = c.viper.WriteConfig()
	if err != nil { // Handle errors reading the config file
		return
	}
	err = c.viper.ReadInConfig() // Find and read the config file
	if err != nil {              // Handle errors reading the config file
		return
	}

	err = c.upload()
	if err != nil { // Handle errors reading the config file
		logrus.Error("[config center]upload config fail:\n", err)
	}

	return
}

func (c *Center) upload() (err error) {
	if c.RemotePath == "" {
		err = errors.New("path is empty")
		return
	}
	Path := strings.TrimLeft(c.RemotePath, "/")
	pathSlice := strings.Split(Path, "/")
	pathSlice = append(pathSlice, c.name)

	Path = ""
	pathLayLen := len(pathSlice)
	data := []byte("")

	for i := 0; i < pathLayLen; i++ {
		Path += "/" + pathSlice[i]
		exist := false
		exist, _, err = c.zkConn.Exists(Path)
		if err != nil {
			return
		}
		if !exist {
			// permission
			var acls = zk.WorldACL(zk.PermAll)
			// create
			var flags int32 = 0

			_, err = c.zkConn.Create(Path, data, flags, acls)
			if err != nil {
				return
			}
		}
	}
	data, err = ioutil.ReadFile(c.localPathName())
	_, stat, err := c.zkConn.Get(Path)
	if err != nil {
		return
	}
	_, err = c.zkConn.Set(Path, data, stat.Version)

	return
}

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
