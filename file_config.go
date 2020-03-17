package go_config_centor

import (
	"github.com/spf13/viper"
)

type FileConfig struct {
	*viper.Viper
	center *Center
}

//func newFileConfig(center *Center) (f *FileConfig, err error) {
//	f = &FileConfig{
//		Viper:  viper.New(),
//		center: center,
//	}
//	filePathName := path.Join(center.localPath, strings.TrimRight(center.name, "."+path.Ext(center.name))+".yaml")
//	err = ifFileNotExistThenCreate(filePathName)
//	if err != nil {
//		return
//	}
//
//	f.SetConfigFile(filePathName)
//	f.SetDefault("zk", []string{
//		"192.168.0.3:2181",
//	})
//	f.SetDefault("debug", true)
//
//	err = f.Sync()
//	if err != nil {
//		return
//	}
//
//	center.zkHosts = f.GetStringSlice("zk")
//	return
//}

//func (f *FileConfig) Sync() error {
//	return syncViperFile(f.Viper)
//}

func (f *FileConfig) Debug() bool {
	return f.GetBool("debug")
}

func (f *FileConfig) GetZKHosts() []string {
	return f.GetStringSlice("zk")
}
