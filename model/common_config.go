package model

type CommonConfig struct {
	Hbase struct {
		Thrift  string
		Thrift2 string
	}
	Redis struct {
		Type       string
		MasterName string
		// A seed list of host:port addresses of cluster nodes.
		Addrs    []string
		Password string
	}

	Postgresql SQL

	Zookeeper struct {
		Hosts []string
	}
}

type SQL struct {
	DBName string
	Read   []SQLBase
	Write  SQLBase
}
type SQLBase struct {
	Host     string
	Password string
	Port     int
	User     string
}
