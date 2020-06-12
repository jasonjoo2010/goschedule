module github.com/jasonjoo2010/goschedule/store/etcdv2

go 1.14

require (
	github.com/coreos/etcd v3.3.22+incompatible
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/jasonjoo2010/enhanced-utils v0.0.0-20200608114736-cca38f61401f
	github.com/jasonjoo2010/goschedule v0.0.0-20200608114925-f12e4b874176
	github.com/sirupsen/logrus v1.6.0
	github.com/stretchr/testify v1.5.1
)

replace github.com/coreos/bbolt v1.3.4 => go.etcd.io/bbolt v1.3.4
