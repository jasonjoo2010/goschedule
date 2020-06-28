module github.com/jasonjoo2010/goschedule/store/etcdv3

go 1.14

require (
	github.com/coreos/etcd v3.3.22+incompatible
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/jasonjoo2010/enhanced-utils v0.0.1
	github.com/jasonjoo2010/enhanced-utils/concurrent/distlock/etcdv3 v0.0.1
	github.com/jasonjoo2010/goschedule v0.1.0
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.5.1
	go.uber.org/zap v1.15.0 // indirect
	google.golang.org/grpc v1.26.0
)

replace github.com/coreos/bbolt v1.3.4 => go.etcd.io/bbolt v1.3.4
