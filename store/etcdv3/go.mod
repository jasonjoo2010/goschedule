module github.com/jasonjoo2010/goschedule/store/etcdv3

go 1.14

require (
	github.com/coreos/etcd v3.3.22+incompatible
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/jasonjoo2010/goschedule v0.0.0-20210516080810-02f9975abcd9
	github.com/labstack/gommon v0.3.0
	github.com/sirupsen/logrus v1.6.0
	github.com/stretchr/testify v1.7.0
	go.uber.org/zap v1.16.0 // indirect
	google.golang.org/genproto v0.0.0-20210513213006-bf773b8c8384 // indirect
)

replace (
	github.com/coreos/bbolt v1.3.4 => go.etcd.io/bbolt v1.3.4
	google.golang.org/grpc => google.golang.org/grpc v1.26.0
)
