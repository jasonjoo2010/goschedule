module github.com/jasonjoo2010/goschedule/store/etcdv2

go 1.14

require (
	github.com/coreos/etcd v3.3.22+incompatible
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/jasonjoo2010/goschedule v1.0.0
	github.com/json-iterator/go v1.1.11 // indirect
	github.com/labstack/gommon v0.3.0
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/stretchr/testify v1.7.0
	google.golang.org/appengine v1.6.7
)

replace github.com/coreos/bbolt v1.3.4 => go.etcd.io/bbolt v1.3.4
