// creator contains code for standing up pilosa clusters
package creator

import "io"

type Cluster interface {
	Start() error
	Hosts() []string
	Shutdown() error
	Logs() []io.Reader
}
