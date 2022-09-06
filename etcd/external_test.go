package etcd

import (
	"testing"

	"github.com/featurebasedb/featurebase/v3/logger"
)

func TestNewExternalEtcd(t *testing.T) {
	logger := logger.NewLogfLogger(t)

	type args struct {
		p *Etcd
		o Options
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{name: "constructor nil", args: args{nil, Options{}}, want: "", wantErr: true},
		{name: "constructor", args: args{&Etcd{knownNodes: make(map[string]*nodeData), logger: logger}, Options{
			Id:        "f0",
			Cluster:   "f0=http://localhost:10101",
			EtcdHosts: "0.0.0.0:2329",
		}}, want: "f0", wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewExternalEtcd(tt.args.p, tt.args.o)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewExternalEtcd() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr == false && got.ID() != tt.want {
				t.Errorf("NewExternalEtcd() = %v, want %v", got.id, tt.want)
			}
		})
	}
}
