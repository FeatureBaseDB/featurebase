package bankgen

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestBankgen(t *testing.T) {
	p, err := NewPutCmd()
	if err != nil {
		t.Fatalf("failed to get NewPutCmd: %v", err)
	}

	p.KafkaBootstrapServers = []string{"kafka:9092"}
	p.SchemaRegistryURL = "http://schema-registry:8081"
	p.Topic = fmt.Sprintf("topic-%d", rand.New(rand.NewSource(time.Now().Unix())).Uint64())
	p.NumRecords = 100

	err = p.Run()
	if err != nil {
		t.Fatalf("failed to run bankgen: %v", err)
	}

}
