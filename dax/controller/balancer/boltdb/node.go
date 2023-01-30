package boltdb

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/boltdb"
	"github.com/featurebasedb/featurebase/v3/dax/controller"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
)

// Ensure type implements interface.
var _ controller.NodeService = (*NodeService)(nil)

// NodeService represents a service for managing nodes.
type NodeService struct {
	db *boltdb.DB

	logger logger.Logger
}

// NewNodeService returns a new instance of NodeService with default values.
func NewNodeService(db *boltdb.DB, logger logger.Logger) *NodeService {
	return &NodeService{
		db:     db,
		logger: logger,
	}
}

func (s *NodeService) CreateNode(tx dax.Transaction, addr dax.Address, node *dax.Node) error {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return dax.NewErrInvalidTransaction()
	}

	bkt := txx.Bucket(bucketBalancer)
	if bkt == nil {
		return errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketBalancer)
	}

	val, err := json.Marshal(node)
	if err != nil {
		return errors.Wrap(err, "marshalling node to json")
	}

	if err := bkt.Put(addressKey(addr), val); err != nil {
		return errors.Wrap(err, "putting node")
	}

	return nil
}

func (s *NodeService) ReadNode(tx dax.Transaction, addr dax.Address) (*dax.Node, error) {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return nil, dax.NewErrInvalidTransaction()
	}

	bkt := txx.Bucket(bucketBalancer)
	if bkt == nil {
		return nil, errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketBalancer)
	}

	b := bkt.Get(addressKey(addr))
	if b == nil {
		return nil, dax.NewErrNodeDoesNotExist(addr)
	}

	node := &dax.Node{}
	if err := json.Unmarshal(b, node); err != nil {
		return nil, errors.Wrap(err, "unmarshalling node json")
	}

	return node, nil
}

func (s *NodeService) DeleteNode(tx dax.Transaction, addr dax.Address) error {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return dax.NewErrInvalidTransaction()
	}

	bkt := txx.Bucket(bucketBalancer)
	if bkt == nil {
		return errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketBalancer)
	}

	if err := bkt.Delete(addressKey(addr)); err != nil {
		return errors.Wrapf(err, "deleting node key: %s", addressKey(addr))
	}

	return nil
}

func (s *NodeService) Nodes(tx dax.Transaction) ([]*dax.Node, error) {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return nil, dax.NewErrInvalidTransaction()
	}

	nodes, err := s.getNodes(txx)
	if err != nil {
		return nil, errors.Wrap(err, "getting nodes")
	}

	return nodes, nil
}

func (s *NodeService) getNodes(tx *boltdb.Tx) ([]*dax.Node, error) {
	c := tx.Bucket(bucketBalancer).Cursor()

	// Deserialize rows into Node objects.
	nodes := make([]*dax.Node, 0)

	prefix := []byte(prefixFmtNodes)
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		if v == nil {
			s.logger.Printf("nil value for key: %s", k)
			continue
		}

		node := &dax.Node{}
		if err := json.Unmarshal(v, node); err != nil {
			return nil, errors.Wrap(err, "unmarshalling node json")
		}

		nodes = append(nodes, node)
	}

	return nodes, nil
}

const (
	prefixFmtNodes = "nodes/"
)

// addressKey returns a key based on address.
func addressKey(addr dax.Address) []byte {
	key := fmt.Sprintf(prefixFmtNodes+"%s", addr)
	return []byte(key)
}
