package boltdb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/errors"
	"github.com/molecula/featurebase/v3/logger"
)

var (
	bucketNodes = Bucket("nodeServiceNodes")
)

// NodeServiceBuckets defines the buckets used by this package. It can be called
// during setup to create the buckets ahead of time.
var NodeServiceBuckets []Bucket = []Bucket{
	bucketNodes,
}

// Ensure type implements interface.
var _ dax.NodeService = (*NodeService)(nil)

// NodeService represents a service for managing nodes.
type NodeService struct {
	db *DB

	logger logger.Logger
}

// NewNodeService returns a new instance of NodeService with default values.
func NewNodeService(db *DB, logger logger.Logger) *NodeService {
	return &NodeService{
		db:     db,
		logger: logger,
	}
}

func (s *NodeService) CreateNode(ctx context.Context, addr dax.Address, node *dax.Node) error {
	tx, err := s.db.BeginTx(ctx, true)
	if err != nil {
		return errors.Wrap(err, "getting transaction")
	}
	defer tx.Rollback()

	bkt := tx.Bucket(bucketNodes)
	if bkt == nil {
		return errors.Errorf(ErrFmtBucketNotFound, bucketNodes)
	}

	val, err := json.Marshal(node)
	if err != nil {
		return errors.Wrap(err, "marshalling node to json")
	}

	if err := bkt.Put(addressKey(addr), val); err != nil {
		return errors.Wrap(err, "putting node")
	}

	return tx.Commit()
}

func (s *NodeService) ReadNode(ctx context.Context, addr dax.Address) (*dax.Node, error) {
	tx, err := s.db.BeginTx(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	bkt := tx.Bucket(bucketNodes)
	if bkt == nil {
		return nil, errors.Errorf(ErrFmtBucketNotFound, bucketNodes)
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

func (s *NodeService) DeleteNode(ctx context.Context, addr dax.Address) error {
	tx, err := s.db.BeginTx(ctx, true)
	if err != nil {
		return errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	bkt := tx.Bucket(bucketNodes)
	if bkt == nil {
		return errors.Errorf(ErrFmtBucketNotFound, bucketNodes)
	}

	if err := bkt.Delete(addressKey(addr)); err != nil {
		return errors.Wrapf(err, "deleting node key: %s", addressKey(addr))
	}

	return tx.Commit()
}

func (s *NodeService) Nodes(ctx context.Context) ([]*dax.Node, error) {
	tx, err := s.db.BeginTx(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "getting tx")
	}
	defer tx.Rollback()

	nodes, err := s.getNodes(ctx, tx)
	if err != nil {
		return nil, errors.Wrap(err, "getting nodes")
	}

	return nodes, nil
}

func (s *NodeService) getNodes(ctx context.Context, tx *Tx) ([]*dax.Node, error) {
	c := tx.Bucket(bucketNodes).Cursor()

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
