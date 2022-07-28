package idk

import (
	"fmt"

	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

// ReadString reads the string value for the specified column, from the row with
// the specified id.
func (c *PostgresClient) ReadString(id uint64, column string) (string, error) {
	sql := fmt.Sprintf("SELECT %s FROM %s WHERE id = %d", column, c.TableName, id)

	var value string
	row := c.client.QueryRow(sql)

	err := row.Scan(&value)
	if err != nil {
		return "", errors.Wrap(err, "querying")
	}
	return value, nil
}

// RowExists returns true if a row exists containing the specified id.
func (c *PostgresClient) RowExists(id uint64) (bool, error) {
	sql := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE id = %d", c.TableName, id)

	var value int
	err := c.client.QueryRow(sql).Scan(&value)
	if err != nil {
		return false, errors.Wrap(err, "querying")
	}
	return value != 0, nil
}

// DropTable drops the PostgresClient's table from the Postgres backend, for test cleanup.
func (c *PostgresClient) DropTable() error {
	_, err := c.client.Exec(fmt.Sprintf("DROP TABLE %s", c.TableName))
	return err
}
