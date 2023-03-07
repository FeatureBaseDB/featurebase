package batch

// Inserter can be implemented by anything which can handle a SQL statement
// representing a write operation. An example is `BULK INSERT`. The Insert()
// method on this interface does not return any results other than an error.
type Inserter interface {
	Insert(sql string) error
}
