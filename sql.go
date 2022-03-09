package pilosa

type SQLResponse struct {
	Schema        SQLSchema       `json:"schema"`
	Data          [][]interface{} `json:"data"`
	Error         string          `json:"error"`
	Warnings      []string        `json:"warnings"`
	ExecutionTime int64           `json:"execution-time"`
}
