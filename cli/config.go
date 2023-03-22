package cli

// Config represents the configuration for the command.
type Config struct {
	Host string `json:"host"`
	Port string `json:"port"`

	OrganizationID string `json:"org-id"`
	Database       string `json:"db"`

	// CloudAuth
	CloudAuth CloudAuthConfig `json:"cloud-auth"`

	// Kafka
	KafkaConfig string `json:"kafka-config"`

	HistoryPath string `json:"history-path"`

	// CSV (Comma-Separated Values) table output mode.
	CSV bool `json:"csv"`
}

type CloudAuthConfig struct {
	ClientID string `json:"client-id"`
	Region   string `json:"region"`
	Email    string `json:"email"`
	Password string `json:"password"`
}
