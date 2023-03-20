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
}

type CloudAuthConfig struct {
	ClientID string `json:"client-id"`
	Region   string `json:"region"`
	Email    string `json:"email"`
	Password string `json:"password"`
}
