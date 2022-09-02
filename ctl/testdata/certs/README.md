

# these test certs were generated with the following commands

certstrap --depot-path certs init --common-name pilosa-ca --expires "100 years"
certstrap --depot-path certs request-cert --common-name localhost --domain localhost
certstrap --depot-path certs sign "localhost" --CA pilosa-ca --expires "100 years"

# certstrap version
dev-25ea708a

(built with go 1.13)
