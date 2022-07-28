## Molecula Consumer Kafka SASL 

#### Background

The goal of **Molecula Consumer Kafka SASL** is to allow IDK to connect to Kafka brokers with SASL-SSL enabled. Currently, the Kafka library we are using in **Molecula Consumer Kafka** and **Molecula Consumer Kafka Static** is Kafka-go from [Segment IO](https://github.com/segmentio/kafka-go) and it does not seem to support both SASL and SSL. After some research, we found that [Confluent Kafka Go](https://github.com/segmentio/kafka-go) is a tool we can use to connect to Kafka brokers with SASL and SSL. **Molecula Consumer Kafka SASL** was written based on **Molecula Consumer Kafka Static**.

#### Future

There should be a ticket coming soon to re-write **Molecula Consumer Kafka** with [Confluent Kafka Go](https://github.com/segmentio/kafka-go). At this moment, one of the challenges that will come up is finding a way to cross compile cgo through different architectures (**arm64!**) [*make docker-release*]. Once **Molecula Consumer Kafka** re-write with [Confluent Kafka Go](https://github.com/segmentio/kafka-go) is done, Kafka-go from [Segment IO](https://github.com/segmentio/kafka-go) should be removed as there is no need to have 2 libraries that basically do the same thing to exist within the project.

#### Kafka SASL

**Molecula Consumer Kafka SASL** should function and behave just like **Molecula Consumer Kafka Static**. The only difference is: **Molecula Consumer Kafka SASL** will be able to connect to Kafka brokers with SASL and SSL enabled by providing the needed information.

#### New options

The following options were added for Kafka SASL. None of the newly added are required.
- Username:   SASL authentication username
- Password:   SASL authentication password
- IdAlgorithm:    The endpoint identification algorithm used by clients to validate server host name. The default value is https. 
- SaslMechanism:  SASL mechanism to use for authentication. Range: GSSAPI, PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER
- SecurityProtocol:   Protocol used to communicate with brokers. Range: plaintext, ssl, sasl_plaintext, sasl_ssl
- SslCaLocation:  File or directory path to CA certificate(s) for verifying the broker's key.
- SslCertificateLocation: Path to client's public key (PEM) used for authentication.
- SslKeyLocation: Path to client's private key (PEM) used for authentication.
- SslKeyPassword: Private key passphrase (for use with ssl.key.location and set_ssl_cert()).

#### How to start Kafka SASL and connect to Lccal IDK Broker with SASL and SSL enabled

1. Start Local IDK by navigating to project IDK then enter: ***make shutdown && sudo make startup***
    - Looking at **docker-compose.yml**, non SASL & non SSL broker is at kafka:9092
    - SASL & SSL broker is at kafka:9094
2. Connect to kafka:9094 
    - The SASL and SSL information needed to connect to kafka:9094 is under docker-sasl/client-ssl-test.conf
    - test connection by listing the topics in kafka:9094:
        - kafka-topics --bootstrap-server kafka:9094 --list **--command-config client-ssl-test.conf.conf**
3. Create a new topic to test sending/receiving messages
    - kafka-topics --bootstrap-server kafka:9094 --create --replication-factor 1 --partitions 10 **--topic testSasl** --command-config client-ssl-test.conf
4. Create a header file
    - Since **Molecula Consumer Kafka SASL** was written based on **Molecula Consumer Kafka Static**, it needs a header file that has a schema inside it
    - Example header.json:
    ```
         [
            {
            "name":"test_string",
            "path":[
                "s"
            ],
            "type":"string"
            }
        ]
    ```
    - example message that matches the above schema: **{"s":"test 1"}**
5. Create a producer
    - kafka-console-producer --bootstrap-server kafka:9094 **--topic testSasl** --producer.config client-ssl-test.conf
6. SSL Certificates:   
    - There is a script to generate SSL certificates under docker-sasl/ssl_keys/gen-ssl-certs.sh
    - Can also use existing certs in the above folder and feed cert files to molecula-consumer-kafka-sasl 
    - These certs are needed since in this scenario, we are using kafka with SSL enabled on localhost so we have to geenrate self-signed SSL certs.
7. Run molecula-consumer-kafka-sasl
    - If binary is not there, build by using **go build**
    - Run :
        - ./molecula-consumer-kafka-sasl --auto-generate --index test_string --group testGroup --header header.json --topics testSasl --kafka-hosts kafka:9094 --kafka-sasl.username kafkaClient1 --kafka-sasl.password kafkaClient1pw --kafka-sasl.sasl-mechanism PLAIN --kafka-sasl.security-protocol SASL_SSL --kafka-sasl.ssl-ca-location ../../docker-sasl/ssl_keys/ca-cert --kafka-sasl.ssl-certificate-location ../../docker-sasl/ssl_keys/client_kafkaClient_client.pem --kafka-sasl.ssl-key-location ../../docker-sasl/ssl_keys/client_kafkaClient_client.key --kafka-sasl.ssl-key-password 123456
8. Send a message with kafka-console-producer
9. molecula-consumer-kafka-sasl should be able to receive the message and commit it to locally
10. Double check by going to local pilosa (http://127.0.0.1:10101/) -> query builder -> select test_string in the Table dropdown -> Run
    - the results should show the messages that were sent by kafka-console-producer
