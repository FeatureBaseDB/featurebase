package pilosactl

import (
	"fmt"
	"net"
	"os"
	"os/user"
	"strings"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

type SSH struct {
	client *ssh.Client
}

// NewSSH wraps up some of the complexity of using the crypto/ssh pacakge
// directly assuming you want to connect using public key auth and you can pass
// a keyfile or your key is accessible through ssh agent.
func NewSSH(host, username, keyfile string) (*SSH, error) {
	if username == "" {
		user, err := user.Current()
		if err != nil {
			return nil, err
		}
		username = user.Username
	}

	var auth ssh.AuthMethod
	if keyfile == "" {
		sshAgent, err := net.Dial("unix", os.Getenv("SSH_AUTH_SOCK"))
		if err != nil {
			return nil, err
		}
		auth = ssh.PublicKeysCallback(agent.NewClient(sshAgent).Signers)
	} else {
		return nil, fmt.Errorf("using a keyfile is unimplemented")
	}

	config := &ssh.ClientConfig{
		User: username,
		Auth: []ssh.AuthMethod{auth},
	}

	if strings.Index(host, ":") == -1 {
		host = host + ":22"
	}

	client, err := ssh.Dial("tcp", host, config)
	if err != nil {
		return nil, fmt.Errorf("NewSHH failed Dial: %v ", err)
	}

	return &SSH{client: client}, nil
}

func SSHClients(hosts []string, username, keyfile string) ([]*SSH, error) {
	clients := make([]*SSH, len(hosts))
	for i, host := range hosts {
		client, err := NewSSH(host, username, keyfile)
		if err != nil {
			return nil, err
		}
		clients[i] = client
	}
	return clients, nil
}

func (s *SSH) NewSession() (*ssh.Session, error) {
	return s.client.NewSession()
}
