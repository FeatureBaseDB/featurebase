package remote

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"

	"code.google.com/p/go.crypto/ssh"
)

type keychain struct {
	keys []ssh.Signer
}

func (k *keychain) Key(i int) (ssh.PublicKey, error) {
	if i < 0 || i >= len(k.keys) {
		return nil, nil
	}
	return k.keys[i].PublicKey(), nil
}

func (k *keychain) Sign(i int, rand io.Reader, data []byte) (sig []byte, err error) {
	return k.keys[i].Sign(rand, data)
}

func (k *keychain) add(key ssh.Signer) {
	k.keys = append(k.keys, key)
}

func (k *keychain) loadPEM(file string) error {
	buf, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	key, err := ssh.ParsePrivateKey(buf)
	if err != nil {
		return err
	}
	k.add(key)
	return nil
}

func (k *keychain) loadPEMString(buf string) error {
	key, err := ssh.ParsePrivateKey([]byte(buf))
	if err != nil {
		return err
	}
	k.add(key)
	return nil
}

type SSH struct {
	client *ssh.ClientConn
}

func (self *SSH) Launch(command string, server_log_path string) error {
	command = fmt.Sprintf("/usr/bin/nohup bash -c \\\n\"%s\" `</dev/null` >%s 2>&1 &", command, server_log_path)
	var b bytes.Buffer
	var e bytes.Buffer
	session, _ := self.client.NewSession()
	defer session.Close()
	session.Stdout = &b
	session.Stderr = &e
	return session.Run(command)
}

func (self *SSH) Run(command string, sudo bool) (string, error) {
	var b bytes.Buffer
	var e bytes.Buffer
	session, _ := self.client.NewSession()
	defer session.Close()
	session.Stdout = &b
	session.Stderr = &e
	if sudo {
		command = fmt.Sprintf("/usr/bin/sudo bash <<CMD\nexport PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin\n%s\nCMD", command)
	} else {
		command = fmt.Sprintf("bash <<CMD\nexport PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin\n%s\nCMD", command)
	}

	if err := session.Run(command); err != nil {
		//error
		return e.String(), err
	}
	return b.String(), nil
}

func (self *SSH) CopyTo(content io.Reader, dest_name string) error {

	session, err := self.client.NewSession()
	defer session.Close()

	go func() {
		w, _ := session.StdinPipe()
		defer w.Close()
		//		w.Write(content)

		buf := make([]byte, 4096)
		for {
			// read a chunk
			n, err := content.Read(buf)
			if err != nil && err != io.EOF {
				panic(err)
			}
			if n == 0 {
				break
			}

			// write a chunk
			if _, err := w.Write(buf[:n]); err != nil {
				panic(err)
			}
		}

	}()
	err = session.Run(fmt.Sprintf("/bin/cat >%s", dest_name))
	return err
}

func (self *SSH) CopyFrom(dest_name string, out io.Writer) error {

	session, _ := self.client.NewSession()
	defer session.Close()
	session.Stdout = out
	session.Run(fmt.Sprintf("/bin/cat %s", dest_name))

	return nil

}

func New(host, user, pem_path string) (*SSH, error) {
	ret := new(SSH)
	k := new(keychain)
	k.loadPEM(pem_path)
	config := &ssh.ClientConfig{
		User: user,
		Auth: []ssh.ClientAuth{
			ssh.ClientAuthKeyring(k),
		},
	}
	client, err := ssh.Dial("tcp", host, config)
	if err != nil {
		return nil, err
	}
	ret.client = client

	return ret, err

}
