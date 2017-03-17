package ctl

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

//RunMigrationCommand represents a command for executing a migration plan
type RunMigrationCommand struct {
	//Path for migration plan json file
	Plan             string
	User             string
	PemFile          string
	connection_cache map[string]struct {
		r FileReader
		w FileWriter
	}

	sshconfig *ssh.ClientConfig

	// Standard input/output
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// Returns a new instance of RunMigrationCommand.
func NewRunMigrationCommand(stdin io.Reader, stdout, stderr io.Writer) *RunMigrationCommand {
	return &RunMigrationCommand{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr}
}

func (cmd *RunMigrationCommand) Run(ctx context.Context) error {
	//runMigration(plan)
	return nil
}

func getConfig(user, pem_file_name string) (*ssh.ClientConfig, error) {
	pemBytes, err := ioutil.ReadFile(pem_file_name)
	if err != nil {
		log.Fatal(err)
	}
	signer, err := ssh.ParsePrivateKey(pemBytes)
	if err != nil {
		log.Fatalf("parse key failed:%v", err)
	}
	config := &ssh.ClientConfig{
		User:    user,
		Auth:    []ssh.AuthMethod{ssh.PublicKeys(signer)},
		Timeout: 30 * time.Second,
	}
	return config, err

}

type WriterToCloser interface {
	io.WriterTo
	Close() error
}

type nopWriterToCloser struct {
	io.WriterTo
}

func (nopWriterToCloser) Close() error { return nil }

// NopCloser returns a ReadCloser with a no-op Close method wrapping
// the provided Reader r.
func NopWriterToCloser(w io.WriterTo) WriterToCloser {
	return nopWriterToCloser{w}
}

type sftpreader struct {
	sftp *sftp.Client
}

func (s *sftpreader) Open(path string) (WriterToCloser, error) {
	x, err := s.sftp.Open(path)
	return x, err
}

type sftpwriter struct {
	sftp *sftp.Client
}

func (s *sftpwriter) Create(path string) (io.WriteCloser, error) {
	x, err := s.sftp.Create(path)
	return x, err
}

func (s *sftpwriter) EnsureDirs(path string) error {
	//x, err := s.sftp.Dir()
	return nil
}

type FileWriter interface {
	Create(path string) (io.WriteCloser, error)
	EnsureDirs(full_path string) error
}

type FileReader interface {
	Open(path string) (WriterToCloser, error)
}

type localwriter struct{}

func (lw *localwriter) Create(path string) (io.WriteCloser, error) {
	return os.Create(path)
}

func (s *localwriter) EnsureDirs(path string) error {
	//x, err := s.sftp.Dir()
	return nil
}

type localreader struct{}

func (lw *localreader) Open(path string) (WriterToCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return NopWriterToCloser(bufio.NewReader(f)), nil
}

func ConnectLocalRemote(Host string, config *ssh.ClientConfig) (FileReader, FileWriter, error) {
	conn, err := ssh.Dial("tcp", Host, config)
	if err != nil {
		return nil, nil, errors.New("Failed to connect file source host: " + Host + " " + err.Error())
	}
	sftp, err := sftp.NewClient(conn)
	if err != nil {
		return nil, nil, errors.New("Failed to setup sftp : " + Host + " " + err.Error())
	}
	return &sftpreader{sftp}, &localwriter{}, nil
}

func ConnectRemoteLocal(Host string, config *ssh.ClientConfig) (FileReader, FileWriter, error) {
	conn, err := ssh.Dial("tcp", Host, config)
	if err != nil {
		return nil, nil, errors.New("Failed to connect file source host: " + Host + " " + err.Error())
	}
	sftp, err := sftp.NewClient(conn)
	if err != nil {
		return nil, nil, errors.New("Failed to setup sftp : " + Host + " " + err.Error())
	}
	return &localreader{}, &sftpwriter{sftp}, nil
}

func ConnectJumpRemote(srcHost, destHost string, config *ssh.ClientConfig) (FileReader, FileWriter, error) {
	jump, err := ssh.Dial("tcp", srcHost, config)
	if err != nil {
		return nil, nil, errors.New("Failed to connect file source host: " + srcHost + " " + err.Error())
	}

	jmpcon, err := jump.Dial("tcp", destHost)
	if err != nil {
		return nil, nil, errors.New("Failed to connect file dest host: " + srcHost + " " + err.Error())
	}

	dest_con, chans, reqs, err := ssh.NewClientConn(jmpcon, destHost, config)
	if err != nil {
		return nil, nil, errors.New("Failed to handshake : " + srcHost + " " + destHost + " " + err.Error())
	}
	dest_ssh := ssh.NewClient(dest_con, chans, reqs)

	dest_ftp, err := sftp.NewClient(dest_ssh)
	if err != nil {
		return nil, nil, errors.New("Failed to setup sftp : " + destHost + " " + err.Error())
	}

	src_ftp, err := sftp.NewClient(jump)
	if err != nil {
		return nil, nil, errors.New("Failed to setup sftp : " + srcHost + " " + err.Error())
	}
	return &sftpreader{src_ftp}, &sftpwriter{dest_ftp}, nil
}

///////////////

func CopyFiles(reader FileReader, writer FileWriter, src_path, dest_path string) error {
	dest_file, err := writer.Create(dest_path)
	if err != nil {
		return errors.New("Failed to create: " + dest_path + " " + err.Error())
	}

	src_file, err := reader.Open(src_path)
	if err != nil {
		return errors.New("Failed to open:" + src_path + " " + err.Error())
	}

	_, err = src_file.WriteTo(dest_file)
	if err != nil {
		return errors.New("Failed to write file: " + dest_path + " " + err.Error())
	}
	src_file.Close()
	dest_file.Close()
	return nil
}

func (cmd *RunMigrationCommand) getCacheConnection(key string) (struct {
	r FileReader
	w FileWriter
}, bool) {
	e, found := cmd.connection_cache[key]
	return e, !found
}
func (cmd *RunMigrationCommand) rememberConnection(
	key string,
	r FileReader,
	w FileWriter) {

	i := struct {
		r FileReader
		w FileWriter
	}{r, w}
	cmd.connection_cache[key] = i
}

//get or create a cached connection
func (cmd *RunMigrationCommand) getPair(src, dest string) (FileReader, FileWriter, error) {
	var buffer bytes.Buffer
	buffer.WriteString(src)
	buffer.WriteString("|")
	buffer.WriteString(dest)
	key := buffer.String()

	pair, notcached := cmd.getCacheConnection(key)
	if notcached {
		r, w, e := ConnectJumpRemote(src, dest, cmd.sshconfig)
		if e != nil {
			return nil, nil, e
		}
		cmd.rememberConnection(key, r, w)
		return r, w, nil
	}
	//remote/remote
	return pair.r, pair.w, nil
}
func (cmd *RunMigrationCommand) runMigration(plan Plan) {
	for srchost, v := range plan.Actions {
		remfiles := make(map[string]struct{})
		for _, part := range v {
			reader, writer, _ := cmd.getPair(srchost, part.Host)
			destBase := plan.SrcDataDir + "/" + part.Db + "/" + part.Frame
			writer.EnsureDirs(destBase)
			srcBase := plan.DestDataDir + "/" + part.Db + "/" + part.Frame + "/" + strconv.FormatUint(part.Slice, 10)
			destBase = destBase + "/" + strconv.FormatUint(part.Slice, 10)
			CopyFiles(reader, writer, srcBase, destBase)
			//save for removal
			remfiles[srcBase] = struct{}{}

			srcBase += ".cache"
			destBase += ".cache"
			CopyFiles(reader, writer, srcBase, destBase)
			//save for removal
			remfiles[srcBase] = struct{}{}
		}

		// for f, _ := range remfiles {
		// 	fmt.Printf("# remove local file %s \n", f)
		// }

	}
}

// func main() {
// 	src := "172.31.30.226:22"
// 	dest := "172.31.17.196:22"
// 	config, _ := getConfig("ubuntu", "/home/ubuntu/.ssh/pilosa.pem")
// 	r, w, err := ConnectJumpRemote(src, dest, config)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	CopyFiles(r, w, "testfile", "testfile")
// }
