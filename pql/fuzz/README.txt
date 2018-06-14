See https://github.com/dvyukov/go-fuzz


Quickstart:

go get -u github.com/dvyukov/go-fuzz/...
go-fuzz-build github.com/pilosa/pilosa/pql
go-fuzz -bin=./pql-fuzz.zip -workdir=$GOPATH/src/github.com/pilosa/pilosa/pql/fuzz
