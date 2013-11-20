PWD=`pwd`
NAME=${1:-pilosa}
export GOPATH=$PWD/$NAME
read -p "This will create a new pilosa environment at $GOPATH (DELETING IF NECESSARY!)! Continue? (y/n) " -n 1 -r
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
	echo
	echo "Aborting!"
	exit 1
fi
echo Building environment \"$NAME\" at $GOPATH...

rm -rf $GOPATH
mkdir -p $GOPATH/src
git clone git@ops:nuevo-pilosa.git $GOPATH/src/pilosa
cd $GOPATH
ln -s src/pilosa pilosa

go get github.com/vube/depman
./bin/depman -path=pilosa install

hg clone -u release https://code.google.com/p/gocircuit src/circuit_src
cp -r src/circuit_src/src/circuit src

export GOCIRCUIT=$GOPATH/src/circuit_src
export ZKINCLUDE=$GOCIRCUIT/misc/starter-kit-darwin/zookeeper/include
export ZKLIB=$GOCIRCUIT/misc/starter-kit-darwin/zookeeper/lib

export CGO_CFLAGS="-I$ZKINCLUDE"
export CGO_LDFLAGS="$ZKLIB/libzookeeper_mt.a"

go install circuit/cmd/...
go install pilosa/...

cat << EOF > app.config
{
	"Zookeeper": {
		"Workers": ["localhost:2181"],
		"Dir":     "/circuit/pilosa"
	},
	"Deploy": {
		"Dir":    "$GOPATH",
		"Worker": "pilosa-worker"
	},
	"Build": {
		"Host":             "localhost",
		"Jail":             "{{ \`HOME\` | env }}/.pilosa/build",
		"ZookeeperInclude": "$ZKINCLUDE",
		"ZookeeperLib":     "$ZKLIB",
		"Tool":             "$GOPATH/bin/4build",
		"PrefixPath":       "",

		"AppRepo":          "{rsync}$GOPATH/src/pilosa",
		"AppSrc":           "$GOPATH/src/pilosa",

		"GoRepo":           "{rsync}{{ \`GOROOT\` | env }}",
		"RebuildGo":        false,

		"CircuitRepo":      "{rsync}$GOCIRCUIT",
		"CircuitSrc":       "/",

		"WorkerPkg":        "pilosa/cmd/pilosa-cruncher",
		"CmdPkgs":          ["pilosa/cmd/pilosa-spawn"],
		"ShipDir":          "{{ \`HOME\` | env }}/.pilosa/ship"
	}
}
EOF

cat << EOF > activate
export CIR=app.config
export GOPATH=$GOPATH
export PATH=$GOPATH/bin:\$PATH
EOF

echo Done building environment. Run \". activate\" to set environment variables.
