.PHONY: proto run-pilosa run-app

proto: pilosa
	docker run --rm -v $(PWD)/pilosa:/pilosa -w /pilosa/proto/ jfbrandhorst/grpc-web-generators protoc -I/usr/include -I. --plugin=protoc-gen-ts=/usr/local/bin/protoc-gen-ts --js_out=import_style=commonjs,binary:. --ts_out=service=grpc-web:. pilosa.proto
	(echo "/* eslint-disable */"; cat pilosa/proto/pilosa_pb.js; echo "/* eslint-enable */") > src/proto/pilosa_pb.js
	(echo "/* eslint-disable */"; cat pilosa/proto/pilosa_pb_service.js; echo "/* eslint-enable */") > src/proto/pilosa_pb_service.js
	cp pilosa/proto/pilosa_pb.d.ts pilosa/proto/pilosa_pb_service.d.ts src/proto

pilosa:
	git clone git@github.com:molecula/pilosa.git

run-pilosa:
	pilosa server --handler.allowed-origins http://localhost:3000

run-app:
	yarn run start
