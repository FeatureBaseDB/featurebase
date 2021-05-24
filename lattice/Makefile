.PHONY: proto run-pilosa run-app

proto: pilosa
	docker run --rm -v $(PWD)/..:/pilosa -w /pilosa/proto/ jfbrandhorst/grpc-web-generators protoc -I/usr/include -I. --plugin=protoc-gen-ts=/usr/local/bin/protoc-gen-ts --js_out=import_style=commonjs,binary:. --ts_out=service=grpc-web:. pilosa.proto
	(echo "/* eslint-disable */"; cat ../proto/pilosa_pb.js; echo "/* eslint-enable */") > src/proto/pilosa_pb.js
	(echo "/* eslint-disable */"; cat ../proto/pilosa_pb_service.js; echo "/* eslint-enable */") > src/proto/pilosa_pb_service.js
	cp ../proto/pilosa_pb.d.ts ../proto/pilosa_pb_service.d.ts src/proto

pilosa:
	cd .. && git checkout master && git pull

run-pilosa:
	pilosa server --handler.allowed-origins http://localhost:3000

run-app:
	yarn run start
