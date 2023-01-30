.PHONY: test testv test-integration testv-integration

MCLOUD_ENV ?= sandbox
MCLOUD_ENV_FILE=.env.$(MCLOUD_ENV)
-include $(MCLOUD_ENV_FILE)


GO=go

test:
	$(GO) test ./... -short

testv:
	$(GO) test -v ./... -short

test-integration:
	mkdir -p ../coverage-from-docker
	$(GO) test ./test/dax -count 1 -timeout 20m -run TestDAXIntegration/$(RUN)

testv-integration:
	$(GO) test -v ./test/dax -count 1 -timeout 20m -run TestDAXIntegration/$(RUN)


############################### AWS STUFF ###############################

AWS_REGION  ?=
AWS_PROFILE ?=

AWS = aws --profile=$(AWS_PROFILE) --region=$(AWS_REGION)

# After pushing new images, use "make redeploy-ecs" to redeploy all DAX services.
redeploy-ecs: redeploy-svc-mds redeploy-svc-computer redeploy-svc-queryer

redeploy-svc-%:
	$(AWS) ecs update-service --cluster DAX --service $*-$(MCLOUD_ENV)-ecs-service --force-new-deployment --no-cli-pager


# Scale changes the desired count of the computer service. e.g. "make scale N=4"
scale:
	$(AWS) ecs update-service --cluster DAX --service computer-$(MCLOUD_ENV)-ecs-service --desired-count=$(N) --no-cli-pager


I ?= 0
# Get a shell on a running contianer. e.g. "make mds-shell", "make datagen-shell", etc.
%-shell:
	$(eval TASK_ARN := $(shell $(AWS) ecs list-tasks --cluster=DAX --family=$*-family | jq -r .taskArns[$(I)]))
	$(AWS) ecs execute-command --cluster=DAX --task=$(TASK_ARN) --command=/bin/sh --interactive

datagen: task-arn-datagen
	$(eval TASK_ARN := $(shell $(AWS) ecs list-tasks --cluster=DAX --family=$*-family | jq -r .taskArns[$(I)]))
	$(AWS) ecs run-task --cluster=DAX --task-definition=$(TASK_ARN) --cli-input-json=file://./datagen_task_input.json --no-cli-pager --enable-execute-command


####################### docker-compose stuff ##############################3


dc-reset: dc-prereqs dc-down
	rm -rf ./dax-data/{snapshotter,writelogger}/*

dc-build:
	cd .. && $(MAKE) build-for-quick
	docker-compose build

dc-up:
	docker-compose up -d

dc-down:
	docker-compose down

dc-full-reup: dc-reset dc-build dc-up

dc-logs:
	docker-compose logs -f

dc-logs-%:
	docker-compose logs -f $*

dc-prereqs:
	mkdir -p ../.quick

dc-cli:
	featurebase cli --host localhost --port 8080 --org-id=testorg --db-id=testdb

# This is just an example. For it to work, you'll first need to:
#   featurebase cli --host localhost --port 8080 --org-id=testorg --db-id=testdb
#   create table keysidstbl2 (_id string, slice idset);
dc-datagen:
	docker-compose run datagen --end-at=500 --pilosa.batch-size=500 --featurebase.table-name=keysidstbl2

dc-exec-%:
	docker-compose exec $* /bin/sh
