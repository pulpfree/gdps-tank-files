include .env

default: build

deploy: build awsPackage awsDeploy

clean:
	@rm -rf dist
	@mkdir -p dist

build: clean
	@for dir in `ls handler`; do \
		GOOS=linux go build -o dist/$$dir github.com/pulpfree/gdps-tank-files/handler/$$dir; \
	done
	@cp ./config/defaults.yaml dist/

validate:
	sam validate

run: build
	sam local start-api -n env.json

run-tf:
	sam local invoke --env-vars env.json "TankFilesFunc"

create-event:
	sam local generate-event s3 --bucket s3://gdps-tank-files-srcbucket-1b4usmv6ms16v --key env.json > event_file.json

awsPackage:
	aws cloudformation package \
   --template-file template.yaml \
   --output-template-file packaged-tpl.yaml \
   --s3-bucket $(AWS_BUCKET_NAME) \
   --s3-prefix $(AWS_BUCKET_PREFIX) \
   --profile $(AWS_PROFILE)

awsDeploy:
	aws cloudformation deploy \
   --template-file packaged-tpl.yaml \
   --stack-name $(AWS_STACK_NAME) \
   --capabilities CAPABILITY_IAM \
   --profile $(AWS_PROFILE)

describe:
	@aws cloudformation describe-stacks \
		--region $(AWS_REGION) \
		--stack-name $(AWS_STACK_NAME)