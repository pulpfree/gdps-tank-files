package main

import (
	"context"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pulpfree/gdps-tank-files/config"
	"github.com/pulpfree/gdps-tank-files/processor"
	log "github.com/sirupsen/logrus"
)

var cfg *config.Config

const defaultsFilePath = "./defaults.yaml"

func init() {
	cfg = &config.Config{
		DefaultsFilePath: defaultsFilePath,
	}
	err := cfg.Load()
	if err != nil {
		log.Fatal(err)
		return
	}
}

func handleRequest(ctx context.Context, s3Event events.S3Event) {

	p, err := processor.New(cfg)
	if err != nil {
		log.Fatal(err)
		return
	}

	for _, record := range s3Event.Records {

		input := &s3.GetObjectInput{
			Bucket: aws.String(record.S3.Bucket.Name),
			Key:    aws.String(record.S3.Object.Key),
		}
		err = p.ProcessFile(input)
		if err != nil {
			log.Errorf("failed to process file: %s", err)
			return
		}
	}

}

func main() {
	lambda.Start(handleRequest)
}
