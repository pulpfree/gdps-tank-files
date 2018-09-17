package dynamo

import (
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/pulpfree/gdps-tank-files/config"
	log "github.com/sirupsen/logrus"
)

// Dynamo struct
type Dynamo struct {
	db *dynamodb.DynamoDB
}

// Tank & Status constants
const (
	tankTable  = "GDS_Tank"
	OK         = "OK"
	ERROR      = "ERROR"
	PENDING    = "PENDING"
	PROCESSING = "PROCESSING"
)

// NewDB connection function
func NewDB(cfg *config.Dynamo) (*Dynamo, error) {

	var err error

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(cfg.Region),
	})
	if err != nil {
		return nil, err
	}
	svc := dynamodb.New(sess)

	return &Dynamo{
		db: svc,
	}, err
}

// UpdateLevels method
func (d *Dynamo) UpdateLevels(depths map[string]map[string]int, tankID string) (output *dynamodb.UpdateItemOutput, err error) {

	t := time.Now()
	levels, err := dynamodbattribute.MarshalMap(depths)
	if err != nil {
		return output, err
	}

	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(tankTable),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":errorMsg": {
				S: aws.String(" "),
			},
			":levels": {
				M: levels,
			},
			":status": {
				S: aws.String(OK),
			},
			":updatedAt": {
				N: aws.String(strconv.FormatInt(t.Unix(), 10)),
			},
		},
		ExpressionAttributeNames: map[string]*string{
			"#status": aws.String("Status"),
		},
		Key: map[string]*dynamodb.AttributeValue{
			"ID": {
				S: aws.String(tankID),
			},
		},
		ReturnValues:     aws.String("UPDATED_NEW"),
		UpdateExpression: aws.String("set ErrorMsg = :errorMsg, Levels = :levels, #status = :status, UpdatedAt = :updatedAt"),
	}

	output, err = d.db.UpdateItem(input)
	if err != nil {
		log.Errorf("Failed to update record: %s", err.Error())
		return output, err
	}

	return output, err
}

// UpdateWithError method
func (d *Dynamo) UpdateWithError(errMsg, tankID string) (output *dynamodb.UpdateItemOutput, err error) {

	t := time.Now()

	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(tankTable),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":errorMsg": {
				S: aws.String(errMsg),
			},
			":status": {
				S: aws.String(ERROR),
			},
			":updatedAt": {
				N: aws.String(strconv.FormatInt(t.Unix(), 10)),
			},
		},
		ExpressionAttributeNames: map[string]*string{
			"#status": aws.String("Status"),
		},
		Key: map[string]*dynamodb.AttributeValue{
			"ID": {
				S: aws.String(tankID),
			},
		},
		ReturnValues:     aws.String("UPDATED_NEW"),
		UpdateExpression: aws.String("set ErrorMsg = :errorMsg, #status = :status, UpdatedAt = :updatedAt"),
	}

	output, err = d.db.UpdateItem(input)
	if err != nil {
		log.Errorf("Failed to update record: %s", err.Error())
		return output, err
	}

	return output, err
}
