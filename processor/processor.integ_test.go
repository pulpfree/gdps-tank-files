package processor

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/suite"
)

// TestProcessFile method
func (suite *UnitSuite) TestProcessFile() {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objKey),
	}
	err := suite.p.ProcessFile(input)
	suite.NoError(err)
}

// TestIntegSuite function
func TestIntegSuite(t *testing.T) {
	suite.Run(t, new(UnitSuite))
}
