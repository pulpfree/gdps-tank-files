package processor

import (
	"errors"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pulpfree/gdps-tank-files/config"
	"github.com/pulpfree/gdps-tank-files/dynamo"
	"github.com/stretchr/testify/suite"
)

const (
	bucket           = "gdps-tank-files-srcbucket-1b4usmv6ms16v"
	defaultsFilePath = "../config/defaults.yaml"
	objKey           = "tankFile_20.csv"
)

// UnitSuite struct
type UnitSuite struct {
	suite.Suite
	p *Processor
}

// SetupTest method
func (suite *UnitSuite) SetupTest() {
	os.Setenv("Stage", "test")

	cfg := &config.Config{DefaultsFilePath: defaultsFilePath}
	p, err := New(cfg)
	suite.NoError(err)

	suite.p = p
	suite.p.input = &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objKey),
	}
	suite.IsType(new(Processor), suite.p)
}

// TestConfig method
func (suite *UnitSuite) TestConfig() {
	suite.IsType(new(config.Config), suite.p.cfg)
	suite.NotEqual("", suite.p.cfg.AWSRegion, "Expected AWSRegion to be populated")
}

// TestExtractTankID method
func (suite *UnitSuite) TestExtractTankID() {
	err := suite.p.extractTankID()
	suite.NoError(err)
	suite.Equal("20", suite.p.tankID, "Expect tankID to match")

	suite.p.input.Key = aws.String("phonyname.jpg")
	err = suite.p.extractTankID()
	suite.Error(err)

	suite.p.input.Key = aws.String("badid_123.csv")
	err = suite.p.extractTankID()
	suite.Error(err)
}

// TestFetchFile method
func (suite *UnitSuite) TestFetchFile() {
	err := suite.p.fetchFile()
	suite.NoError(err)
}

// TestPersistLevels method
func (suite *UnitSuite) TestPersistLevels() {
	suite.p.input = &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objKey),
	}
	_ = suite.p.fetchFile()
	_ = suite.p.extractTankID()

	_, err := suite.p.persistLevels()
	suite.NoError(err)
}

// TestRecordFailure method
func (suite *UnitSuite) TestRecordFailure() {
	suite.p.input = &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objKey),
	}
	// suite.p.input = input
	_ = suite.p.fetchFile()
	_ = suite.p.extractTankID()

	e := errors.New("Phony error for testing")
	output, err := suite.p.recordFailure(e.Error())
	suite.NoError(err)
	suite.Equal(*output.Attributes["Status"].S, dynamo.ERROR)
}

// TestUnitSuite function
func TestUnitSuite(t *testing.T) {
	suite.Run(t, new(UnitSuite))
}
