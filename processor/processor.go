package processor

import (
	"bytes"
	"encoding/csv"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pulpfree/gdps-tank-files/config"
	"github.com/pulpfree/gdps-tank-files/dynamo"
	log "github.com/sirupsen/logrus"
)

// Processor struct
type Processor struct {
	cfg    *config.Config
	db     *dynamo.Dynamo
	depths map[string]map[string]int
	dwnldr *s3manager.Downloader
	input  *s3.GetObjectInput
	tankID string
}

// New function
func New(cfg *config.Config) (p *Processor, err error) {

	p = new(Processor)
	p.cfg = cfg

	err = p.cfg.Load()
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(p.cfg.AWSRegion),
	})
	if err != nil {
		return nil, err
	}

	p.dwnldr = s3manager.NewDownloader(sess)

	p.db, err = dynamo.NewDB(p.cfg.Dynamo)
	if err != nil {
		return nil, err
	}

	return p, err
}

// ProcessFile method
func (p *Processor) ProcessFile(input *s3.GetObjectInput) (err error) {

	p.input = input
	err = p.extractTankID()
	if err != nil {
		log.Errorf("failed to extract tank id: %s", err)
		p.recordFailure(err.Error())
		return err
	}

	err = p.fetchFile()
	if err != nil {
		log.Errorf("failed to fetch file: %s", err)
		p.recordFailure(err.Error())
		return err
	}

	_, err = p.persistLevels()
	if err != nil {
		log.Errorf("failed to persist levels: %s", err)
		p.recordFailure(err.Error())
		return err
	}

	err = p.cleanUpS3()
	if err != nil {
		log.Errorf("failed to clean up s3: %s", err)
		return err
	}

	log.Infof("Processed tank id: %s", p.tankID)

	return err
}

// fetchFile method
//
// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/s3manager/#Downloader.Download
// https://stackoverflow.com/questions/46019484/buffer-implementing-io-writerat-in-go
func (p *Processor) fetchFile() (err error) {

	buf := aws.NewWriteAtBuffer([]byte{})
	p.dwnldr.Download(buf, p.input)
	r := bytes.NewReader(buf.Bytes())
	reader := csv.NewReader(r)
	p.depths = make(map[string]map[string]int)

	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		lev, _ := strconv.Atoi(line[0])
		lit, _ := strconv.Atoi(line[1])
		p.depths[line[0]] = map[string]int{"level": lev, "litres": lit}
	}

	return err
}

// extractTankID method
//
// Expected filename structure is: tankFile_<tankID>.csv (or tankFile_20.csv)
func (p *Processor) extractTankID() (err error) {

	pcs := strings.Split(*p.input.Key, ".")
	if pcs[1] != "csv" {
		err = errors.New("Invalid file type")
		return err
	}

	p.tankID = strings.Split(pcs[0], "_")[1]
	if len(p.tankID) != 2 {
		return errors.New("TankID must be 2 characters in length")
	}

	return nil
}

// persistLevels method
func (p *Processor) persistLevels() (output *dynamodb.UpdateItemOutput, err error) {
	output, err = p.db.UpdateLevels(p.depths, p.tankID)
	return output, err
}

// recordFailure method
func (p *Processor) recordFailure(errStr string) (output *dynamodb.UpdateItemOutput, err error) {
	output, err = p.db.UpdateWithError(errStr, p.tankID)
	return output, err
}

// cleanUpS3
func (p *Processor) cleanUpS3() (err error) {

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(p.cfg.AWSRegion)},
	)
	if err != nil {
		log.Errorf("failed to create aws session: %s", err.Error())
		return err
	}
	svc := s3.New(sess)

	// t := time.Now()
	ts := strconv.FormatInt(time.Now().Unix(), 10)

	origKey := *p.input.Key
	origBucket := *p.input.Bucket
	source := origBucket + "/" + origKey
	newKey := p.cfg.S3FilePrefix + "/" + ts + "_" + p.tankID + ".csv" // Add timestamp to file

	_, err = svc.CopyObject(&s3.CopyObjectInput{
		Bucket:     aws.String(p.cfg.S3Bucket),
		CopySource: aws.String(source),
		Key:        aws.String(newKey),
	})
	if err != nil {
		log.Errorf("Unable to copy item from bucket %q to bucket %q, %v", source, p.cfg.S3Bucket, err)
		return err
	}

	// Now delete source file
	_, err = svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(origBucket), Key: aws.String(origKey)})
	if err != nil {
		log.Errorf("Unable to delete object %q from bucket %q, %v", origKey, origBucket, err)
		return err
	}

	return err
}
