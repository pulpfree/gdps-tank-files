# Steps To Process File

1. Loop through files and call process function on each object
2. Validate file and extract tank id from file name
3. If file is invalid, update GDS_Tank table with INVALID_FILE status
4. Create json object from csv file
5. Update Levels and status in GDS_Tank table
6. Rename file with timestamp and move to s3://ca-gales/tankFiles/