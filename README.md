# rds_metrics

## INTRODUCTION

This repo hosts a golang lambda function that serves as a framework to add any
custom metric which AWS doesn't readily provide. As an example, currently, the
maximum auto increment count as maximum value of auto increment column across
all tables in the mysql DB associated with AWS lambda function in this repo,
is implemented.

The repo has been broadly divideed as:
* main.go : This is the main entry-point to the lambda function
* dao : package hosting code interfacing directly with AWS RDS mysql
* metrics: package hosting code that wraps dao interfaces into meaningful metric
           fetching and logic to push the same to AWS cloudwatch metrics
* vendor: This folder contains the go libraries required to compile the code in
          this repo.
* go.mod: Contains list of dependencies used by go mod dependency management tool
* go.sum: Contains list of dependencies along with exact checksums and version
          binding used by go mod dependency management tool

## Setup Instructions
1. Create AWS account
2. Create a mysql instance on AWS RDS with public accessibility and default
   security group and VPC settings and name and password as `postman2019`.
3. Create a schema by name `postman2019` and add a few tables with records.
   Make sure to randomly include column in a some/all/none of the tables as
   `auto-increment`
4. Create a AWS Lambda function with:
   * Give function name as `postman2019`
   * Select runtime as `Go 1.x`
5. In the created AWS lambda function designer:
   * Trigger as `Cloudwatch Events` with a fixed rate of 5 minutes.
   * Under layers, add `Amazon Cloudwatch logs` and `Amazon Cloudwatch`
6. Setup go, Clone this repo and build the code in this repo using command
   `GOOS=linux go build -o main -mod vendor` 
7. Compress the above(Step 6 above) built binary as main.zip and upload the
   same in the lambda function created in 4 above in the function code section
   and choose runtime as `Go 1.x` and enter `main` as Handler
8. Click `Save` on the top in the lambda function screen
9. From AWS servcies, navigate to `CloudWatch`. Go to `Metrics` and in
   `All Metrics` tab, one can see a new metric called,
   `MySql/AutoIncrement->DBName->postman2019`
