package main

import (
	"github.com/anmolbabu/rds-autoincrement/metrics"
	"github.com/aws/aws-lambda-go/lambda"
)

func main() {
	lambda.Start(metrics.UpdateMetrics)
}
