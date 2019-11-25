package main

import (
	"github.com/anmolbabu/rds-autoincrement/metrics"
	"github.com/aws/aws-lambda-go/lambda"
)

func main() {
	// Start the lambda entry point function
	lambda.Start(metrics.UpdateMetrics)
}
