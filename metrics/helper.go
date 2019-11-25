package metrics

import (
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
)

// MetricMetaData describes the metric
type MetricMetaData struct {
	Name  string
	Value string
}

// MetricFetcher is a type alias for the functions that collects and represents the collected
// metric in a manner that PushMetricToCloudWatch can push the metrics to cloudwatch so that
// a generic framework UpdateMetrics can run all MetricFetchers and push them to cloudwatch using
// PushMetricToCloudWatch
type MetricFetcher func() (string, string, float64, MetricMetaData, error)

// MetricFetchers is the list of metric fetchers that will be parallely executed by the framework
var MetricFetchers = []MetricFetcher{
	AutoIncrementFetcher,
}

// PushMetricToCloudWatch pushes the metric passed to cloudwatch
func PushMetricToCloudWatch(namespace string, name string, value float64, metaInfo MetricMetaData) error {
	// Acquire aws session
	s := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	// create a cloudwatch service handler
	svc := cloudwatch.New(s)

	// push the metric in cloudwatch format
	_, err := svc.PutMetricData(
		&cloudwatch.PutMetricDataInput{
			Namespace: aws.String(namespace),
			MetricData: []*cloudwatch.MetricDatum{
				&cloudwatch.MetricDatum{
					MetricName: aws.String(name),
					Unit:       aws.String("Count"),
					Value:      aws.Float64(value),
					Dimensions: []*cloudwatch.Dimension{
						&cloudwatch.Dimension{
							Name:  aws.String(metaInfo.Name),
							Value: aws.String(metaInfo.Value),
						},
					},
				},
			},
		},
	)
	// handle error
	if err != nil {
		// assert if error is awserror and if so, return a meaningful error
		if awsErr, ok := err.(awserr.Error); ok {
			return fmt.Errorf("failed to push metric %s/%s:%f with meta data %v. Error: %#v", namespace, name, value, metaInfo, awsErr)
		}
		// return error
		return fmt.Errorf("failed to push metric %s/%s:%f with meta data %v. Error: %#v", namespace, name, value, metaInfo, err)
	}

	return nil
}

// UpdateMetrics fetches metrics by executing the fetchers in MetricFetchers and pushes them
// to cloudwatch all in parallel
func UpdateMetrics() error {
	// err is aggregation of errors while running each of the MetricFetchers
	var err error

	// wg is go's way of concurrency control essentially a way to say when the main thread can resume execution
	var wg sync.WaitGroup
	// main thread after spawning go routines should resume only once all go routines are done executing
	wg.Add(len(MetricFetchers))

	// Loop over all the metric fetchers and spawn execution of each MetricFetcher as a go-routine(concurrent execution)
	for _, metricFetcher := range MetricFetchers {
		go func() {
			// execute the metricfetcher to get its corresponding clouwatch form
			namespace, name, value, metaInfo, e := metricFetcher()
			// if err is not nil, accummulate it and error and later instead of failing fast...
			// So, that other metrics can be collected instead of failing completely for failure in collecting one/some
			// of the metrics
			if e != nil {
				if err == nil {
					err = e
					return
				} else {
					err = fmt.Errorf("%s.%s", err, e.Error())
					return
				}
			}

			fmt.Println(namespace, name, value, metaInfo, e)

			// Push the metric just collected
			e = PushMetricToCloudWatch(namespace, name, value, metaInfo)
			// If error accummulate error
			if e != nil {
				// ToDo(@Anmol Babu) handle the error concatenation better
				if err == nil {
					err = e
					return
				} else {
					err = fmt.Errorf("%s.%s", err, e.Error())
					return
				}
			}
			// Signal completion of one routine to be waited on
			wg.Done()
		}()
	}
	// Wait until all go-routines are done executing
	wg.Wait()
	return err
}
