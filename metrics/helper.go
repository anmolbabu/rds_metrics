package metrics

import (
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
)

type MetricMetaData struct {
	Name  string
	Value string
}

type MetricFetcher func() (string, string, float64, MetricMetaData, error)

var MetricFetchers = []MetricFetcher{
	AutoIncrementFetcher,
}

func PushMetricToCloudWatch(namespace string, name string, value float64, metaInfo MetricMetaData) error {
	s := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := cloudwatch.New(s)

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
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			return fmt.Errorf("failed to push metric %s/%s:%f with meta data %v. Error: %#v", namespace, name, value, metaInfo, awsErr)
		}
		return fmt.Errorf("failed to push metric %s/%s:%f with meta data %v. Error: %#v", namespace, name, value, metaInfo, err)
	}

	return nil
}

func UpdateMetrics() error {
	var err error

	var wg sync.WaitGroup
	wg.Add(len(MetricFetchers))

	for _, metricFetcher := range MetricFetchers {
		go func() {
			namespace, name, value, metaInfo, e := metricFetcher()
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

			e = PushMetricToCloudWatch(namespace, name, value, metaInfo)
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
			wg.Done()
		}()
	}
	wg.Wait()
	return err
}
