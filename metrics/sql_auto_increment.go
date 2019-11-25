package metrics

import (
	"fmt"

	"github.com/anmolbabu/rds-autoincrement/dao"
)

// AutoIncrementFetcher is a MetricFetcher(defined in metrics/helper.go) that collects auto increment count
func AutoIncrementFetcher() (namespace string, name string, value float64, metaInfo MetricMetaData, err error) {
	// ToDo(@Anmol Babu): Make the msc object available from a object pool

	// Get dao instance
	msc, err := dao.NewClient()
	if err != nil {
		fmt.Printf("%#v\n", err)
		return namespace, name, value, metaInfo, err
	}
	defer msc.Close()

	// Fetch maximum autoincrement count in mysql db
	maxAutoIncrementCount, err := msc.GetAutoIncrementCount()
	if err != nil {
		return namespace, name, value, metaInfo, err
	}

	return "MySql/AutoIncrement", "AutoIncrement", float64(maxAutoIncrementCount), MetricMetaData{Name: "DBName", Value: "postman2019"}, nil
}
