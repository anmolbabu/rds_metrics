package metrics

import (
	"fmt"

	"github.com/anmolbabu/rds-autoincrement/dao"
)

func AutoIncrementFetcher() (namespace string, name string, value float64, metaInfo MetricMetaData, err error) {
	// ToDo(@Anmol Babu): Make the msc object available from a object pool
	msc, err := dao.New("postman2019", "postman2019", "postman2019.c6oscqwrvlor.us-east-2.rds.amazonaws.com", 3306, "postman2019")
	if err != nil {
		fmt.Printf("%#v\n", err)
		return namespace, name, value, metaInfo, err
	}
	defer msc.Close()

	maxAutoIncrementCount, err := msc.GetAutoIncrementCount()
	if err != nil {
		return namespace, name, value, metaInfo, err
	}

	return "MySql/AutoIncrement", "AutoIncrement", float64(maxAutoIncrementCount), MetricMetaData{Name: "DBName", Value: "postman2019"}, nil
}
