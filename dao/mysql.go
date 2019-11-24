package dao

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type FieldInfo struct {
	Field   string
	Type    string
	Null    string
	Key     string
	Default interface{}
	Extra   string
}

type TableInfo []FieldInfo

// ToDo(@Anmol Babu): Make the MySqlClient a object pool
type MySqlClient struct {
	conn *sql.DB
}

func New(userName string, password string, dbLoc string, dbPort int, dbName string) (*MySqlClient, error) {
	conn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", userName, password, dbLoc, dbPort, dbName))
	if err != nil {
		return nil, fmt.Errorf("failed to create a db connection. Error: %#v", err)
	}
	return &MySqlClient{
		conn: conn,
	}, nil
}

func (msc *MySqlClient) ListTables() ([]string, error) {
	tables := []string{}

	res, err := msc.conn.Query("SHOW TABLES")
	if err != nil {
		return tables, fmt.Errorf("failed to list tables. Error: %#v", err)
	}

	for res.Next() {
		var table string
		res.Scan(&table)
		tables = append(tables, table)
	}

	return tables, nil
}

func (msc *MySqlClient) Close() {
	msc.conn.Close()
}

func (msc *MySqlClient) DescribeTable(tableName string) (*TableInfo, error) {
	var ti TableInfo
	res, err := msc.conn.Query(fmt.Sprintf("DESCRIBE %s", tableName))
	if err != nil {
		return nil, fmt.Errorf("failed to describe table %s. Error: %#v", tableName, err)
	}
	for res.Next() {
		var fi FieldInfo
		err := res.Scan(&fi.Field, &fi.Type, &fi.Null, &fi.Key, &fi.Default, &fi.Extra)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal table info %#v. Error: %#v", res, err)
		}
		ti = append(ti, fi)
	}
	return &ti, nil
}

func (msc *MySqlClient) GetMaxColumnValue(column string, tableName string) (int, error) {
	queryStr := fmt.Sprintf("Select MAX(%s) from %s", column, tableName)
	var maxValueStr string

	res, err := msc.conn.Query(queryStr)
	if err != nil {
		return 0, fmt.Errorf("failed to get max value of column %s in table %s. Error: %#v", column, tableName, err)
	}

	for res.Next() {
		res.Scan(&maxValueStr)
	}

	maxValue, err := strconv.Atoi(maxValueStr)
	if err != nil {
		return 0, fmt.Errorf("failed to convert max count %s of field %s in table %s into integer.Error: %#v", maxValueStr, column, tableName, err)
	}

	return maxValue, nil
}

func (msc *MySqlClient) GetAutoIncrementCount() (int, error) {
	var maxAutoIncrementCount int
	var accuErr error

	tables, err := msc.ListTables()
	if err != nil {
		return 0, fmt.Errorf("failed to fetch auto increment count. Error: %#v", err)
	}

	ch := make(chan int)
	errCh := make(chan error)
	defer close(ch)
	defer close(errCh)

	for _, tableName := range tables {
		go func(tableName string) {
			// ToDo(@Anmol Babu): Make this calculation logic run as go routines
			var tableMaxAutoIncrement int
			ti, err := msc.DescribeTable(tableName)
			if err != nil {
				errCh <- err
				return
			}

			for _, columnInfo := range *ti {
				if strings.Contains(columnInfo.Extra, "auto_increment") {
					maxColVal, err := msc.GetMaxColumnValue(columnInfo.Field, tableName)
					if err != nil {
						errCh <- fmt.Errorf("failed to fetch auto increment count for table %s column %s. Error: %#v", tableName, columnInfo.Field, err)
						return
					}

					if tableMaxAutoIncrement < maxColVal {
						tableMaxAutoIncrement = maxColVal
					}

				}
			}
			ch <- tableMaxAutoIncrement
			fmt.Printf("table: %s maxAutoIncrement: %d\n", tableName, tableMaxAutoIncrement)
		}(tableName)
	}

	for ind := 0; ind < len(tables); ind++ {
		select {
		case tableMaxAutoIncrement, ok := <-ch:
			if ok && maxAutoIncrementCount < tableMaxAutoIncrement {
				maxAutoIncrementCount = tableMaxAutoIncrement
			}
		case err, ok := <-errCh:
			if ok {
				if accuErr != nil {
					accuErr = err
				} else {
					accuErr = fmt.Errorf("%s.%s", accuErr.Error(), err.Error())
				}
			}
		}
	}

	return maxAutoIncrementCount, err
}
