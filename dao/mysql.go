package dao

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// FieldInfo is a struct that represents the description of each field in output of mysql table describe command
type FieldInfo struct {
	Field   string
	Type    string
	Null    string
	Key     string
	Default interface{}
	Extra   string
}

// TableInfo is the collection of FieldInfos essentially type representing output of table describe mysql command
type TableInfo []FieldInfo

// MySqlClient holds the mysql connection object
// ToDo(@Anmol Babu): Make the MySqlClient a object pool
type MySqlClient struct {
	conn *sql.DB
}

// New returns a new instance of MySqlClient
func New(userName string, password string, dbLoc string, dbPort int, dbName string) (*MySqlClient, error) {
	conn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", userName, password, dbLoc, dbPort, dbName))
	if err != nil {
		return nil, fmt.Errorf("failed to create a db connection. Error: %#v", err)
	}
	return &MySqlClient{
		conn: conn,
	}, nil
}

func NewClient() (*MySqlClient, error) {
	port, err := strconv.Atoi(os.Getenv("DB_PORT"))
	if err != nil {
		if os.Getenv("DB_PORT") == "" {
			return nil, fmt.Errorf("DB_PORT a mandatory parameter, is not injected as env variable")
		}
		return nil, err
	}
	return New(
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_ENDPOINT"),
		port,
		os.Getenv("DB_NAME"),
	)
}

// ListTables lists the mysql tables in the db in the MySqlClient
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

// Close closes the mysql connection
func (msc *MySqlClient) Close() {
	msc.conn.Close()
}

// DescribeTable describes the table passed and returns the description of the table and errors if any
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

// GetMaxColumnValue returns the maximum value in a given column in the given table and errors if any
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

// GetAutoIncrementCount returns the maximum of autoincrement counts across all tables and errors if any
func (msc *MySqlClient) GetAutoIncrementCount() (int, error) {
	var maxAutoIncrementCount int
	// Errors if any accummulated so that the errors are not neglected and at the same time error for one/some
	// tables shouldnot block metric calculation
	var accuErr error

	// List tables
	tables, err := msc.ListTables()
	if err != nil {
		return 0, fmt.Errorf("failed to fetch auto increment count. Error: %#v", err)
	}

	ch := make(chan int)
	errCh := make(chan error)
	defer close(ch)
	defer close(errCh)

	// For each table, spawn a go routine, that calculates a local maximum auto-increment count within the table
	for _, tableName := range tables {
		go func(tableName string) {
			// ToDo(@Anmol Babu): Make this calculation logic run as go routines
			var tableMaxAutoIncrement int

			// Describe the table to see fi there's a column of type auto increment
			ti, err := msc.DescribeTable(tableName)
			if err != nil {
				errCh <- err
				return
			}

			// Ideally in a mysql table, there can only be 1 column that is auto-increment type, however, this block
			// takes care of a impossibility of more than 1 auto-increment column
			for _, columnInfo := range *ti {
				// A column is auto increment type if, the FieldInfo in the TableInfo fetched from describe table command
				// contains auto_increment
				if strings.Contains(columnInfo.Extra, "auto_increment") {
					// Get maximum value of auto increment enabled column
					maxColVal, err := msc.GetMaxColumnValue(columnInfo.Field, tableName)
					// if error, pass it to error channel so that, it is accumulated and returned as error at the end without
					// breaking the logic for other tables and columns
					if err != nil {
						errCh <- fmt.Errorf("failed to fetch auto increment count for table %s column %s. Error: %#v", tableName, columnInfo.Field, err)
						return
					}

					// Reset tableMaxAutoIncrement counter if there's a column with greater auto_increment value than the previously seen
					// columns in current table... This is a remote possibility as mysql restricts atmost 1 auto-increment column. However the
					// logic that works for many columns certainly works for 1
					if tableMaxAutoIncrement < maxColVal {
						tableMaxAutoIncrement = maxColVal
					}

				}
			}
			// on success fetch of maximum auto increment count for current table, push it to channel so that the other end
			// of channel can find max across tables
			ch <- tableMaxAutoIncrement
			fmt.Printf("table: %s maxAutoIncrement: %d\n", tableName, tableMaxAutoIncrement)
		}(tableName)
	}

	for ind := 0; ind < len(tables); ind++ {
		select {
		case tableMaxAutoIncrement, ok := <-ch:
			// If there is a message on the channel ch, it means one of the table routines returned a local maximum
			// So, calculate global across the tables in db, max auto-increment count
			if ok && maxAutoIncrementCount < tableMaxAutoIncrement {
				maxAutoIncrementCount = tableMaxAutoIncrement
			}
		case err, ok := <-errCh:
			// If there is a message on errCh, it means one of the routines calculating the local maximum auto_increment
			// in one of the tables, returned error.Accumulate this error for final return so that, error in one place doesn't
			// break the whole calculation
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
