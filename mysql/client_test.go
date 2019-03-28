package mysql

import (
	"strings"
	"testing"
)

var DestTestCfg = "root:@tcp(127.0.0.1:4000)/test"

func TestGetMysql(t *testing.T) {
	db := GetMysql(DestTestCfg)
	defer db.Close()

	rows, err := db.Query("show databases")
	defer rows.Close()

	if err != nil {
		t.Error(err)
	}

	columns, err := rows.Columns()
	if err != nil {
		t.Error(err)
	}
	if len(columns) == 1 && !strings.EqualFold(columns[0], "Database") {
		t.Error("show database error")
	}
	for rows.Next() {
		var d string
		err = rows.Scan(&d)
		if err != nil {
			t.Error(err)
		}
	}

}
