package mysql

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"log"
)

func GetMysql(cfg string) *sql.DB {
	db, err := sql.Open("mysql", cfg)
	if err != nil {
		log.Fatal("can not connect tidb ")
	}

	return db
}
