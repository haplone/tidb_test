package utils

import (
	"io"
	"log"
)

func CheckErr(err error) {
	if err != nil && err != io.EOF {
		log.Printf("err occer: ", err)
	}
}

func CheckSqlErr(err error, sql string) {
	if err != nil && err != io.EOF {
		log.Printf("err occer:  %s \n %s", err, sql)
	}
}
