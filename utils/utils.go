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
