package file

import (
	"log"
	"strings"
	"testing"
)

func TestGetDbCfgs(t *testing.T) {
	dbs := GetDbCfgs("../test_file", "database_list.txt")
	if len(dbs) != 1 {
		t.Errorf("parse database list error")
	}
	for _, db := range dbs {
		db.parseTblList()
		for _, tbl := range db.Tbls {
			tt := tbl
			go tt.ParseSql()
			consumeSql(tt)
		}
	}
	//t.Error("==")
}

func consumeSql(tbl TblCfg) {
	log.Printf("tbl[`%s`.`%s`]: start", tbl.DbName, tbl.TblName)
	for {
		select {
		case sql := <-tbl.SqlCh:
			if strings.EqualFold(sql, "") {
				log.Printf("tbl[`%s`.`%s`]: over", tbl.DbName, tbl.TblName)
				return
			}
			log.Printf("tbl[`%s`.`%s`]: %s", tbl.DbName, tbl.TblName, sql)
			//default:
			//	return
		}
	}
	return
}

func TestParseDbNames(t *testing.T) {
	dbs := ParseDbNames("../test_file", "database_list.txt")
	if len(dbs) != 1 {
		t.Errorf("parse database list error")
	}
}

func NewDb() DbCfg {
	return NewDbCfg("../test_file", "db1")
}

func NewTbl() TblCfg {
	return NewTblCfg("../test_file", "db1", "tbl3")
}

func TestDbCfg_parseTblList(t *testing.T) {
	db := NewDb()
	db.parseTblList()
	if len(db.Tbls) != 3 {
		t.Errorf("parse tbls error")
	}
	//for _, tbl := range db.Tbls {
	//	select {
	//	case <-tbl.SqlCh:
	//	}
	//}
}

func TestDbCfg_parseCreateDbSql(t *testing.T) {
	db := NewDb()
	db.parseCreateDbSql()
	if !strings.EqualFold(db.CreateSql, "create database db1;") {
		t.Errorf("parse db create sql error")
	}
}

func TestTBlCfg_parseCreateTblSql(t *testing.T) {
	tbl := NewTbl()
	tbl.parseCreateTblSql()
	if !strings.EqualFold(tbl.CreateSql, "create table db1.tbl3(id int,name varchar(128));") {
		t.Errorf("parse tbl create sql error")
	}
}
