package file

import (
	"strings"
	"testing"
)

func TestParseDbNames(t *testing.T) {
	dbs := ParseDbNames("../test_file", "database_list.txt")
	if len(dbs) != 2 {
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
	for _, tbl := range db.Tbls {
		select {
		case <-tbl.SqlCh:
		}
	}
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
