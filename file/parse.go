package file

import (
	"bufio"
	"fmt"
	"github.com/haplone/tidb_test/utils"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

func GetDbCfgs(FoldName, FileName string) []DbCfg {
	var dbs []DbCfg
	dbNames := ParseDbNames(FoldName, FileName)

	for _, n := range dbNames {
		db := NewDbCfg(FoldName, n)
		dbs = append(dbs, db)
	}
	return dbs
}

func ParseDbNames(FoldName, FileName string) []string {
	var names []string

	f, err := os.Open(fmt.Sprintf("%s/%s", FoldName, FileName))
	utils.CheckErr(err)
	buf := bufio.NewReader(f)
	for {
		line, err := buf.ReadString('\n')

		if err != nil {
			if err == io.EOF {
				break
			}
			utils.CheckErr(err)
			break
		}

		if strings.Contains(line, ",") {
			line = strings.Split(line, ",")[0]
		}
		line = strings.TrimSpace(line)
		if len(line) != 0 {
			names = append(names, line)
		}
	}
	return names
}

type DbCfg struct {
	FoldName  string
	DbName    string
	Tbls      []TblCfg
	CreateSql string
}

func NewDbCfg(FoldName, DbName string) DbCfg {
	return DbCfg{
		FoldName: FoldName,
		DbName:   DbName,
	}
}

func (d *DbCfg) AddTbl(t TblCfg) {
	log.Printf("%s add tbl %s", t.DbName, t.TblName)
	d.Tbls = append(d.Tbls, t)
}

func (d *DbCfg) Parse() {
	d.parseCreateDbSql()
	d.parseTblList()
}

func (d *DbCfg) GetDbFold() string {
	return fmt.Sprintf("%s/%s", d.FoldName, d.DbName)
}

func (d *DbCfg) parseCreateDbSql() {
	sqlFile := fmt.Sprintf("%s/data/%s-schema-create.sql", d.GetDbFold(), d.DbName)
	f, err := os.Open(sqlFile)
	utils.CheckErr(err)
	s, err := ioutil.ReadAll(f)
	utils.CheckErr(err)
	d.CreateSql = string(s)
}

func (d *DbCfg) parseTblList() {
	dbFold := d.GetDbFold()
	tblListFile := fmt.Sprintf("%s/%s_tables.list", dbFold, d.DbName)

	f, err := os.Open(tblListFile)
	utils.CheckErr(err)
	buf := bufio.NewReader(f)
	for {
		line, err := buf.ReadString('\n')

		if err != nil {
			if err == io.EOF {
				return
			}
			utils.CheckErr(err)
			return
		}

		line = strings.TrimSpace(line)
		if len(line) != 0 {

			tbl := NewTblCfg(d.FoldName, d.DbName, line)
			tbl.parseCreateTblSql()
			//go tbl.parseSql()
			d.AddTbl(tbl)

			//log.Printf("--: %s", line)

		}

	}
}

type TblCfg struct {
	FoldName  string
	DbName    string
	TblName   string
	CreateSql string
	SqlCh     chan string
}

func NewTblCfg(FoldName, DbName, TblName string) TblCfg {
	return TblCfg{
		FoldName: FoldName,
		DbName:   DbName,
		TblName:  TblName,
		SqlCh:    make(chan string, 1000),
	}
}

func (t *TblCfg) GetDbFold() string {
	return fmt.Sprintf("%s/%s/data", t.FoldName, t.DbName)
}

func (t *TblCfg) parseCreateTblSql() {
	sqlFile := fmt.Sprintf("%s/%s.%s-schema.sql", t.GetDbFold(), t.DbName, t.TblName)
	f, err := os.Open(sqlFile)
	utils.CheckErr(err)
	s, err := ioutil.ReadAll(f)
	utils.CheckErr(err)
	t.CreateSql = string(s)
}

func (t *TblCfg) ParseSql() {
	sqlFile := fmt.Sprintf("%s/", t.GetDbFold())
	dl, err := ioutil.ReadDir(sqlFile)
	utils.CheckErr(err)

	defer func() {
		//log.Printf("read sql done for tbl : %s", t.TblName)
		close(t.SqlCh)
	}()

	for _, d := range dl {
		if strings.HasPrefix(d.Name(), fmt.Sprintf("%s.%s.", t.DbName, t.TblName)) && strings.HasSuffix(d.Name(), ".sql") {
			//log.Printf("-- %s", d.Name())
			sf, err := os.Open(fmt.Sprintf("%s/%s", t.GetDbFold(), d.Name()))
			utils.CheckErr(err)
			buf := bufio.NewReader(sf)

			var sql string
			for {
				line, err := buf.ReadString('\n')
				utils.CheckErr(err)
				//log.Printf("--sql: %s", line)

				if !strings.HasPrefix(line, "INSERT") {
					sql = sql + line
				} else {
					t.SqlCh <- sql
					sql = ""
				}

				if err != nil {
					if err == io.EOF {
						if len(sql) > 0 {
							t.SqlCh <- sql
						}
						log.Printf("`%s`.`%s`[%s] read sql file done(eof)", t.DbName, t.TblName, d.Name())
						break
					}
					utils.CheckErr(err)
					break
				}
			}
		}
	}
}
