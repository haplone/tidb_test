package imports

import (
	"flag"
	"fmt"
	"github.com/haplone/tidb_test/file"
	"github.com/haplone/tidb_test/mysql"
	"github.com/haplone/tidb_test/utils"
	"log"
	"strings"
	"sync"
)

func NewImportJob() ImportJob {
	var (
		FoldName = flag.String("fold", "/tmp", "log level: info, debug, warn, error, fatal")
		DbFile   = flag.String("file", "database_list.txt", "")
		Username = flag.String("u", "root", "username for database")
		Pwd      = flag.String("p", "", "pwd for database")
		Host     = flag.String("h", "127.0.0.1", "host for database")
		Port     = flag.String("port", "4000", "port for database")
		LogSize  = flag.Int64("s", 2, "log size for sql")
	)
	flag.Parse()
	return ImportJob{
		LogSize: *LogSize,
		Source: Source{
			FoldName:       *FoldName,
			DbListFileName: *DbFile,
		},
		Dest: Dest{
			Username: *Username,
			Pwd:      *Pwd,
			Host:     *Host,
			Port:     *Port,
		},
	}
}

func (i *ImportJob) Parse() {
	if i.LogSize == 0 {
		i.LogSize = 1000
	}
	dbs := file.GetDbCfgs(i.Source.FoldName, i.Source.DbListFileName)
	for _, c := range dbs {
		c.Parse()
		i.Dbs = append(i.Dbs, c)
	}
}

func (i *ImportJob) Import() {
	for _, db := range i.Dbs {
		cl := mysql.GetMysql(i.Dest.GetConnStr())
		_, err := cl.Exec(db.CreateSql)
		utils.CheckErr(err)
		log.Printf("mysql : %s", i.Dest.GetConnStr())
		log.Printf("create db %s , %d", db.DbName, len(db.Tbls))
		for _, tbl := range db.Tbls {
			//log.Printf("`%s`.`%s` start to  consume and send to mysql", tbl.DbName, tbl.TblName)
			i.Wg.Add(1)
			tbl := tbl
			go i.importTbl(tbl)
		}
	}

	i.Wg.Wait()
}

func (i *ImportJob) importTbl(tbl file.TblCfg) {
	defer func() {
		log.Printf("`%s`.`%s` sqls have consumed and send to mysql", tbl.DbName, tbl.TblName)
		i.Wg.Done()
	}()
	log.Printf("`%s`.`%s` start to  consume and send to mysql", tbl.DbName, tbl.TblName)
	cl := mysql.GetMysql(i.Dest.GetConnStr())
	_, err := cl.Exec(tbl.CreateSql)
	utils.CheckErr(err)

	go tbl.ParseSql()

	var count int64
	for {
		select {
		case sql := <-tbl.SqlCh:
			if strings.EqualFold(sql, "") {
				return
			}
			count += 1
			_, err := cl.Exec(sql)
			if count%i.LogSize == 0 {
				log.Printf("exec %d sqls for `%s`.`%s`,like %s", count, tbl.DbName, tbl.TblName, sql)
			}
			utils.CheckErr(err)
		}
	}
}

type ImportJob struct {
	LogSize int64
	Source  Source
	Dest    Dest
	Dbs     []file.DbCfg
	Wg      *sync.WaitGroup
}

type Source struct {
	FoldName       string
	DbListFileName string
}

type Dest struct {
	Username string
	Pwd      string
	Host     string
	Port     string
}

func (d *Dest) GetConnStr() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/test", d.Username, d.Pwd, d.Host, d.Port)
}
