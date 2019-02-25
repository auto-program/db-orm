package orm

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	"github.com/opentracing/opentracing-go"
	ottag "github.com/opentracing/opentracing-go/ext"
	otlog "github.com/opentracing/opentracing-go/log"
)

type DB interface {
	Query(sql string, args ...interface{}) (*sql.Rows, error)
	Exec(sql string, args ...interface{}) (sql.Result, error)
	SetError(err error)
}

type TracedDB struct {
	DB
	ctx context.Context
}

type DBStore struct {
	*sql.DB
	debug   bool
	slowlog time.Duration
}

type DBTx struct {
	tx           *sql.Tx
	debug        bool
	slowlog      time.Duration
	err          error
	rowsAffected int64
	Ctx          context.Context
}

func OpenTrace(ctx context.Context, db DB) DB {
	return &TracedDB{
		DB:  db,
		ctx: ctx,
	}
}

type DBQuerySession struct {
	*DBStore
	Ctx context.Context
}

func NewDBStore(driver, host string, port int, database, username, password string) (*DBStore, error) {
	var dsn string
	switch strings.ToLower(driver) {
	case "mysql":
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&autocommit=true&parseTime=True",
			username,
			password,
			host,
			port,
			database)
	case "mssql":
		dsn = fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s",
			host, username, password, port, database)
	default:
		return nil, fmt.Errorf("unsupport db driver: %s", driver)
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	return &DBStore{db, false, time.Duration(0)}, nil
}

func NewDBStoreCharset(driver, host string, port int, database, username, password, charset string) (*DBStore, error) {
	var dsn string
	switch strings.ToLower(driver) {
	case "mysql":
		if charset == "" {
			charset = "utf8"
		}
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&autocommit=true&parseTime=True",
			username,
			password,
			host,
			port,
			database,
			charset)
	case "mssql":
		dsn = fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s",
			host, username, password, port, database)
	default:
		return nil, fmt.Errorf("unsupport db driver: %s", driver)
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	return &DBStore{db, false, time.Duration(0)}, nil
}

func (store *DBStore) Debug(b bool) {
	store.debug = b
}

func (store *DBStore) SlowLog(duration time.Duration) {
	store.slowlog = duration
}

func (store *DBStore) Query(sql string, args ...interface{}) (*sql.Rows, error) {
	t1 := time.Now()
	if store.slowlog > 0 {
		defer func(t time.Time) {
			span := time.Now().Sub(t1)
			if span > store.slowlog {
				log.Println("SLOW: ", span.String(), sql, args)
			}
		}(t1)
	}
	if store.debug {
		log.Println("DEBUG: ", sql, args)
	}
	return store.DB.Query(sql, args...)
}

func (store *DBStore) Exec(sql string, args ...interface{}) (sql.Result, error) {
	t1 := time.Now()
	if store.slowlog > 0 {
		defer func(t time.Time) {
			span := time.Now().Sub(t1)
			if span > store.slowlog {
				log.Println("SLOW: ", span.String(), sql, args)
			}
		}(t1)
	}
	if store.debug {
		log.Println("DEBUG: ", sql, args)
	}
	return store.DB.Exec(sql, args...)
}

func (store *DBStore) SetError(err error) {}

func (store *DBStore) Close() error {
	if err := store.DB.Close(); err != nil {
		return err
	}
	store.DB = nil
	return nil
}

func (store *DBStore) BeginTx(ctx context.Context) (*DBTx, error) {
	tx, err := store.Begin()
	if err != nil {
		return nil, err
	}

	return &DBTx{
		tx:      tx,
		debug:   store.debug,
		slowlog: store.slowlog,
		Ctx:     ctx,
	}, nil
}

func (tx *DBTx) Close() error {
	if tx.Ctx != nil {
		select {
		case <-tx.Ctx.Done():
			tx.err = tx.Ctx.Err()
		default:
		}
	}
	if tx.err != nil {
		return tx.tx.Rollback()
	}
	return tx.tx.Commit()
}

func (tx *DBTx) Query(sql string, args ...interface{}) (result *sql.Rows, err error) {
	t1 := time.Now()
	if tx.slowlog > 0 {
		defer func(t time.Time) {
			span := time.Now().Sub(t1)
			if span > tx.slowlog {
				log.Println("SLOW: ", span.String(), sql, args)
			}
		}(t1)
	}
	if tx.debug {
		log.Println("DEBUG: ", sql, args)
	}

	if tx.Ctx != nil {
		result, err = tx.tx.QueryContext(tx.Ctx, sql, args...)
		tx.err = err
		return
	}
	result, err = tx.tx.Query(sql, args...)
	tx.err = err
	return result, tx.err
}

func (tx *DBTx) Exec(sql string, args ...interface{}) (result sql.Result, err error) {
	t1 := time.Now()
	if tx.slowlog > 0 {
		defer func(t time.Time) {
			span := time.Now().Sub(t1)
			if span > tx.slowlog {
				log.Println("SLOW: ", span.String(), sql, args)
			}
		}(t1)
	}
	if tx.debug {
		log.Println("DEBUG: ", sql, args)
	}
	if tx.Ctx != nil {
		result, err = tx.tx.ExecContext(tx.Ctx, sql, args...)
		tx.err = err
		return
	}
	result, err = tx.tx.Exec(sql, args...)
	tx.err = err
	return
}

func (tx *DBTx) SetError(err error) {
	tx.err = err
}

func (tx *DBTx) SetContext(ctx context.Context) {
	tx.Ctx = ctx
}

func (db *TracedDB) Query(sql string, args ...interface{}) (*sql.Rows, error) {
	span, _ := opentracing.StartSpanFromContext(db.ctx, "DB Query")
	span.LogFields(otlog.String("sql.query", fmt.Sprint(sql, ",", args)))
	defer span.Finish()
	rows, err := db.DB.Query(sql, args...)
	if err != nil {
		logErrorToSpan(span, err)
	}
	return rows, err
}

func (db *TracedDB) Exec(sql string, args ...interface{}) (sql.Result, error) {
	span, _ := opentracing.StartSpanFromContext(db.ctx, "DB Exec")
	span.LogFields(otlog.String("sql.query", fmt.Sprint(sql, ",", args)))
	defer span.Finish()
	result, err := db.DB.Exec(sql, args...)
	if err != nil {
		logErrorToSpan(span, err)
	}
	return result, err
}

func (db *TracedDB) SetError(error)  {
}

func logErrorToSpan(span opentracing.Span, err error) {
	ottag.Error.Set(span, true)
	span.LogFields(otlog.Error(err))
}
