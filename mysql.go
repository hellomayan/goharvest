package goharvest

import (
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"sort"
)

type mysqlDB struct {
	db        *sql.DB
	markStmt  *sql.Stmt
	purgeStmt *sql.Stmt
	resetStmt *sql.Stmt
}

// NewMySQLBinding creates a mysql binding for the given dataSource and outboxTable args.
func NewMySQLBinding(dataSource string, outboxTable string) (DatabaseBinding, error) {
	return newPostgresBinding(func() (*sql.DB, error) {
		return sql.Open("mysql", dataSource)
	}, outboxTable)
}

func newMySQLBinding(dbProvider databaseProvider, outboxTable string) (DatabaseBinding, error) {
	success := false
	var db *sql.DB
	var markStmt, purgeStmt, resetStmt *sql.Stmt
	defer func() {
		if !success {
			if db != nil {
				db.Close()
			}
			closeResources(markStmt, purgeStmt, resetStmt)
		}
	}()

	db, err := dbProvider()
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)

	markStmt, err = db.Prepare(fmt.Sprintf(markQueryTemplate, outboxTable, outboxTable))
	if err != nil {
		return nil, err
	}

	purgeStmt, err = db.Prepare(fmt.Sprintf(purgeQueryTemplate, outboxTable))
	if err != nil {
		return nil, err
	}

	resetStmt, err = db.Prepare(fmt.Sprintf(resetQueryTemplate, outboxTable))
	if err != nil {
		return nil, err
	}

	success = true
	return &database{
		db:        db,
		markStmt:  markStmt,
		purgeStmt: purgeStmt,
		resetStmt: resetStmt,
	}, nil
}

func StandardMySQLBindingProvider() DatabaseBindingProvider {
	return NewMySQLBinding
}

func (db mysqlDB) Mark(leaderID uuid.UUID, limit int) ([]OutboxRecord, error) {
	rows, err := db.markStmt.Query(leaderID, limit)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	records := make([]OutboxRecord, 0, limit)
	for rows.Next() {
		record := OutboxRecord{}
		var keys []string
		var values []string
		err := rows.Scan(
			&record.ID,
			&record.CreateTime,
			&record.KafkaTopic,
			&record.KafkaKey,
			&record.KafkaValue,
			keys,
			values,
			//TODO implement Array for MYSQL
			//pq.Array(&keys),
			//pq.Array(&values),
			&record.LeaderID,
		)
		if err != nil {
			return nil, err
		}
		numKeys := len(keys)
		if len(keys) != len(values) {
			return nil, fmt.Errorf("unequal number of header keys (%d) and values (%d)", numKeys, len(values))
		}

		record.KafkaHeaders = make(KafkaHeaders, numKeys)
		for i := 0; i < numKeys; i++ {
			record.KafkaHeaders[i] = KafkaHeader{keys[i], values[i]}
		}
		records = append(records, record)
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].ID < records[j].ID
	})

	return records, nil
}

func (db mysqlDB) Purge(id int64) (bool, error) {
	res, err := db.purgeStmt.Exec(id)
	if err != nil {
		return false, err
	}
	affected, _ := res.RowsAffected()
	if affected != 1 {
		return false, nil
	}
	return true, err
}

func (db mysqlDB) Reset(id int64) (bool, error) {
	res, err := db.resetStmt.Exec(id)
	if err != nil {
		return false, err
	}
	affected, _ := res.RowsAffected()
	if affected != 1 {
		return false, nil
	}
	return true, err
}

func (db mysqlDB) Dispose() {
	db.db.Close()
	closeResources(db.markStmt, db.purgeStmt, db.resetStmt)
}
