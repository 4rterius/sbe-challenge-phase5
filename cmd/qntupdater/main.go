package main

import (
	"database/sql"
	"fmt"
	"os"
	"qntupdater/internal/qntupdater"

	_ "github.com/go-sql-driver/mysql" // mysql driver
	lg "github.com/sirupsen/logrus"
)

var lgr *lg.Logger = lg.New()

func main() {
	// Set up the logger
	lgr.SetOutput(os.Stdout)

	// Parse cli arguments

	if len(os.Args) != 3 {
		lgr.WithFields(lg.Fields{
			"got":      len(os.Args) - 1,
			"expected": 2,
		}).Fatalln("received incorrect number of arguments")
	}

	lgr.WithFields(lg.Fields{
		"input_path":  os.Args[1],
		"config_path": os.Args[2],
	}).Infoln("started execution")

	// Parse config

	config := qntupdater.ScriptConfig{}
	err := config.ReadFrom(os.Args[2])
	if err != nil {
		lgr.WithFields(lg.Fields{
			"err":  err,
			"path": os.Args[2],
		}).Fatalln("failed to read config")
	}

	lgr.WithFields(lg.Fields{
		"db_name":    config.DbName,
		"db_user":    config.DbUser,
		"tbl_prefix": config.TablePrefix,
	}).Infoln("parsed config")

	// Parse input values

	records, err := qntupdater.GetQuantitiesFromFile(os.Args[1])
	if err != nil {
		lgr.WithFields(lg.Fields{
			"err":  err,
			"path": os.Args[1],
		}).Fatalln("failed to parse quantities")
	}

	lgr.WithFields(lg.Fields{
		"num": len(records),
	}).Infoln("parsed quantities")

	// Connect to the database

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(localhost:3306)/%s", config.DbUser, config.DbPswd, config.DbName))
	if err != nil {
		lgr.WithFields(lg.Fields{"err": err}).Fatalln("failed to open database")
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		lgr.WithFields(lg.Fields{"err": err}).Fatalln("failed to ping database")
	}

	lgr.Infoln("connected and pinged database")

	// Build ean13 -> id_stock_available map

	stmtIDStockAvailable, err := db.Prepare(
		fmt.Sprintf(
			"SELECT id_stock_available"+
				" FROM %sproduct_attribute"+
				" INNER JOIN %sstock_available"+
				"   ON %sproduct_attribute.id_product=%sstock_available.id_product"+
				"   AND %sproduct_attribute.id_product_attribute=%sstock_available.id_product_attribute"+
				" WHERE ean13=?",
			config.TablePrefix, config.TablePrefix, config.TablePrefix,
			config.TablePrefix, config.TablePrefix, config.TablePrefix))
	if err != nil {
		lgr.WithFields(lg.Fields{
			"err":  err,
			"subj": "id_stock_available",
		}).Fatalln("failed to prepare statement")
	}
	defer stmtIDStockAvailable.Close()

	idStockAvailableMap := make(map[string]int)
	idStockAvailable := 0

	for i, r := range records {
		ean13 := r[0]

		rows, err := stmtIDStockAvailable.Query(ean13)
		if err != nil {
			lgr.WithFields(lg.Fields{
				"err":    err,
				"subj":   "id_stock_available",
				"record": i,
			}).Errorln("failed to query")
			continue
		}

		if rows.Next() {
			err := rows.Scan(&idStockAvailable)
			if err != nil {
				lgr.WithFields(lg.Fields{
					"err":    err,
					"subj":   "id_stock_available",
					"record": i,
				}).Errorln("failed to extract the value from query result")
			} else {
				idStockAvailableMap[ean13] = idStockAvailable
			}
		}

		rows.Close()
	}

	// Open the update transaction

	loopSuccess := true

	tx, err := db.Begin()
	if err != nil {
		lgr.WithFields((lg.Fields{"err": err})).Fatalln("failed to start a transaction")
	}
	defer func() {
		if loopSuccess {
			// Commit the update transaction if succeeded for each input record
			if err := tx.Commit(); err != nil {
				lgr.WithFields(lg.Fields{"err": err}).Error("failed to commit update transaction")
			} else {
				lgr.Info("commited update transaction")
			}
		} else {
			// Roll it back if got an error at least once
			if err := tx.Rollback(); err != nil {
				lgr.WithFields(lg.Fields{"err": err}).Error("failed to roll back update transaction")
			} else {
				lgr.Warn("rolled back update transaction")
			}
		}
	}()

	lgr.Infoln("started an update transaction")

	// Prepare update statements in the transaction

	stmtUpdateProduct, err := tx.Prepare(
		fmt.Sprintf("UPDATE %sproduct SET quantity=? WHERE ean13=?", config.TablePrefix))
	if err != nil {
		lgr.WithFields(lg.Fields{
			"err":  err,
			"subj": "update_product",
		}).Fatalln("failed to prepare statement")
	}
	defer stmtUpdateProduct.Close()

	stmtUpdateCombination, err := tx.Prepare(
		fmt.Sprintf("UPDATE %sstock_available SET quantity=? WHERE id_stock_available=?", config.TablePrefix))
	if err != nil {
		lgr.WithFields(lg.Fields{
			"err":  err,
			"subj": "update_combination",
		}).Fatalln("failed to prepare statement")
	}
	defer stmtUpdateCombination.Close()

	// Run the main loop

	for i, r := range records {
		ean13, qnt := r[0], r[1]
		rowsAffected := int64(0)

		// qnt = "0"

		lgr.WithFields(lg.Fields{
			"ean13":    ean13,
			"quantity": qnt,
			"record":   i,
		}).Infoln("processing record")

		// Try to update product
		res, err := stmtUpdateProduct.Exec(qnt, ean13)
		if err != nil {
			lgr.WithFields(lg.Fields{
				"err":    err,
				"record": i,
			}).Errorln("failed to update product")
			loopSuccess = false
		}

		if rowc, err := res.RowsAffected(); err != nil {
			lgr.WithFields(lg.Fields{
				"err":    err,
				"record": i,
				"stmt":   "update_product",
			}).Errorln("failed to retrieve rows affected")
			loopSuccess = false
		} else {
			rowsAffected = rowc
		}

		// Try to update the combinations
		if idStockAvailable, ok := idStockAvailableMap[ean13]; ok {
			_, err := stmtUpdateCombination.Exec(qnt, idStockAvailable)
			if err != nil {
				lgr.WithFields(lg.Fields{
					"err":                err,
					"record":             i,
					"id_stock_available": idStockAvailable,
				}).Errorln("failed to update combination")
				loopSuccess = false
			}
		} else if rowsAffected == 0 {
			lgr.WithFields(lg.Fields{
				"ean13":  ean13,
				"record": i,
			}).Infoln("product not affected and ean13 not in the id_stock_available map")
		}
	}
}
