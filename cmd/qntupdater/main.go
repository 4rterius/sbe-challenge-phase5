package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql" // mysql driver
	"log"
	"os"
	"qntupdater/internal/qntupdater"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatalln("Incorrect number of arguments, usage:  qntupdater source.csv config.my")
	}

	log.Printf("Script called to read data from '%s' using config at '%s'\n", os.Args[1], os.Args[2])

	// Parse config

	config := qntupdater.ScriptConfig{}
	err := config.ReadFrom(os.Args[2])
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("Config parsed, connecting to '%s'@'localhost', prefix='%s' as '%s'\n", config.DbUser, config.TablePrefix, config.DbUser)

	// Parse input values

	records, err := qntupdater.GetQuantitiesFromFile(os.Args[1])
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("Records parsed, %d in total\n", len(records))

	// Connect to the database and open a transaction

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(localhost:3306)/%s", config.DbUser, config.DbPswd, config.DbName))
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("Connected and pinged the database")

	// Prepare statements

	stmtUpdateProduct, err := db.Prepare(
		fmt.Sprintf("UPDATE %sproduct SET quantity=? WHERE ean13=?", config.TablePrefix))
	if err != nil {
		log.Fatalln(err)
	}
	defer stmtUpdateProduct.Close()

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
		log.Fatalln(err)
	}
	defer stmtIDStockAvailable.Close()

	stmtUpdateCombination, err := db.Prepare(
		fmt.Sprintf("UPDATE %sstock_available SET quantity=? WHERE id_stock_available=?", config.TablePrefix))
	if err != nil {
		log.Fatalln(err)
	}
	defer stmtUpdateCombination.Close()

	// Run the main loop

	for i, r := range records {
		ean13, qnt := r[0], r[1]
		log.Printf("Processing record  idx=%d ean13=%s qnt=%s\n", i, ean13, qnt)

		// Try to update product
		_, err := stmtUpdateProduct.Exec(qnt, ean13)
		if err != nil {
			log.Printf("Failed to update product  record=%d err=%v\n", i, err)
		}

		// Get id_stock_available
		rows, err := stmtIDStockAvailable.Query(ean13)
		if err != nil {
			log.Printf("Failed to query 'id_stock_available'  record=%d err=%v\n", i, err)
			break
		}

		var idStockAvailable int = 0

		if rows.Next() {
			err := rows.Scan(&idStockAvailable)
			if err != nil {
				log.Printf("Failed to extract 'id_stock_available' from query result  record=%d err=%v\n", i, err)
			} else {
				_, err := stmtUpdateCombination.Exec(qnt, idStockAvailable)
				if err != nil {
					log.Printf("Failed to update combination  id_stock_available=%d record=%d err=%v\n", idStockAvailable, i, err)
				}
			}
		}

		if err = rows.Err(); err != nil {
			log.Printf("Error at 'id_stock_available' query  record=%d err=%v\n", i, err)
		}

		rows.Close()
	}
}
