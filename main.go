package main

import (
	"bufio"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"log"
	"net/http"
	"os"
	"time"
)

const (
	loadFromDB                   = false
	rateLimit                    = 100 * time.Millisecond
	authToken                    = "583c97e4277fce6b6769277d100dae6e10c55e96cc574ac187848c10f4998c31"
	userDB                       = "XXXX"
	passDB                       = "XXXX"
	urlDB                        = "paymentmethod00.slave01.mlaws.com:6612"
	schemaDB                     = "pmethods"
	sqlDeductionSchemaRelCount   = "select count(1) from deduction_schema_rel where marketplace = 'NONE' and status = 'active' and site_id = 'MLA' and expiration_date < SYSDATE() order by id desc"
	sqlDeductionSchemaRel        = "select collector_id, differential_pricingid from deduction_schema_rel where marketplace = 'NONE' and status = 'active' and site_id = 'MLA' and expiration_date < SYSDATE()"
	requestDeductionSearchBasUrl = "http://beta.mpcs-differential-pricing-v2.melifrontends.com"
	requestDeductionSearch       = "/users/%v/deduction/search?user_id=%v&marketplace=NONE&diff_pricing_id=%v&site_id=MLA&client_id=7098342686239412"
	fileName                     = "202003200900.txt"
)

var arrayInstallments = [...]string{"1", "3", "6", "9", "12", "18"}
var arrayPaymentMethodIdWithPoint = [...]string{"amex", "maestro", "maestro", "master", "visa"}

func main() {
	if loadFromDB == true {
		var arrayUserId, arrayDiffPricingId []string = loadDataFromDeductionSchemaRelTable()
		executeDeductionSearchAndSaveResult(arrayUserId, arrayDiffPricingId)
	} else {
		readAndSendRequest()
	}
}

func readAndSendRequest() {
	readFile, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("failed to open file: %s", err)
	}

	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)
	var fileTextLines []string

	for fileScanner.Scan() {
		fileTextLines = append(fileTextLines, fileScanner.Text())
	}

	readFile.Close()

	for i, line := range fileTextLines {
		response := sendRequest(line)
		// status 200
		if response.StatusCode == 200 {
			if err != nil {
				fmt.Println(err)
			}
		}
		fmt.Println(i+1, ") ", response.Status, " - ", line)
	}
}

func executeDeductionSearchAndSaveResult(arrayUserId []string, arrayDiffPricingId []string) {
	f, err := os.Create(fileName)
	if err != nil {
		fmt.Println(err)
		return
	}

	var rowNum = 0
	for i := 0; i < len(arrayUserId); i++ {
		var requestUrl = fmt.Sprintf(requestDeductionSearch, arrayUserId[i], arrayUserId[i], arrayDiffPricingId[i])
		for j := 0; j < len(arrayPaymentMethodIdWithPoint); j++ {
			var requestUrlWithPMI = requestUrl + "&payment_method_id=" + arrayPaymentMethodIdWithPoint[j]
			for k := 0; k < len(arrayInstallments); k++ {
				rowNum++
				var requestUrlWithInstallment = requestUrlWithPMI + "&installments=" + arrayInstallments[k]
				response := sendRequest(requestUrlWithInstallment)
				// status 200
				if response.StatusCode == 200 {
					_, err := f.WriteString(requestUrlWithInstallment + "\n")
					if err != nil {
						fmt.Println(err)
						f.Close()
						return
					}
				}
				fmt.Println(rowNum, ") ", response.Status, " - ", requestUrlWithInstallment)
			}
		}
	}

	err = f.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
}

func loadDataFromDeductionSchemaRelTable() ([]string, []string) {
	db, err := sqlx.Connect("mysql", userDB+":"+passDB+"@tcp("+urlDB+")/"+schemaDB)
	if err != nil {
		log.Fatal(err)
	}
	rows0, err0 := db.Query(sqlDeductionSchemaRelCount)
	if err0 != nil {
		log.Fatal(err)
	}
	countRows := checkCount(rows0)

	rows, err := db.Query(sqlDeductionSchemaRel)
	if err != nil {
		log.Fatal(err)
	}
	cols, err := rows.Columns() // Remember to check err afterwards
	if err != nil {
		log.Fatal(err)
	}

	arrayUserId := make([]string, countRows)
	arrayDiffPricingId := make([]string, countRows)

	aValues := make([]sql.RawBytes, len(cols))
	aScanArgs := make([]interface{}, len(cols))
	for i, _ := range aValues {
		aScanArgs[i] = &aValues[i]
	}
	index := 0
	for rows.Next() {
		err = rows.Scan(aScanArgs...)
		if err != nil {
			log.Fatal(err)
		}
		for i, col := range aValues {
			var column = cols[i]
			if column == "collector_id" {
				arrayUserId[index] = string(col)
			} else if column == "differential_pricingid" {
				arrayDiffPricingId[index] = string(col)
			}
		}
		index++
	}
	return arrayUserId, arrayDiffPricingId
}

func checkCount(rows *sql.Rows) (count int) {
	for rows.Next() {
		err := rows.Scan(&count)
		checkErr(err)
	}
	return count
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func sendRequest(requestParam string) *http.Response {
	var requestHttpBase = requestDeductionSearchBasUrl + requestParam
	request, _ := http.NewRequest("GET", requestHttpBase, nil)
	request.Header.Set("x-caller-scopes", "point")
	request.Header.Set("x-auth-token", authToken)
	client := &http.Client{}
	response, e := client.Do(request)
	if e != nil {
		log.Fatal("error")
	}
	time.Sleep(rateLimit)
	return response
}
