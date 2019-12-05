package qntupdater

import (
	"bufio"
	"encoding/csv"
	"os"
	"strconv"
)

func GetQuantitiesFromFile(path string) ([][]string, error) {
	inputFile, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer inputFile.Close()

	inputReader := csv.NewReader(bufio.NewReader(inputFile))
	inputReader.Comma = ';'

	records, err := inputReader.ReadAll()

	// Validate right-hand column (quantities)
	for _, r := range records {
		if _, err := strconv.ParseInt(r[1], 10, 32); err != nil {
			return nil, err
		}
	}

	return records, nil
}
