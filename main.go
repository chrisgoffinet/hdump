package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/sdming/goh"
	"github.com/sdming/goh/Hbase"
)

func main() {
	host := flag.String("host", "localhost:9090", "hbase hostname")
	tableName := flag.String("table", "", "table name")
	cmd := flag.String("cmd", "", "import or export")
	numWorkers := flag.Int("workers", 20, "number of workers used for importing")
	flag.Parse()

	if *tableName == "" {
		log.Fatal("table name required")
	}

	if *cmd == "" {
		log.Fatal("cmd required, import or export")
	}

	switch *cmd {
	case "export":
		exportTable(*host, *tableName)
	case "import":
		importTable(*host, *tableName, *numWorkers)
	}
}

func exportTable(host string, tableName string) {
	exportFile := fmt.Sprintf("%s.json", tableName)
	file, err := os.Create(exportFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	client, err := goh.NewTcpClient(host, goh.TBinaryProtocol, false)
	if err != nil {
		panic(err)
	}
	if err = client.Open(); err != nil {
		panic(err)
	}
	defer client.Close()

	id, err := client.ScannerOpen(tableName, []byte(""), []string{}, map[string]string{})
	if err != nil {
		panic(err)
	}
	rows := 0
	for {
		if data, err := client.ScannerGet(id); err != nil {
			break
		} else if len(data) == 0 {
			break
		} else {
			j, err := dump(data)
			if err != nil {
				panic(err)
			}
			file.WriteString(j)
			file.WriteString("\n")
			rows++

			if rows%1000 == 0 {
				log.Printf("Dumping rows: %d", rows)
			}
		}
	}
}

func worker(wg *sync.WaitGroup, host string, tableName string, bufCh chan string) {
	defer wg.Done()

	client, err := goh.NewTcpClient(host, goh.TBinaryProtocol, false)
	if err != nil {
		panic(err)
	}
	if err = client.Open(); err != nil {
		panic(err)
	}
	defer client.Close()

	for {
		select {
		case v, open := <-bufCh:
			if open == false {
				return
			}
			var data []Hbase.TRowResult
			if err := json.Unmarshal([]byte(v), &data); err != nil {
				panic(err)
			}
			for _, row := range data {
				var mutations []*Hbase.Mutation
				for name, column := range row.Columns {
					mutation := goh.NewMutation(name, column.Value)
					mutations = append(mutations, mutation)
				}
				client.MutateRow(tableName, row.Row, mutations, map[string]string{})
			}
		}
	}
}

func importTable(host string, tableName string, numWorkers int) {
	importFile := fmt.Sprintf("%s.json", tableName)
	file, err := os.Open(importFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	wg := new(sync.WaitGroup)
	bufCh := make(chan string, 10000)

	// spawn enough workers as goroutines
	for index := 0; index < numWorkers; index++ {
		go worker(wg, host, tableName, bufCh)
		wg.Add(1)
	}

	rows := 0
	for scanner.Scan() {
		line := scanner.Text()
		bufCh <- line
		rows++

		if rows%1000 == 0 {
			log.Printf("Importing rows: %d", rows)
		}
	}

	close(bufCh)
	wg.Wait()

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

}

func dump(data interface{}) (string, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
