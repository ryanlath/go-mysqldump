# Go MYSQL Dump
Create MYSQL dumps in Go without `mysqldump`

> :warning: Fork of a fork...

### Simple Examples
```go
	db := YourMysqlOrMariaDBConnection
	dump := mysqldump.RegisterDB(db) 
	dump.CharsetName = "utf8mb" // default is "utf8"


	// dump database to string
	sql, err := dump.DumpToString()
	if err != nil {
		fmt.Println("Error dumping:", err)
		return
	}

	fmt.Println(sql)


	// Dump database to file 
	directory := "assets/backup" // must exist and be writeable by pgm
	filename := "database-name_" + time.Now().Format("2006-01-_2-150405") // ".sql" will be appended

	err = dump.DumpToFile(directory, filename)
	if err != nil {
		fmt.Println("Error dumping:", err)
		return
	}

	fmt.Println("File is saved.")


	// Dump database to gzip file 
	err = dump.DumpToGzip(directory, filename) // ".sql.gz" will be appended
	if err != nil {
		fmt.Println("Error dumping:", err)
		return
	}

	fmt.Println("File is saved.")
```
