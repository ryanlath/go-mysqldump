package mysqldump

import (
	"bytes"
	"compress/gzip"
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"text/template"
	"time"
)

/*
Data struct to configure dump behavior
    Connection:   Database connection to dump
    IgnoreTables: Mark sensitive tables to ignore
	CharsetName:  utf8 is default
*/
type Data struct {
	Connection       *sql.DB
	IgnoreTables     []string
	MaxAllowedPacket int
	CharsetName      string

	out        bytes.Buffer
	headerTmpl *template.Template
	tableTmpl  *template.Template
	footerTmpl *template.Template
	err        error
}

type table struct {
	Name        string
	Err         error
	CharsetName string

	data   *Data
	rows   *sql.Rows
	types  []reflect.Type
	values []interface{}
}

type metaData struct {
	CharsetName   string
	ServerVersion string
	CompleteTime  string
}

const (
	defaultMaxAllowedPacket = 4194304
	nullType                = "NULL"
)

func RegisterDB(db *sql.DB) *Data {
	return &Data{
		Connection: db,
	}
}

//TODO: some redudancy...
func (data *Data) DumpToFile(dir string, filename string) error {

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return err
	}

	err := data.dump()
	if err != nil {
		return err
	}

	p := path.Join(dir, filename+".sql")

	err = ioutil.WriteFile(p, data.out.Bytes(), 0644)
	if err != nil {
		return err
	}

	return nil
}

func (data *Data) DumpToGzip(dir string, filename string) error {

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return err
	}

	err := data.dump()
	if err != nil {
		return err
	}

	//TODO: probably a better way...
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	gz.Write(data.out.Bytes())
	gz.Close()

	p := path.Join(dir, filename+".sql.gz")

	err = ioutil.WriteFile(p, b.Bytes(), 0644)
	if err != nil {
		return err
	}

	return nil
}

func (data *Data) DumpToString() (string, error) {
	err := data.dump()
	if err != nil {
		return "", err
	}

	return string(data.out.Bytes()), nil
}

// Dump data using struct
func (data *Data) dump() error {
	meta := metaData{}
	data.out.Reset()

	if data.MaxAllowedPacket == 0 {
		data.MaxAllowedPacket = defaultMaxAllowedPacket
	}

	if data.CharsetName == "" {
		data.CharsetName = "utf8"
	}
	meta.CharsetName = data.CharsetName

	if err := meta.updateServerVersion(data.Connection); err != nil {
		return err
	}

	if err := data.getTemplates(); err != nil {
		return err
	}

	if err := data.headerTmpl.Execute(&data.out, meta); err != nil {
		return err
	}

	tables, err := data.getTables()
	if err != nil {
		return err
	}

	for _, name := range tables {
		if err := data.dumpTable(name); err != nil {
			return err
		}
	}
	if data.err != nil {
		return data.err
	}

	meta.CompleteTime = time.Now().String()

	return data.footerTmpl.Execute(&data.out, meta)
}

func (data *Data) dumpTable(name string) error {
	if data.err != nil {
		return data.err
	}
	table, err := data.createTable(name)
	if err != nil {
		return err
	}

	return data.writeTable(table)
}

func (data *Data) writeTable(table *table) error {
	table.CharsetName = data.CharsetName

	if err := data.tableTmpl.Execute(&data.out, table); err != nil {
		return err
	}
	return table.Err
}

// getTemplates initilaizes the templates on data from the constants in this file
func (data *Data) getTemplates() (err error) {
	data.headerTmpl, err = template.New("mysqldumpHeader").Parse(headerTmpl)
	if err != nil {
		return
	}

	data.tableTmpl, err = template.New("mysqldumpTable").Parse(tableTmpl)
	if err != nil {
		return
	}

	data.footerTmpl, err = template.New("mysqldumpTable").Parse(footerTmpl)
	if err != nil {
		return
	}
	return
}

func (data *Data) getTables() ([]string, error) {
	tables := make([]string, 0)

	rows, err := data.Connection.Query("SHOW TABLES")
	if err != nil {
		return tables, err
	}
	defer rows.Close()

	for rows.Next() {
		var table sql.NullString
		if err := rows.Scan(&table); err != nil {
			return tables, err
		}
		if table.Valid && !data.isIgnoredTable(table.String) {
			tables = append(tables, table.String)
		}
	}
	return tables, rows.Err()
}

func (data *Data) isIgnoredTable(name string) bool {
	for _, item := range data.IgnoreTables {
		if item == name {
			return true
		}
	}
	return false
}

func (data *metaData) updateServerVersion(db *sql.DB) (err error) {
	var serverVersion sql.NullString
	err = db.QueryRow("SELECT version();").Scan(&serverVersion)
	data.ServerVersion = serverVersion.String
	return
}

func (data *Data) createTable(name string) (*table, error) {
	t := &table{
		Name: name,
		data: data,
	}

	return t, nil
}

func (table *table) NameEsc() string {
	return "`" + table.Name + "`"
}

func (table *table) CreateSQL() (string, error) {
	var tableReturn, tableSQL sql.NullString
	if err := table.data.Connection.QueryRow("SHOW CREATE TABLE "+table.NameEsc()).Scan(&tableReturn, &tableSQL); err != nil {
		return "", err
	}

	if tableReturn.String != table.Name {
		return "", errors.New("Returned table is not the same as requested table")
	}

	return tableSQL.String, nil
}

// defer rows.Close()
func (table *table) Init() (err error) {
	if len(table.types) != 0 {
		return errors.New("can't init twice")
	}

	table.rows, err = table.data.Connection.Query("SELECT * FROM " + table.NameEsc())
	if err != nil {
		return err
	}

	columns, err := table.rows.Columns()
	if err != nil {
		return err
	}
	if len(columns) == 0 {
		return errors.New("No columns in table " + table.Name + ".")
	}

	tt, err := table.rows.ColumnTypes()
	if err != nil {
		return err
	}

	table.types = make([]reflect.Type, len(tt))
	for i, tp := range tt {
		st := tp.ScanType()
		if tp.DatabaseTypeName() == "BLOB" {
			table.types[i] = reflect.TypeOf(sql.RawBytes{})
		} else if st != nil && (st.Kind() == reflect.Int ||
			st.Kind() == reflect.Int8 ||
			st.Kind() == reflect.Int16 ||
			st.Kind() == reflect.Int32 ||
			st.Kind() == reflect.Int64) {
			table.types[i] = reflect.TypeOf(sql.NullInt64{})
		} else {
			table.types[i] = reflect.TypeOf(sql.NullString{})
		}
	}
	table.values = make([]interface{}, len(tt))
	for i := range table.values {
		table.values[i] = reflect.New(table.types[i]).Interface()
	}
	return nil
}

func (table *table) Next() bool {
	if table.rows == nil {
		if err := table.Init(); err != nil {
			table.Err = err
			return false
		}
	}
	// Fallthrough
	if table.rows.Next() {
		if err := table.rows.Scan(table.values...); err != nil {
			table.Err = err
			return false
		} else if err := table.rows.Err(); err != nil {
			table.Err = err
			return false
		}
	} else {
		table.rows.Close()
		table.rows = nil
		return false
	}
	return true
}

func (table *table) RowValues() string {
	return table.RowBuffer().String()
}

func (table *table) RowBuffer() *bytes.Buffer {
	var b bytes.Buffer
	b.WriteString("(")

	for key, value := range table.values {
		if key != 0 {
			b.WriteString(",")
		}
		switch s := value.(type) {
		case nil:
			b.WriteString(nullType)
		case *sql.NullString:
			if s.Valid {
				fmt.Fprintf(&b, "'%s'", sanitize(s.String))
			} else {
				b.WriteString(nullType)
			}
		case *sql.NullInt64:
			if s.Valid {
				fmt.Fprintf(&b, "%d", s.Int64)
			} else {
				b.WriteString(nullType)
			}
		case *sql.RawBytes:
			if len(*s) == 0 {
				b.WriteString(nullType)
			} else {
				fmt.Fprintf(&b, "_binary '%s'", sanitize(string(*s)))
			}
		default:
			fmt.Fprintf(&b, "'%s'", value)
		}
	}
	b.WriteString(")")

	return &b
}

func (table *table) Stream() <-chan string {
	valueOut := make(chan string, 1)
	go func() {
		defer close(valueOut)
		var insert bytes.Buffer

		for table.Next() {
			b := table.RowBuffer()
			// Truncate our insert if it won't fit
			if insert.Len() != 0 && insert.Len()+b.Len() > table.data.MaxAllowedPacket-1 {
				insert.WriteString(";")
				valueOut <- insert.String()
				insert.Reset()
			}

			if insert.Len() == 0 {
				fmt.Fprintf(&insert, "INSERT INTO %s VALUES ", table.NameEsc())
			} else {
				insert.WriteString(",")
			}
			b.WriteTo(&insert)
		}
		if insert.Len() != 0 {
			insert.WriteString(";")
			valueOut <- insert.String()
		}
	}()
	return valueOut
}
