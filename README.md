# HDump
Simple utility for dumping and importing HBase tables to json

# Requirements
golang 1.11

```
$ brew install golang
```
# Build
```
$ go build
```

# Usage
Exporting a table to json
```
$ hdump -host localhost:9090 -table table1 -cmd export
```
This will produce a file called `table1.json`

Importing a table from json
```
$ hdump -host localhost:9090 -table table1 -cmd import
```