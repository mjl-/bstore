/*
Command bstore provides commands for inspecting a bstore database.

Subcommands:

	usage: store types file.db
	       store drop file.db type
	       store dumptype file.db type
	       store keys file.db type
	       store records file.db type
	       store record file.db type key
	       store exportcsv file.db type >export.csv
	       store exportjson [flags] file.db [type] >export.json
	       store dumpall file.db
*/
package main
