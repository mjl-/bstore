/*
Command bstore provides commands for inspecting a bstore database.

Subcommands:

	usage: bstore types file.db
	       bstore drop file.db type
	       bstore dumptype file.db type
	       bstore keys file.db type
	       bstore records file.db type
	       bstore record file.db type key
	       bstore exportcsv file.db type >export.csv
	       bstore exportjson [flags] file.db [type] >export.json
	       bstore dumpall file.db
*/
package main
