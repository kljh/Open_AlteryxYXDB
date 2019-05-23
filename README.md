## OpenAlteryxYXDB

# Convert XYDB files into SQLite databases.

&nbsp;

## Usage:

- Convert to SQLite database:

`` OpenAlteryxYXDB.exe input.yxdb output.sqlite table_name ``

- Convert to SQLite database (including binary blobs fields):

`` OpenAlteryxYXDB.exe input.yxdb output.sqlite table_name "blob" ``

- Convert to CSV:

`` OpenAlteryxYXDB.exe input.yxdb output.csv ``


&nbsp;

## Type mapping


| YXDB Type | SQLite Type | SQLite affinity |
| --- | ---: | :--- |
| V_WString | nvarchar | TEXT |
| WString | nchar | TEXT |
| V_String | varchar | TEXT |
| String | char | TEXT |
| Int64 | big int | INTEGER |
| Int32 | int | INTEGER |
| Int16 | smallint | INTEGER |
| Byte | tinyint unsigned | INTEGER |
| Bool | boolean | NUMERIC |
| Double | double | REAL |
| Float | float | REAL |
| FixedDecimal | decimal | NUMERIC |
| DateTime | datetime | NUMERIC |
| Date | date | NUMERIC |
| Time | time | NUMERIC |
| Blob | blob | BLOB |


&nbsp;


Based on http://inspiringingenuity.net/2015/05/08/alteryx-open-source-yxdb/
