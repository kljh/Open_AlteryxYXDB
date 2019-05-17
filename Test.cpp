#include "stdafx.h"
#include "Open_AlteryxYXDB.h"
#include <iostream>
#include <fstream>
#include <sqlite3.h>

void replace_all(std::string& str, const std::string& search_str, const std::string& replace_by_str) {
	size_t pos = 0;
	while ((pos = str.find(search_str, pos)) != std::string::npos) {
		str.replace(pos, search_str.length(), replace_by_str);
		pos += replace_by_str.length();
	}
}

std::string rfc4180_csv_escape(const char *cstr) {
	std::string str(cstr);
	if ( !str.empty()
		&& str.find(",")  == std::string::npos
		&& str.find("\n") == std::string::npos
		&& str.find("\"") == std::string::npos)
		return str;

	// in case text contains commas, new line or double quote (and also for empty text), 
	// then we put text between double quotes (and repeat double quotes within).
	replace_all(str, "\"", "\"\"");
	return "\"" + str + "\"";
}

void ReadToCsvFile(const wchar_t *pFile, const wchar_t *pCsvOutFile)
{
	Alteryx::OpenYXDB::Open_AlteryxYXDB file;
	file.Open(pFile);

	std::ofstream fout;
	if (pCsvOutFile) fout.open(pCsvOutFile, std::ofstream::out);

	std::ostream& out = pCsvOutFile ? fout : std::cout;

	// use double precision floating point numbers (default is 6 digits)
	out.precision(15);

	// you can ask about how many fields are in the file, what are there names and types, etc...
	for (unsigned x = 0; x < file.m_recordInfo.NumFields(); ++x)
	{
		if (x != 0)
			out << ",";

		// the FieldBase object has all kinds of information about the field
		// it will also help us (later) get a specific value from a record
		const SRC::FieldBase * pField = file.m_recordInfo[x];
		out << rfc4180_csv_escape(SRC::ConvertToAString(pField->GetFieldName().c_str()));
	}
	out << "\n";

	// read 1 record at a time from the YXDB.  When the file as read past
	// the last record, ReadRecord will return nullptr
	// You could have also called file.GetNumRecords() to know the total ahead of time
	while (const SRC::RecordData *pRec = file.ReadRecord())
	{
		// we now have a record (pRec) but it is an opaque structure
		// we need to use the FieldBase objects to get actual values from it.
		for (unsigned x = 0; x<file.m_recordInfo.NumFields(); ++x)
		{
			// the recordInfo object acts like an array of FieldBase objects
			const SRC::FieldBase * pField = file.m_recordInfo[x];

			if (x != 0)
				out << ",";

			if (IsBinary(pField->m_ft))
			{
				// binary fields are not implicitly convertable to strings
			}
			else if (IsFloat(pField->m_ft))
			{
				// floating number with appropriate precision
				out << pField->GetAsDouble(pRec).value;
			}
			else
			{
				// you could (and probably should) as for GetAsWString to get the unicode value
				out << rfc4180_csv_escape(pField->GetAsAString(pRec).value.pValue);
			}
		}
		out << "\n";
	}
}

std::string ansi_sql_escape(const char *cstr) {
	return rfc4180_csv_escape(cstr);
}

static int sqlite_exec_callback(void *NotUsed, int argc, char **argv, char **azColName) {
    for (int i = 0; i<argc; i++) {
      	printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
	}
	printf("\n");
	return 0;
}

int ReadToSQLiteFile(const wchar_t *pFile, const wchar_t *pSQLiteOutFile, const wchar_t *pSQLiteOutTable, bool read_blob)
{
	Alteryx::OpenYXDB::Open_AlteryxYXDB file;
	fprintf(stdout, "YXDB path: %S \n", pFile);
	file.Open(pFile);
	int numRec(file.GetNumRecords());
	fprintf(stdout, "YXDB records: #%i\n", numRec);
	fprintf(stdout, read_blob ? "Include BLOBs\n" : "Ignore BLOBs (pass 'blob' as 4th argument to include)\n");
	
	sqlite3 *db;
	char *err = 0;
	int rc = sqlite3_open16(pSQLiteOutFile, &db);
	if (rc) {
		fprintf(stderr, "SQLite open error: %s.\n", sqlite3_errmsg(db));
		return 1;
	}

	std::wstring table_name_wstr(pSQLiteOutTable);
	std::string  table_name(table_name_wstr.begin(), table_name_wstr.end());
	
	std::string drop_table_stmt, create_table_stmt, insert_stmt;
	for (unsigned j = 0; j < file.m_recordInfo.NumFields(); ++j)
	{
		if (j != 0) 
			create_table_stmt +=  ", ";

		const SRC::FieldBase * pField = file.m_recordInfo[j];
		create_table_stmt += ansi_sql_escape(SRC::ConvertToAString(pField->GetFieldName().c_str()));
		
		if (IsBinary(pField->m_ft))
			create_table_stmt += " BLOB";
		else if (IsBoolOrInteger(pField->m_ft))
			create_table_stmt += " INTEGER";
		else if (IsFloat(pField->m_ft) || IsNumeric(pField->m_ft)) // FixedDecimal is not a Float
			create_table_stmt += " REAL";
		else if (IsStringOrDate(pField->m_ft))
			create_table_stmt += " TEXT";

		insert_stmt += j==0 ? "?" : ", ?";
	}
	
	drop_table_stmt = "DROP TABLE IF EXISTS " + table_name;
	create_table_stmt = "CREATE TABLE " + table_name  +" ( " + create_table_stmt + " )";
	insert_stmt = "INSERT INTO " + table_name + " VALUES ( " + insert_stmt + " )";

	if (SQLITE_OK != (rc = sqlite3_exec(db, drop_table_stmt.c_str(), sqlite_exec_callback, 0, &err))) {
		fprintf(stderr, "SQLite DROP error: %s.\n", err);
		sqlite3_free(err);
		return 1;
	}
	if (SQLITE_OK != (rc = sqlite3_exec(db, create_table_stmt.c_str(), sqlite_exec_callback, 0, &err))) {
		fprintf(stderr, "SQL CREATE error: %s\n", err);
		sqlite3_free(err);
		return 1;
	}
	if (SQLITE_OK != (rc = sqlite3_exec(db, "BEGIN TRANSACTION", sqlite_exec_callback, 0, &err))) {
		fprintf(stderr, "SQL BEGIN TRANSACTION error: %s\n", err);
		sqlite3_free(err);
		return 1;
	}
	
	sqlite3_stmt *stmt = nullptr;
	if (SQLITE_OK != (rc = sqlite3_prepare_v2(db, insert_stmt.c_str(), -1, &stmt, nullptr))) {
		fprintf(stderr, "SQL prepare error %d: %s\n", rc, sqlite3_errmsg(db));
		return 1;
	}

	int iRec = 0;
	while (const SRC::RecordData *pRec = file.ReadRecord())
	{
		iRec++;
		if (iRec%25000==0)
			fprintf(stdout, "record %i / %i.\n", iRec, numRec);


		for (unsigned j = 0; j<file.m_recordInfo.NumFields(); ++j)
		{
			int pos = j + 1;

			const SRC::FieldBase * pField = file.m_recordInfo[j];
			if (IsBinary(pField->m_ft))
			{
				// binary fields are not implicitly convertable to strings
				if (read_blob) {
					auto blob = pField->GetAsBlob(pRec);
					rc = sqlite3_bind_blob(stmt, pos, blob.value.pValue, blob.value.nLength, SQLITE_TRANSIENT);
				} else {
					rc = sqlite3_bind_null(stmt, pos);
				}
			}
			else if (IsBoolOrInteger(pField->m_ft))
			{
				int n = pField->m_nSize;
				if (n == 8) {
					__int64 i = pField->GetAsInt64(pRec).value;
					rc = sqlite3_bind_int64(stmt, pos, i);
				} else {
					int i = pField->GetAsInt32(pRec).value;
					rc = sqlite3_bind_int(stmt, pos, i);
				}
			}
			else if (IsFloat(pField->m_ft) || IsNumeric(pField->m_ft)) // FixedDecimal is not a Float
			{
				double d = pField->GetAsDouble(pRec).value;
				rc = sqlite3_bind_double(stmt, pos, d);
			}
			else if (IsStringOrDate(pField->m_ft))
			{
				auto s = pField->GetAsWString(pRec);
				rc = sqlite3_bind_text16(stmt, pos, s.value.pValue, -1, SQLITE_TRANSIENT);
			}
			else
			{
				fprintf(stderr, "SQL bind unknown type.\n");
				rc = sqlite3_bind_null(stmt, pos);
			}

			if (rc != SQLITE_OK) {
				fprintf(stderr, "SQL bind error %d: %s\n", rc, sqlite3_errmsg(db));
				return 1;
			}
		}

		rc = sqlite3_step(stmt);
		if (SQLITE_DONE != rc) {
			fprintf(stderr, "SQLite step error %d: %s\n", rc, sqlite3_errmsg(db));
			return 1;
		}
		if (SQLITE_OK != (rc = sqlite3_reset(stmt))) {
			fprintf(stderr, "SQL prepare error %d: %s\n", rc, sqlite3_errmsg(db));
			return 1;
		}
	}

	if (SQLITE_OK != (rc = sqlite3_exec(db, "COMMIT TRANSACTION", sqlite_exec_callback, 0, &err))) {
		fprintf(stderr, "SQL COMMIT TRANSACTION error: %s\n", err);
		sqlite3_free(err);
		return 1;
	}

	if (stmt) sqlite3_finalize(stmt);
	sqlite3_close(db);

	fprintf(stdout, "record %i / %i.\n", iRec, numRec);
	return 0;
}

int _tmain(int argc, _TCHAR* argv[])
{
	// most of the functions in this library can throw class Error if something goes wrong
	try
	{
		if (argc > 3)
		{
			bool read_blob = argc > 4 && std::wstring(argv[4]) == L"blob";
			ReadToSQLiteFile(argv[1], argv[2], argv[3], read_blob);
		} 
		else if (argc > 2)
		{
			ReadToCsvFile(argv[1], argv[2]);
		}
		else
		{
			std::wstring wexe(argv[0]);
			std::string  exe(wexe.begin(), wexe.end());
			std::cout 
				<< "Usage (CSV):    " << exe << " <yxdb input file> <csv output file> \n"
				<< "Usage (SQLite): " << exe << " <yxdb input file> <sqlite output file> <sqlite table name> [blob] \n\n";
		}
	}
	catch (SRC::Error e)
	{
		std::cerr << SRC::ConvertToAString(e.GetErrorDescription()) << "\n";
	}
	return 0;
}
