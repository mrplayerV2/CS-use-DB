using DbAccess;
using log4net;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

public class SqlServerToSQLite
{
	private static bool _isActive = false;

	private static bool _cancelled = false;

	private static Regex _keyRx = new Regex("(([a-zA-Z_äöüÄÖÜß0-9\\.]|(\\s+))+)(\\(\\-\\))?");

	private static Regex _defaultValueRx = new Regex("\\(N(\\'.*\\')\\)");

	private static ILog _log = LogManager.GetLogger(typeof(SqlServerToSQLite));

	public static bool IsActive => _isActive;

	public static void CancelConversion()
	{
		_cancelled = true;
	}

	public static void ConvertSqlServerToSQLiteDatabase(string sqlServerConnString, string sqlitePath, string password, SqlConversionHandler handler, SqlTableSelectionHandler selectionHandler, FailedViewDefinitionHandler viewFailureHandler, bool createTriggers, bool createViews)
	{
		_cancelled = false;
		ThreadPool.QueueUserWorkItem(delegate
		{
			try
			{
				_isActive = true;
				ConvertSqlServerDatabaseToSQLiteFile(sqlServerConnString, sqlitePath, password, handler, selectionHandler, viewFailureHandler, createTriggers, createViews);
				_isActive = false;
				handler(done: true, success: true, 100, "資料移轉成功");
			}
			catch (SqlException exception)
			{
				_log.Error("資料移轉失敗", exception);
				_isActive = false;
				handler(done: true, success: false, 100, "查無原先POS系統資料庫，請洽客服人員");
			}
			catch (Exception ex)
			{
				_log.Error("資料移轉失敗", ex);
				_isActive = false;
				handler(done: true, success: false, 100, ex.Message);
			}
		});
	}

	private static void ConvertSqlServerDatabaseToSQLiteFile(string sqlConnString, string sqlitePath, string password, SqlConversionHandler handler, SqlTableSelectionHandler selectionHandler, FailedViewDefinitionHandler viewFailureHandler, bool createTriggers, bool createViews)
	{
		if (File.Exists(sqlitePath))
		{
			File.Delete(sqlitePath);
		}
		DatabaseSchema databaseSchema = ReadSqlServerSchema(sqlConnString, handler, selectionHandler);
		CreateSQLiteDatabase(sqlitePath, databaseSchema, password, handler, viewFailureHandler, createViews);
		CopySqlServerRowsToSQLiteDB(sqlConnString, sqlitePath, databaseSchema.Tables, password, handler);
		if (createTriggers)
		{
			AddTriggersForForeignKeys(sqlitePath, databaseSchema.Tables, password, handler);
		}
	}

	private static void CopySqlServerRowsToSQLiteDB(string sqlConnString, string sqlitePath, List<TableSchema> schema, string password, SqlConversionHandler handler)
	{
		CheckCancelled();
		handler(done: false, success: true, 0, "準備複製資料表...");
		_log.Debug("preparing to insert tables ...");
		using (SqlConnection sqlConnection = new SqlConnection(sqlConnString))
		{
			sqlConnection.Open();
			using (SQLiteConnection sQLiteConnection = new SQLiteConnection(CreateSQLiteConnectionString(sqlitePath, password)))
			{
				sQLiteConnection.Open();
				for (int i = 0; i < schema.Count; i++)
				{
					SQLiteTransaction sQLiteTransaction = sQLiteConnection.BeginTransaction();
					try
					{
						using (SqlDataReader sqlDataReader = new SqlCommand(BuildSqlServerTableQuery(schema[i]), sqlConnection).ExecuteReader())
						{
							SQLiteCommand sQLiteCommand = BuildSQLiteInsert(schema[i]);
							int num = 0;
							while (sqlDataReader.Read())
							{
								sQLiteCommand.Connection = sQLiteConnection;
								sQLiteCommand.Transaction = sQLiteTransaction;
								List<string> list = new List<string>();
								for (int j = 0; j < schema[i].Columns.Count; j++)
								{
									string text = "@" + GetNormalizedName(schema[i].Columns[j].ColumnName, list);
									sQLiteCommand.Parameters[text].Value = CastValueForColumn(sqlDataReader[j], schema[i].Columns[j]);
									list.Add(text);
								}
								sQLiteCommand.ExecuteNonQuery();
								num++;
								if (num % 1000 == 0)
								{
									CheckCancelled();
									sQLiteTransaction.Commit();
									handler(done: false, success: true, (int)(100.0 * (double)i / (double)schema.Count), "已複製 " + num + " 行: 資料表 " + schema[i].TableName);
									sQLiteTransaction = sQLiteConnection.BeginTransaction();
								}
							}
						}
						CheckCancelled();
						sQLiteTransaction.Commit();
						handler(done: false, success: true, (int)(100.0 * (double)i / (double)schema.Count), "複製資料表: " + schema[i].TableName);
						_log.Debug("finished inserting all rows for table [" + schema[i].TableName + "]");
					}
					catch (Exception exception)
					{
						_log.Error("預期外的錯誤", exception);
						sQLiteTransaction.Rollback();
						throw;
					}
				}
			}
		}
	}

	private static object CastValueForColumn(object val, ColumnSchema columnSchema)
	{
		if (val is DBNull)
		{
			return null;
		}
		DbType dbTypeOfColumn = GetDbTypeOfColumn(columnSchema);
		switch (dbTypeOfColumn)
		{
		case DbType.Int32:
			if (val is short)
			{
				return (int)(short)val;
			}
			if (val is byte)
			{
				return (int)(byte)val;
			}
			if (val is long)
			{
				return (int)(long)val;
			}
			if (val is decimal)
			{
				return (int)(decimal)val;
			}
			break;
		case DbType.Int16:
			if (val is int)
			{
				return (short)(int)val;
			}
			if (val is byte)
			{
				return (short)(byte)val;
			}
			if (val is long)
			{
				return (short)(long)val;
			}
			if (val is decimal)
			{
				return (short)(decimal)val;
			}
			break;
		case DbType.Int64:
			if (val is int)
			{
				return (long)(int)val;
			}
			if (val is short)
			{
				return (long)(short)val;
			}
			if (val is byte)
			{
				return (long)(byte)val;
			}
			if (val is decimal)
			{
				return (long)(decimal)val;
			}
			break;
		case DbType.Single:
			if (val is double)
			{
				return (float)(double)val;
			}
			if (val is decimal)
			{
				return (float)(decimal)val;
			}
			break;
		case DbType.Double:
			if (val is float)
			{
				return (double)(float)val;
			}
			if (val is double)
			{
				return (double)val;
			}
			if (val is decimal)
			{
				return (double)(decimal)val;
			}
			break;
		case DbType.String:
			if (val is Guid)
			{
				return ((Guid)val).ToString();
			}
			break;
		case DbType.Guid:
			if (val is string)
			{
				return ParseStringAsGuid((string)val);
			}
			if (val is byte[])
			{
				return ParseBlobAsGuid((byte[])val);
			}
			break;
		default:
			_log.Error("argument exception - illegal database type");
			throw new ArgumentException("Illegal database type [" + Enum.GetName(typeof(DbType), dbTypeOfColumn) + "]");
		case DbType.Binary:
		case DbType.Boolean:
		case DbType.DateTime:
			break;
		}
		return val;
	}

	private static Guid ParseBlobAsGuid(byte[] blob)
	{
		byte[] array = blob;
		if (blob.Length > 16)
		{
			array = new byte[16];
			for (int i = 0; i < 16; i++)
			{
				array[i] = blob[i];
			}
		}
		else if (blob.Length < 16)
		{
			array = new byte[16];
			for (int j = 0; j < blob.Length; j++)
			{
				array[j] = blob[j];
			}
		}
		return new Guid(array);
	}

	private static Guid ParseStringAsGuid(string str)
	{
		try
		{
			return new Guid(str);
		}
		catch (Exception)
		{
			return Guid.Empty;
		}
	}

	private static SQLiteCommand BuildSQLiteInsert(TableSchema ts)
	{
		SQLiteCommand sQLiteCommand = new SQLiteCommand();
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.Append("INSERT INTO [" + ts.TableName + "] (");
		for (int i = 0; i < ts.Columns.Count; i++)
		{
			stringBuilder.Append("[" + ts.Columns[i].ColumnName + "]");
			if (i < ts.Columns.Count - 1)
			{
				stringBuilder.Append(", ");
			}
		}
		stringBuilder.Append(") VALUES (");
		List<string> list = new List<string>();
		for (int j = 0; j < ts.Columns.Count; j++)
		{
			string text = "@" + GetNormalizedName(ts.Columns[j].ColumnName, list);
			stringBuilder.Append(text);
			if (j < ts.Columns.Count - 1)
			{
				stringBuilder.Append(", ");
			}
			DbType dbTypeOfColumn = GetDbTypeOfColumn(ts.Columns[j]);
			SQLiteParameter parameter = new SQLiteParameter(text, dbTypeOfColumn, ts.Columns[j].ColumnName);
			sQLiteCommand.Parameters.Add(parameter);
			list.Add(text);
		}
		stringBuilder.Append(")");
		sQLiteCommand.CommandText = stringBuilder.ToString();
		sQLiteCommand.CommandType = CommandType.Text;
		return sQLiteCommand;
	}

	private static string GetNormalizedName(string str, List<string> names)
	{
		StringBuilder stringBuilder = new StringBuilder();
		for (int i = 0; i < str.Length; i++)
		{
			if (char.IsLetterOrDigit(str[i]) || str[i] == '_')
			{
				stringBuilder.Append(str[i]);
			}
			else
			{
				stringBuilder.Append("_");
			}
		}
		if (names.Contains(stringBuilder.ToString()))
		{
			return GetNormalizedName(stringBuilder.ToString() + "_", names);
		}
		return stringBuilder.ToString();
	}

	private static DbType GetDbTypeOfColumn(ColumnSchema cs)
	{
		if (cs.ColumnType == "tinyint")
		{
			return DbType.Byte;
		}
		if (cs.ColumnType == "int")
		{
			return DbType.Int32;
		}
		if (cs.ColumnType == "smallint")
		{
			return DbType.Int16;
		}
		if (cs.ColumnType == "bigint")
		{
			return DbType.Int64;
		}
		if (cs.ColumnType == "bit")
		{
			return DbType.Boolean;
		}
		if (cs.ColumnType == "nvarchar" || cs.ColumnType == "varchar" || cs.ColumnType == "text" || cs.ColumnType == "ntext")
		{
			return DbType.String;
		}
		if (cs.ColumnType == "float")
		{
			return DbType.Double;
		}
		if (cs.ColumnType == "real")
		{
			return DbType.Single;
		}
		if (cs.ColumnType == "blob")
		{
			return DbType.Binary;
		}
		if (cs.ColumnType == "numeric")
		{
			return DbType.Double;
		}
		if (cs.ColumnType == "timestamp" || cs.ColumnType == "datetime" || cs.ColumnType == "datetime2" || cs.ColumnType == "date" || cs.ColumnType == "time")
		{
			return DbType.DateTime;
		}
		if (cs.ColumnType == "nchar" || cs.ColumnType == "char")
		{
			return DbType.String;
		}
		if (cs.ColumnType == "uniqueidentifier" || cs.ColumnType == "guid")
		{
			return DbType.Guid;
		}
		if (cs.ColumnType == "xml")
		{
			return DbType.String;
		}
		if (cs.ColumnType == "sql_variant")
		{
			return DbType.Object;
		}
		if (cs.ColumnType == "integer")
		{
			return DbType.Int64;
		}
		_log.Error("illegal db type found");
		throw new ApplicationException("Illegal DB type found (" + cs.ColumnType + ")");
	}

	private static string BuildSqlServerTableQuery(TableSchema ts)
	{
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.Append("SELECT ");
		for (int i = 0; i < ts.Columns.Count; i++)
		{
			stringBuilder.Append("[" + ts.Columns[i].ColumnName + "]");
			if (i < ts.Columns.Count - 1)
			{
				stringBuilder.Append(", ");
			}
		}
		stringBuilder.Append(" FROM " + ts.TableSchemaName + ".[" + ts.TableName + "]");
		return stringBuilder.ToString();
	}

	private static void CreateSQLiteDatabase(string sqlitePath, DatabaseSchema schema, string password, SqlConversionHandler handler, FailedViewDefinitionHandler viewFailureHandler, bool createViews)
	{
		_log.Debug("Creating SQLite database...");
		SQLiteConnection.CreateFile(sqlitePath);
		_log.Debug("SQLite file was created successfully at [" + sqlitePath + "]");
		using (SQLiteConnection sQLiteConnection = new SQLiteConnection(CreateSQLiteConnectionString(sqlitePath, password)))
		{
			sQLiteConnection.Open();
			int num = 0;
			foreach (TableSchema table in schema.Tables)
			{
				try
				{
					AddSQLiteTable(sQLiteConnection, table);
				}
				catch (Exception exception)
				{
					_log.Error("AddSQLiteTable failed", exception);
					throw;
				}
				num++;
				CheckCancelled();
				handler(done: false, success: true, (int)((double)num * 50.0 / (double)schema.Tables.Count), "產生資料表: " + table.TableName);
				_log.Debug("added schema for SQLite table [" + table.TableName + "]");
			}
			num = 0;
			if (createViews)
			{
				foreach (ViewSchema view in schema.Views)
				{
					try
					{
						AddSQLiteView(sQLiteConnection, view, viewFailureHandler);
					}
					catch (Exception exception2)
					{
						_log.Error("AddSQLiteView failed", exception2);
						throw;
					}
					num++;
					CheckCancelled();
					handler(done: false, success: true, 50 + (int)((double)num * 50.0 / (double)schema.Views.Count), "產生view: " + view.ViewName);
					_log.Debug("added schema for SQLite view [" + view.ViewName + "]");
				}
			}
		}
		_log.Debug("finished adding all table/view schemas for SQLite database");
	}

	private static void AddSQLiteView(SQLiteConnection conn, ViewSchema vs, FailedViewDefinitionHandler handler)
	{
		string viewSQL = vs.ViewSQL;
		_log.Info("\n\n" + viewSQL + "\n\n");
		SQLiteTransaction sQLiteTransaction = conn.BeginTransaction();
		try
		{
			new SQLiteCommand(viewSQL, conn, sQLiteTransaction).ExecuteNonQuery();
			sQLiteTransaction.Commit();
		}
		catch (SQLiteException)
		{
			sQLiteTransaction.Rollback();
			if (handler == null)
			{
				throw;
			}
			ViewSchema viewSchema = new ViewSchema();
			viewSchema.ViewName = vs.ViewName;
			viewSchema.ViewSQL = vs.ViewSQL;
			string text = handler(viewSchema);
			if (text != null)
			{
				viewSchema.ViewSQL = text;
				AddSQLiteView(conn, viewSchema, handler);
			}
		}
	}

	private static void AddSQLiteTable(SQLiteConnection conn, TableSchema dt)
	{
		string text = BuildCreateTableQuery(dt);
		_log.Info("\n\n" + text + "\n\n");
		new SQLiteCommand(text, conn).ExecuteNonQuery();
	}

	private static string BuildCreateTableQuery(TableSchema ts)
	{
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.Append("CREATE TABLE [" + ts.TableName + "] (\n");
		bool pkey = false;
		for (int i = 0; i < ts.Columns.Count; i++)
		{
			string value = BuildColumnStatement(ts.Columns[i], ts, ref pkey);
			stringBuilder.Append(value);
			if (i < ts.Columns.Count - 1)
			{
				stringBuilder.Append(",\n");
			}
		}
		if (ts.PrimaryKey != null && ts.PrimaryKey.Count > 0 && !pkey)
		{
			stringBuilder.Append(",\n");
			stringBuilder.Append("    PRIMARY KEY (");
			for (int j = 0; j < ts.PrimaryKey.Count; j++)
			{
				stringBuilder.Append("[" + ts.PrimaryKey[j] + "]");
				if (j < ts.PrimaryKey.Count - 1)
				{
					stringBuilder.Append(", ");
				}
			}
			stringBuilder.Append(")\n");
		}
		else
		{
			stringBuilder.Append("\n");
		}
		if (ts.ForeignKeys.Count > 0)
		{
			stringBuilder.Append(",\n");
			for (int k = 0; k < ts.ForeignKeys.Count; k++)
			{
				ForeignKeySchema foreignKeySchema = ts.ForeignKeys[k];
				string value2 = $"    FOREIGN KEY ([{foreignKeySchema.ColumnName}])\n        REFERENCES [{foreignKeySchema.ForeignTableName}]([{foreignKeySchema.ForeignColumnName}])";
				stringBuilder.Append(value2);
				if (k < ts.ForeignKeys.Count - 1)
				{
					stringBuilder.Append(",\n");
				}
			}
		}
		stringBuilder.Append("\n");
		stringBuilder.Append(");\n");
		if (ts.Indexes != null)
		{
			for (int l = 0; l < ts.Indexes.Count; l++)
			{
				string str = BuildCreateIndex(ts.TableName, ts.Indexes[l]);
				stringBuilder.Append(str + ";\n");
			}
		}
		return stringBuilder.ToString();
	}

	private static string BuildCreateIndex(string tableName, IndexSchema indexSchema)
	{
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.Append("CREATE ");
		if (indexSchema.IsUnique)
		{
			stringBuilder.Append("UNIQUE ");
		}
		stringBuilder.Append("INDEX [" + tableName + "_" + indexSchema.IndexName + "]\n");
		stringBuilder.Append("ON [" + tableName + "]\n");
		stringBuilder.Append("(");
		for (int i = 0; i < indexSchema.Columns.Count; i++)
		{
			stringBuilder.Append("[" + indexSchema.Columns[i].ColumnName + "]");
			if (!indexSchema.Columns[i].IsAscending)
			{
				stringBuilder.Append(" DESC");
			}
			if (i < indexSchema.Columns.Count - 1)
			{
				stringBuilder.Append(", ");
			}
		}
		stringBuilder.Append(")");
		return stringBuilder.ToString();
	}

	private static string BuildColumnStatement(ColumnSchema col, TableSchema ts, ref bool pkey)
	{
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.Append("\t[" + col.ColumnName + "]\t");
		if (col.IsIdentity)
		{
			if (ts.PrimaryKey.Count == 1 && (col.ColumnType == "tinyint" || col.ColumnType == "int" || col.ColumnType == "smallint" || col.ColumnType == "bigint" || col.ColumnType == "integer"))
			{
				stringBuilder.Append("integer PRIMARY KEY AUTOINCREMENT");
				pkey = true;
			}
			else
			{
				stringBuilder.Append("integer");
			}
		}
		else
		{
			if (col.ColumnType == "int")
			{
				stringBuilder.Append("integer");
			}
			else
			{
				stringBuilder.Append(col.ColumnType);
			}
			if (col.Length > 0)
			{
				stringBuilder.Append("(" + col.Length + ")");
			}
		}
		if (!col.IsNullable)
		{
			stringBuilder.Append(" NOT NULL");
		}
		if (col.IsCaseSensitivite.HasValue && !col.IsCaseSensitivite.Value)
		{
			stringBuilder.Append(" COLLATE NOCASE");
		}
		string value = StripParens(col.DefaultValue);
		value = DiscardNational(value);
		_log.Debug("DEFAULT VALUE BEFORE [" + col.DefaultValue + "] AFTER [" + value + "]");
		if (value != string.Empty && value.ToUpper().Contains("GETDATE"))
		{
			_log.Debug("converted SQL Server GETDATE() to CURRENT_TIMESTAMP for column [" + col.ColumnName + "]");
			stringBuilder.Append(" DEFAULT (CURRENT_TIMESTAMP)");
		}
		else if (value != string.Empty && IsValidDefaultValue(value))
		{
			stringBuilder.Append(" DEFAULT " + value);
		}
		return stringBuilder.ToString();
	}

	private static string DiscardNational(string value)
	{
		Match match = new Regex("N\\'([^\\']*)\\'").Match(value);
		if (match.Success)
		{
			return match.Groups[1].Value;
		}
		return value;
	}

	private static bool IsValidDefaultValue(string value)
	{
		if (IsSingleQuoted(value))
		{
			return true;
		}
		if (!double.TryParse(value, out double _))
		{
			return false;
		}
		return true;
	}

	private static bool IsSingleQuoted(string value)
	{
		value = value.Trim();
		if (value.StartsWith("'") && value.EndsWith("'"))
		{
			return true;
		}
		return false;
	}

	private static string StripParens(string value)
	{
		Match match = new Regex("\\(([^\\)]*)\\)").Match(value);
		if (!match.Success)
		{
			return value;
		}
		return StripParens(match.Groups[1].Value);
	}

	private static DatabaseSchema ReadSqlServerSchema(string connString, SqlConversionHandler handler, SqlTableSelectionHandler selectionHandler)
	{
		List<TableSchema> list = new List<TableSchema>();
		using (SqlConnection sqlConnection = new SqlConnection(connString))
		{
			sqlConnection.Open();
			List<string> list2 = new List<string>();
			List<string> list3 = new List<string>();
			using (SqlDataReader sqlDataReader = new SqlCommand("select * from INFORMATION_SCHEMA.TABLES  where TABLE_TYPE = 'BASE TABLE'", sqlConnection).ExecuteReader())
			{
				while (sqlDataReader.Read())
				{
					if (sqlDataReader["TABLE_NAME"] != DBNull.Value && sqlDataReader["TABLE_SCHEMA"] != DBNull.Value)
					{
						list2.Add((string)sqlDataReader["TABLE_NAME"]);
						list3.Add((string)sqlDataReader["TABLE_SCHEMA"]);
					}
				}
			}
			int num = 0;
			for (int i = 0; i < list2.Count; i++)
			{
				string text = list2[i];
				string tschma = list3[i];
				TableSchema tableSchema = CreateTableSchema(sqlConnection, text, tschma);
				CreateForeignKeySchema(sqlConnection, tableSchema);
				list.Add(tableSchema);
				num++;
				CheckCancelled();
				handler(done: false, success: true, (int)((double)num * 50.0 / (double)list2.Count), "解析資料表: " + text);
				_log.Debug("parsed table schema for [" + text + "]");
			}
		}
		_log.Debug("finished parsing all tables in SQL Server schema");
		if (selectionHandler != null)
		{
			List<TableSchema> list4 = selectionHandler(list);
			if (list4 != null)
			{
				list = list4;
			}
		}
		Regex regex = new Regex("dbo\\.", RegexOptions.IgnoreCase | RegexOptions.Compiled);
		List<ViewSchema> list5 = new List<ViewSchema>();
		using (SqlConnection sqlConnection2 = new SqlConnection(connString))
		{
			sqlConnection2.Open();
			using (SqlDataReader sqlDataReader2 = new SqlCommand("SELECT TABLE_NAME, VIEW_DEFINITION  from INFORMATION_SCHEMA.VIEWS", sqlConnection2).ExecuteReader())
			{
				int num2 = 0;
				while (sqlDataReader2.Read())
				{
					ViewSchema viewSchema = new ViewSchema();
					if (sqlDataReader2["TABLE_NAME"] != DBNull.Value && sqlDataReader2["VIEW_DEFINITION"] != DBNull.Value)
					{
						viewSchema.ViewName = (string)sqlDataReader2["TABLE_NAME"];
						viewSchema.ViewSQL = (string)sqlDataReader2["VIEW_DEFINITION"];
						viewSchema.ViewSQL = regex.Replace(viewSchema.ViewSQL, string.Empty);
						list5.Add(viewSchema);
						num2++;
						CheckCancelled();
						handler(done: false, success: true, 50 + (int)((double)num2 * 50.0 / (double)list5.Count), "解析View: " + viewSchema.ViewName);
						_log.Debug("parsed view schema for [" + viewSchema.ViewName + "]");
					}
				}
			}
		}
		return new DatabaseSchema
		{
			Tables = list,
			Views = list5
		};
	}

	private static void CheckCancelled()
	{
		if (_cancelled)
		{
			throw new ApplicationException("User cancelled the conversion");
		}
	}

	private static TableSchema CreateTableSchema(SqlConnection conn, string tableName, string tschma)
	{
		TableSchema tableSchema = new TableSchema();
		tableSchema.TableName = tableName;
		tableSchema.TableSchemaName = tschma;
		tableSchema.Columns = new List<ColumnSchema>();
		using (SqlDataReader sqlDataReader = new SqlCommand("SELECT COLUMN_NAME,COLUMN_DEFAULT,IS_NULLABLE,DATA_TYPE,  (columnproperty(object_id(TABLE_NAME), COLUMN_NAME, 'IsIdentity')) AS [IDENT], CHARACTER_MAXIMUM_LENGTH AS CSIZE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tableName + "' ORDER BY ORDINAL_POSITION ASC", conn).ExecuteReader())
		{
			while (sqlDataReader.Read())
			{
				object obj = sqlDataReader["COLUMN_NAME"];
				if (!(obj is DBNull))
				{
					string columnName = (string)sqlDataReader["COLUMN_NAME"];
					obj = sqlDataReader["COLUMN_DEFAULT"];
					string text = (!(obj is DBNull)) ? ((string)obj) : string.Empty;
					obj = sqlDataReader["IS_NULLABLE"];
					bool isNullable = (string)obj == "YES";
					string text2 = (string)sqlDataReader["DATA_TYPE"];
					bool isIdentity = false;
					if (sqlDataReader["IDENT"] != DBNull.Value)
					{
						isIdentity = (((int)sqlDataReader["IDENT"] == 1) ? true : false);
					}
					int length = (sqlDataReader["CSIZE"] != DBNull.Value) ? Convert.ToInt32(sqlDataReader["CSIZE"]) : 0;
					ValidateDataType(text2);
					if (text2 == "timestamp")
					{
						text2 = "blob";
					}
					else
					{
						switch (text2)
						{
						case "datetime":
						case "smalldatetime":
						case "date":
						case "datetime2":
						case "time":
							text2 = "datetime";
							break;
						case "decimal":
							text2 = "numeric";
							break;
						case "money":
						case "smallmoney":
							text2 = "numeric";
							break;
						case "binary":
						case "varbinary":
						case "image":
							text2 = "blob";
							break;
						case "tinyint":
							text2 = "smallint";
							break;
						case "bigint":
							text2 = "integer";
							break;
						case "sql_variant":
							text2 = "blob";
							break;
						case "xml":
							text2 = "varchar";
							break;
						case "uniqueidentifier":
							text2 = "guid";
							break;
						case "ntext":
							text2 = "text";
							break;
						case "nchar":
							text2 = "char";
							break;
						}
					}
					if (text2 == "bit" || text2 == "int")
					{
						if (text == "('False')")
						{
							text = "(0)";
						}
						else if (text == "('True')")
						{
							text = "(1)";
						}
					}
					text = FixDefaultValueString(text);
					ColumnSchema columnSchema = new ColumnSchema();
					columnSchema.ColumnName = columnName;
					columnSchema.ColumnType = text2;
					columnSchema.Length = length;
					columnSchema.IsNullable = isNullable;
					columnSchema.IsIdentity = isIdentity;
					columnSchema.DefaultValue = AdjustDefaultValue(text);
					tableSchema.Columns.Add(columnSchema);
				}
			}
		}
		using (SqlDataReader sqlDataReader2 = new SqlCommand("EXEC sp_pkeys '" + tableName + "'", conn).ExecuteReader())
		{
			tableSchema.PrimaryKey = new List<string>();
			while (sqlDataReader2.Read())
			{
				string item = (string)sqlDataReader2["COLUMN_NAME"];
				tableSchema.PrimaryKey.Add(item);
			}
		}
		using (SqlDataReader sqlDataReader3 = new SqlCommand("EXEC sp_tablecollations '" + tschma + "." + tableName + "'", conn).ExecuteReader())
		{
			while (sqlDataReader3.Read())
			{
				bool? isCaseSensitivite = null;
				string b = (string)sqlDataReader3["name"];
				if (sqlDataReader3["tds_collation"] != DBNull.Value)
				{
					isCaseSensitivite = (((((byte[])sqlDataReader3["tds_collation"])[2] & 0x10) != 0) ? new bool?(false) : new bool?(true));
				}
				if (isCaseSensitivite.HasValue)
				{
					foreach (ColumnSchema column in tableSchema.Columns)
					{
						if (column.ColumnName == b)
						{
							column.IsCaseSensitivite = isCaseSensitivite;
							break;
						}
					}
				}
			}
		}
		try
		{
			using (SqlDataReader sqlDataReader4 = new SqlCommand("exec sp_helpindex '" + tschma + "." + tableName + "'", conn).ExecuteReader())
			{
				tableSchema.Indexes = new List<IndexSchema>();
				while (sqlDataReader4.Read())
				{
					string indexName = (string)sqlDataReader4["index_name"];
					string text3 = (string)sqlDataReader4["index_description"];
					string keys = (string)sqlDataReader4["index_keys"];
					if (!text3.Contains("primary key"))
					{
						IndexSchema item2 = BuildIndexSchema(indexName, text3, keys);
						tableSchema.Indexes.Add(item2);
					}
				}
				return tableSchema;
			}
		}
		catch (Exception)
		{
			_log.Warn("failed to read index information for table [" + tableName + "]");
			return tableSchema;
		}
	}

	private static void ValidateDataType(string dataType)
	{
		if (dataType == "int" || dataType == "smallint" || dataType == "bit" || dataType == "float" || dataType == "real" || dataType == "nvarchar" || dataType == "varchar" || dataType == "timestamp" || dataType == "varbinary" || dataType == "image" || dataType == "text" || dataType == "ntext" || dataType == "bigint" || dataType == "char" || dataType == "numeric" || dataType == "binary" || dataType == "smalldatetime" || dataType == "smallmoney" || dataType == "money" || dataType == "tinyint" || dataType == "uniqueidentifier" || dataType == "xml" || dataType == "sql_variant" || dataType == "datetime2" || dataType == "date" || dataType == "time" || dataType == "decimal" || dataType == "nchar" || dataType == "datetime")
		{
			return;
		}
		throw new ApplicationException("Validation failed for data type [" + dataType + "]");
	}

	private static string FixDefaultValueString(string colDefault)
	{
		bool flag = false;
		string text = colDefault.Trim();
		int num = -1;
		int num2 = -1;
		for (int i = 0; i < text.Length; i++)
		{
			if (text[i] == '\'' && num == -1)
			{
				num = i;
			}
			if (text[i] == '\'' && num != -1 && i > num2)
			{
				num2 = i;
			}
		}
		if (num != -1 && num2 > num)
		{
			return text.Substring(num, num2 - num + 1);
		}
		StringBuilder stringBuilder = new StringBuilder();
		for (int j = 0; j < text.Length; j++)
		{
			if (text[j] != '(' && text[j] != ')')
			{
				stringBuilder.Append(text[j]);
				flag = true;
			}
		}
		if (flag)
		{
			return "(" + stringBuilder.ToString() + ")";
		}
		return stringBuilder.ToString();
	}

	private static void CreateForeignKeySchema(SqlConnection conn, TableSchema ts)
	{
		ts.ForeignKeys = new List<ForeignKeySchema>();
		using (SqlDataReader sqlDataReader = new SqlCommand("SELECT   ColumnName = CU.COLUMN_NAME,   ForeignTableName  = PK.TABLE_NAME,   ForeignColumnName = PT.COLUMN_NAME,   DeleteRule = C.DELETE_RULE,   IsNullable = COL.IS_NULLABLE FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS C INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS FK ON C.CONSTRAINT_NAME = FK.CONSTRAINT_NAME INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK ON C.UNIQUE_CONSTRAINT_NAME = PK.CONSTRAINT_NAME INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE CU ON C.CONSTRAINT_NAME = CU.CONSTRAINT_NAME INNER JOIN   (     SELECT i1.TABLE_NAME, i2.COLUMN_NAME     FROM  INFORMATION_SCHEMA.TABLE_CONSTRAINTS i1     INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE i2 ON i1.CONSTRAINT_NAME = i2.CONSTRAINT_NAME     WHERE i1.CONSTRAINT_TYPE = 'PRIMARY KEY'   ) PT ON PT.TABLE_NAME = PK.TABLE_NAME INNER JOIN INFORMATION_SCHEMA.COLUMNS AS COL ON CU.COLUMN_NAME = COL.COLUMN_NAME AND FK.TABLE_NAME = COL.TABLE_NAME WHERE FK.Table_NAME='" + ts.TableName + "'", conn).ExecuteReader())
		{
			while (sqlDataReader.Read())
			{
				ForeignKeySchema foreignKeySchema = new ForeignKeySchema();
				foreignKeySchema.ColumnName = (string)sqlDataReader["ColumnName"];
				foreignKeySchema.ForeignTableName = (string)sqlDataReader["ForeignTableName"];
				foreignKeySchema.ForeignColumnName = (string)sqlDataReader["ForeignColumnName"];
				foreignKeySchema.CascadeOnDelete = ((string)sqlDataReader["DeleteRule"] == "CASCADE");
				foreignKeySchema.IsNullable = ((string)sqlDataReader["IsNullable"] == "YES");
				foreignKeySchema.TableName = ts.TableName;
				ts.ForeignKeys.Add(foreignKeySchema);
			}
		}
	}

	private static IndexSchema BuildIndexSchema(string indexName, string desc, string keys)
	{
		IndexSchema indexSchema = new IndexSchema();
		indexSchema.IndexName = indexName;
		string[] array = desc.Split(',');
		for (int i = 0; i < array.Length; i++)
		{
			if (array[i].Trim().Contains("unique"))
			{
				indexSchema.IsUnique = true;
				break;
			}
		}
		indexSchema.Columns = new List<IndexColumn>();
		array = keys.Split(',');
		foreach (string text in array)
		{
			Match match = _keyRx.Match(text.Trim());
			if (!match.Success)
			{
				throw new ApplicationException("Illegal key name [" + text + "] in index [" + indexName + "]");
			}
			string value = match.Groups[1].Value;
			IndexColumn indexColumn = new IndexColumn();
			indexColumn.ColumnName = value;
			if (match.Groups[2].Success)
			{
				indexColumn.IsAscending = false;
			}
			else
			{
				indexColumn.IsAscending = true;
			}
			indexSchema.Columns.Add(indexColumn);
		}
		return indexSchema;
	}

	private static string AdjustDefaultValue(string val)
	{
		if (val == null || val == string.Empty)
		{
			return val;
		}
		Match match = _defaultValueRx.Match(val);
		if (match.Success)
		{
			return match.Groups[1].Value;
		}
		return val;
	}

	private static string CreateSQLiteConnectionString(string sqlitePath, string password)
	{
		SQLiteConnectionStringBuilder sQLiteConnectionStringBuilder = new SQLiteConnectionStringBuilder();
		sQLiteConnectionStringBuilder.DataSource = sqlitePath;
		if (password != null)
		{
			sQLiteConnectionStringBuilder.Password = password;
		}
		sQLiteConnectionStringBuilder.PageSize = 4096;
		sQLiteConnectionStringBuilder.UseUTF16Encoding = true;
		return sQLiteConnectionStringBuilder.ConnectionString;
	}

	private static void AddTriggersForForeignKeys(string sqlitePath, IEnumerable<TableSchema> schema, string password, SqlConversionHandler handler)
	{
		using (SQLiteConnection sQLiteConnection = new SQLiteConnection(CreateSQLiteConnectionString(sqlitePath, password)))
		{
			sQLiteConnection.Open();
			foreach (TableSchema item in schema)
			{
				try
				{
					AddTableTriggers(sQLiteConnection, item);
				}
				catch (Exception exception)
				{
					_log.Error("AddTableTriggers failed", exception);
					throw;
				}
			}
		}
		_log.Debug("finished adding triggers to schema");
	}

	private static void AddTableTriggers(SQLiteConnection conn, TableSchema dt)
	{
		foreach (TriggerSchema foreignKeyTrigger in TriggerBuilder.GetForeignKeyTriggers(dt))
		{
			new SQLiteCommand(WriteTriggerSchema(foreignKeyTrigger), conn).ExecuteNonQuery();
		}
	}

	public static string WriteTriggerSchema(TriggerSchema ts)
	{
		return string.Concat("CREATE TRIGGER [", ts.Name, "] ", ts.Type, " ", ts.Event, " ON [", ts.Table, "] BEGIN ", ts.Body, " END;");
	}
}