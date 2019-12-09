using System;
using System.Data;
using System.Data.SQLite;
using System.Text;


public enum CommandOperationType
{
	ExecuteNonQuery,
	ExecuteReader,
	ExecuteScalar,
	ExecuteReaderReturnDataTable
}

public enum TableOperation
{
	Select,
	Insert,
	Delete,
	Update
}


public class DataBaseUtilities
{
	public static object DBOperation(string ConnectionString, TableOperation tblOperation, string strSelectField, string strTableName, string strWhereClause, string strOrderClause, string[,] strFieldArray, string[] strWhereParameterArray, CommandOperationType cmdType)
	{
		string text = "";
		string text2 = "";
		string text3 = "";
		string[] array = null;
		int num = (strWhereParameterArray != null && strWhereParameterArray.Length != 0) ? strWhereParameterArray.Length : 0;
		if (strWhereClause != "" && num > 0)
		{
			array = new string[num];
			for (int i = 0; i < array.Length; i++)
			{
				array[i] = "@where_" + i;
			}
			strWhereClause = string.Format(strWhereClause, array);
		}
		int num2 = (strFieldArray != null && strFieldArray.Rank != 0) ? (strFieldArray.Length / strFieldArray.Rank) : 0;
		if (tblOperation == TableOperation.Update && strFieldArray.Length > 0)
		{
			for (int j = 0; j < num2; j++)
			{
				text = text + ((j > 0) ? ", " : "") + strFieldArray[j, 0] + "=" + ((strFieldArray[j, 1] == "NULL") ? "NULL" : ("@setOrValue_" + j + "_" + strFieldArray[j, 0]));
			}
		}
		else if (tblOperation == TableOperation.Insert && strFieldArray.Length > 0)
		{
			for (int k = 0; k < num2; k++)
			{
				text2 = text2 + ((k > 0) ? ", " : "") + strFieldArray[k, 0];
				text3 = text3 + ((k > 0) ? ", " : "") + ((strFieldArray[k, 1] == "NULL") ? "NULL" : ("@setOrValue_" + k + "_" + strFieldArray[k, 0]));
			}
		}
		string commandText;
		switch (tblOperation)
		{
		case TableOperation.Select:
			commandText = "SELECT " + strSelectField + " FROM " + strTableName + ((strWhereClause == "") ? "" : (" WHERE " + strWhereClause)) + ((strOrderClause == "") ? "" : (" ORDER BY " + strOrderClause));
			break;
		case TableOperation.Delete:
			commandText = "DELETE FROM " + strTableName + ((strWhereClause == "") ? "" : (" WHERE " + strWhereClause));
			break;
		case TableOperation.Update:
			commandText = "UPDATE " + strTableName + " SET " + text + ((strWhereClause == "") ? "" : (" WHERE " + strWhereClause));
			break;
		case TableOperation.Insert:
			commandText = "INSERT INTO " + strTableName + "(" + text2 + ")VALUES (" + text3 + ")";
			break;
		default:
			commandText = "";
			break;
		}
		using (SQLiteConnection sQLiteConnection = new SQLiteConnection(ConnectionString))
		{
			using (SQLiteCommand sQLiteCommand = new SQLiteCommand(commandText, sQLiteConnection))
			{
				for (int l = 0; l < num2; l++)
				{
					if (strFieldArray[l, 1] != "NULL")
					{
						sQLiteCommand.Parameters.AddWithValue("@setOrValue_" + l + "_" + strFieldArray[l, 0], strFieldArray[l, 1]);
					}
				}
				for (int m = 0; m < num; m++)
				{
					sQLiteCommand.Parameters.AddWithValue(array[m], strWhereParameterArray[m]);
				}
				try
				{
					sQLiteCommand.CommandTimeout = 0;
					sQLiteConnection.Open();
					object obj;
					switch (cmdType)
					{
					case CommandOperationType.ExecuteNonQuery:
						obj = sQLiteCommand.ExecuteNonQuery();
						break;
					case CommandOperationType.ExecuteScalar:
						obj = sQLiteCommand.ExecuteScalar();
						break;
					case CommandOperationType.ExecuteReader:
						obj = sQLiteCommand.ExecuteReader();
						break;
					case CommandOperationType.ExecuteReaderReturnDataTable:
						obj = sQLiteCommand.ExecuteReader();
						if (obj != null)
						{
							DataTable dataTable = new DataTable();
							dataTable.Load((SQLiteDataReader)obj);
							return dataTable;
						}
						return null;
					default:
						obj = null;
						break;
					}
					if (obj != null)
					{
						return obj;
					}
					return -1;
				}
				catch (Exception)
				{
					return -1;
				}
			}
		}
	}

	public static object DBOperation(string ConnectionString, string sql, string[] strParameterArray, CommandOperationType cmdType)
	{
		string[] array = null;
		int num = (strParameterArray != null && strParameterArray.Length != 0) ? strParameterArray.Length : 0;
		if (sql != "" && num > 0)
		{
			array = new string[num];
			for (int i = 0; i < array.Length; i++)
			{
				array[i] = "@param_" + i;
			}
			sql = string.Format(sql, array);
		}
		using (SQLiteConnection sQLiteConnection = new SQLiteConnection(ConnectionString))
		{
			using (SQLiteCommand sQLiteCommand = new SQLiteCommand(sql, sQLiteConnection))
			{
				for (int j = 0; j < num; j++)
				{
					sQLiteCommand.Parameters.AddWithValue(array[j], strParameterArray[j]);
				}
				try
				{
					sQLiteCommand.CommandTimeout = 0;
					sQLiteConnection.Open();
					object obj;
					switch (cmdType)
					{
					case CommandOperationType.ExecuteNonQuery:
						obj = sQLiteCommand.ExecuteNonQuery();
						break;
					case CommandOperationType.ExecuteScalar:
						obj = sQLiteCommand.ExecuteScalar();
						break;
					case CommandOperationType.ExecuteReader:
						obj = sQLiteCommand.ExecuteReader();
						break;
					case CommandOperationType.ExecuteReaderReturnDataTable:
						obj = sQLiteCommand.ExecuteReader();
						if (obj != null)
						{
							DataTable dataTable = new DataTable();
							dataTable.Load((SQLiteDataReader)obj);
							return dataTable;
						}
						return null;
					default:
						obj = null;
						break;
					}
					if (obj != null)
					{
						return obj;
					}
					return -1;
				}
				catch (Exception)
				{
					return -1;
				}
			}
		}
	}

	public static string dataTabletoHTMLTable(DataTable dt)
	{
		if (dt == null)
		{
			return "";
		}
		StringBuilder stringBuilder = new StringBuilder("<table><tr>");
		foreach (DataColumn column2 in dt.Columns)
		{
			stringBuilder.AppendFormat("<td>{0}</td>", column2.Caption);
		}
		stringBuilder.Append("</tr>");
		foreach (DataRow row in dt.Rows)
		{
			stringBuilder.Append("<tr>");
			foreach (DataColumn column3 in dt.Columns)
			{
				stringBuilder.AppendFormat("<td>{0}</td>", row[column3].ToString());
			}
			stringBuilder.Append("</tr>");
		}
		stringBuilder.Append("</table>");
		return stringBuilder.ToString();
	}
}
