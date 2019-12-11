using System;
using System.Data.OleDb;

public class QueryResult
{
	private OleDbDataReader reader;

	public QueryResult(ref OleDbDataReader reader)
	{
		this.reader = reader;
	}

	public bool fetchRow()
	{
		if (reader.Read())
		{
			return true;
		}
		return false;
	}

	public string getString(string fieldName)
	{
		return reader[fieldName].ToString();
	}

	public string getString(int index)
	{
		return reader[index].ToString();
	}

	public long getLong(string fieldName)
	{
		try
		{
			return Convert.ToInt64(reader[fieldName].ToString());
		}
		catch (Exception ex)
		{
			throw ex;
		}
	}

	public long getLong(int index)
	{
		try
		{
			return Convert.ToInt64(reader[index].ToString());
		}
		catch (Exception ex)
		{
			throw ex;
		}
	}

	public int getInt(string fieldName)
	{
		int value = 1;
		try
		{
			int.TryParse(reader[fieldName].ToString(), out value);
			return value;
		}
		catch (Exception ex)
		{
			throw ex;
		}
	}

	public int getInt(int index)
	{
		try
		{
			return Convert.ToInt32(reader[index].ToString());
		}
		catch (Exception ex)
		{
			throw ex;
		}
	}

	public float getFloat(string fieldName)
	{
		try
		{
			return (float)Convert.ToDouble(reader[fieldName].ToString());
		}
		catch (Exception ex)
		{
			throw ex;
		}
	}

	public float getFloat(int index)
	{
		try
		{
			return (float)Convert.ToDouble(reader[index].ToString());
		}
		catch (Exception ex)
		{
			throw ex;
		}
	}
}
