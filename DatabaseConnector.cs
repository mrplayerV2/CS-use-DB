public class DatabaseConnector
{
	public readonly DatabaseType databaseType;

	public readonly string source;

	public readonly string account = "";

	private readonly string password = "";

	private bool _connected;

	public string dbName;

	private OleDbConnection AccessConnection;

	public bool connected => _connected;

	public DatabaseConnector(DatabaseType databaseType, string source, string account, string password, string dbName)
	{
		this.databaseType = databaseType;
		this.source = source;
		this.account = account;
		this.password = password;
	}

	public DatabaseConnector(DatabaseType databaseType, string source)
	{
		this.databaseType = databaseType;
		this.source = source;
	}

	public bool connect()
	{
		switch (databaseType)
		{
		case DatabaseType.ACCESS:
			return connect_Access();
		case DatabaseType.EXCEL:
			return connect_Excel();
		case DatabaseType.SQLSERVER:
			return connect_SQLServer();
		case DatabaseType.SQLITE:
			return connect_SQLite();
		default:
			return false;
		}
	}

	public bool close()
	{
		switch (databaseType)
		{
		case DatabaseType.ACCESS:
			return close_Access();
		case DatabaseType.EXCEL:
			return close_Excel();
		case DatabaseType.SQLSERVER:
			return close_SQLServer();
		case DatabaseType.SQLITE:
			return close_SQLite();
		default:
			return false;
		}
	}

	private bool close_SQLite()
	{
		throw new NotImplementedException();
	}

	private bool close_SQLServer()
	{
		throw new NotImplementedException();
	}

	private bool close_Excel()
	{
		throw new NotImplementedException();
	}

	private bool close_Access()
	{
		try
		{
			AccessConnection.Close();
			_connected = false;
			return true;
		}
		catch (Exception)
		{
			return false;
		}
	}

	private bool connect_Access()
	{
		string strConn = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + source + ";Jet OLEDB:Database Password=hywebhdlw";
		AccessConnection = new OleDbConnection(strConn);
		try
		{
			AccessConnection.Open();
			_connected = true;
			return true;
		}
		catch (Exception)
		{
			return false;
		}
	}

	private bool connect_Excel()
	{
		throw new NotImplementedException();
	}

	private bool connect_SQLServer()
	{
		throw new NotImplementedException();
	}

	private bool connect_SQLite()
	{
		throw new NotImplementedException();
	}

	public QueryResult executeQuery(string cmdStr)
	{
		switch (databaseType)
		{
		case DatabaseType.ACCESS:
			return query_Access(cmdStr);
		case DatabaseType.EXCEL:
			return null;
		case DatabaseType.SQLSERVER:
			return null;
		case DatabaseType.SQLITE:
			return null;
		default:
			return null;
		}
	}

	public int executeNonQuery(string cmdStr)
	{
		switch (databaseType)
		{
		case DatabaseType.ACCESS:
			return nonQuery_Access(cmdStr);
		case DatabaseType.EXCEL:
			return 0;
		case DatabaseType.SQLSERVER:
			return 0;
		case DatabaseType.SQLITE:
			return 0;
		default:
			return 0;
		}
	}

	public int executeNonQuery(List<string> cmdStrs)
	{
		switch (databaseType)
		{
		case DatabaseType.ACCESS:
			return nonQuery_Access(cmdStrs);
		case DatabaseType.EXCEL:
			return 0;
		case DatabaseType.SQLSERVER:
			return 0;
		case DatabaseType.SQLITE:
			return 0;
		default:
			return 0;
		}
	}

	private int nonQuery_Access(List<string> cmdStrs)
	{
		if (!connected || databaseType != 0)
		{
			return 0;
		}
		using (OleDbCommand cmd = new OleDbCommand())
		{
			cmd.Connection = AccessConnection;
			using (List<string>.Enumerator enumerator = cmdStrs.GetEnumerator())
			{
				while (enumerator.MoveNext())
				{
					string cmdStr = cmd.CommandText = enumerator.Current;
					try
					{
						cmd.ExecuteNonQuery();
					}
					catch (Exception)
					{
					}
				}
			}
		}
		return 1;
	}

	private int nonQuery_Access(string cmdStr)
	{
		if (!connected || databaseType != 0)
		{
			return 0;
		}
		using (OleDbCommand cmd = new OleDbCommand())
		{
			cmd.Connection = AccessConnection;
			cmd.CommandText = cmdStr;
			try
			{
				return cmd.ExecuteNonQuery();
			}
			catch
			{
				return 0;
			}
		}
	}

	private QueryResult query_Access(string cmdStr)
	{
		if (!connected || databaseType != 0)
		{
			return null;
		}
		using (OleDbCommand cmd = new OleDbCommand())
		{
			cmd.Connection = AccessConnection;
			cmd.CommandText = cmdStr;
			try
			{
				OleDbDataReader reader = cmd.ExecuteReader();
				return new QueryResult(ref reader);
			}
			catch (Exception)
			{
				return null;
			}
		}
	}
}

public enum DatabaseType
{
	ACCESS,
	EXCEL,
	SQLSERVER,
	SQLITE
}
