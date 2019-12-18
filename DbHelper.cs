using System;
using System.Configuration;
using System.Data;
using System.Data.Common;
using DBAccess;

namespace DBAccess
{

 

    public class DbHelper
    {

        private static string dbProviderName = "System.Data.SqlClient";//ConfigurationManager.AppSettings["DbHelperProvider"];

        //private static string dbConnectionString = ConfigurationManager.ConnectionStrings["DSMS"].ToString();
        private static string dbConnectionString = ConfigurationManager.ConnectionStrings["DSMS"].ConnectionString;

        private DbConnection connection;

        public DbHelper()
        {

            this.connection = CreateConnection(dbConnectionString);

        }

        public DbHelper(string connectionString)
        {

            this.connection = CreateConnection(connectionString);

        }

        public static DbConnection CreateConnection()
        {

            DbProviderFactory dbfactory = DbProviderFactories.GetFactory(dbProviderName);

            DbConnection dbconn = dbfactory.CreateConnection();

            dbconn.ConnectionString = dbConnectionString;

            return dbconn;

        }

        public static DbConnection CreateConnection(string connectionString)
        {

            DbProviderFactory dbfactory = DbProviderFactories.GetFactory(dbProviderName);

            DbConnection dbconn = dbfactory.CreateConnection();

            dbconn.ConnectionString = connectionString;

            return dbconn;

        }

 

        public DbCommand GetStoredProcCommond(string storedProcedure)
        {

            DbCommand dbCommand = connection.CreateCommand();

            dbCommand.CommandText = storedProcedure;

            dbCommand.CommandType = System.Data.CommandType.StoredProcedure;

            return dbCommand;

        }

        public DbCommand GetSqlStringCommond(string sqlQuery)
        {

            DbCommand dbCommand = connection.CreateCommand();

            dbCommand.CommandText = sqlQuery;

            dbCommand.CommandType = System.Data.CommandType.Text;

            return dbCommand;

        }

 

        public void AddParameterCollection(DbCommand cmd, DbParameterCollection dbParameterCollection)
        {

            foreach (DbParameter dbParameter in dbParameterCollection)
            {

                cmd.Parameters.Add(dbParameter);

            }

        }

        public void AddOutParameter(DbCommand cmd, string parameterName, DbType dbType, int size)
        {

            DbParameter dbParameter = cmd.CreateParameter();

            dbParameter.DbType = dbType;

            dbParameter.ParameterName = parameterName;

            dbParameter.Size = size;

            dbParameter.Direction = ParameterDirection.Output;

            cmd.Parameters.Add(dbParameter);

        }

        public void AddInParameter(DbCommand cmd, string parameterName, DbType dbType, object value)
        {

            DbParameter dbParameter = cmd.CreateParameter();

            dbParameter.DbType = dbType;

            dbParameter.ParameterName = parameterName;

            dbParameter.Value = value;

            dbParameter.Direction = ParameterDirection.Input;

            cmd.Parameters.Add(dbParameter);

        }

        public void AddReturnParameter(DbCommand cmd, string parameterName, DbType dbType)
        {

            DbParameter dbParameter = cmd.CreateParameter();

            dbParameter.DbType = dbType;

            dbParameter.ParameterName = parameterName;

            dbParameter.Direction = ParameterDirection.ReturnValue;

            cmd.Parameters.Add(dbParameter);

        }

        public DbParameter GetParameter(DbCommand cmd, string parameterName)
        {

            return cmd.Parameters[parameterName];

        }

 

 

        public DataSet ExecuteDataSet(DbCommand cmd)
        {

            DbProviderFactory dbfactory = DbProviderFactories.GetFactory(dbProviderName);

            DbDataAdapter dbDataAdapter = dbfactory.CreateDataAdapter();

            dbDataAdapter.SelectCommand = cmd;

            DataSet ds = new DataSet();

            dbDataAdapter.Fill(ds);

            return ds;

        }

 

        public DataTable ExecuteDataTable(DbCommand cmd)
        {

            DbProviderFactory dbfactory = DbProviderFactories.GetFactory(dbProviderName);

            DbDataAdapter dbDataAdapter = dbfactory.CreateDataAdapter();

            dbDataAdapter.SelectCommand = cmd;

            DataTable dataTable = new DataTable();

            dbDataAdapter.Fill(dataTable);

            return dataTable;

        }

 

        public DbDataReader ExecuteReader(DbCommand cmd)
        {

            cmd.Connection.Open();

            DbDataReader reader = cmd.ExecuteReader(CommandBehavior.CloseConnection);

            return reader;

        }

        public int ExecuteNonQuery(DbCommand cmd)
        {

            cmd.Connection.Open();

            int ret = cmd.ExecuteNonQuery();

            cmd.Connection.Close();

            return ret;

        }

 

        public object ExecuteScalar(DbCommand cmd)
        {

            cmd.Connection.Open();

            object ret = cmd.ExecuteScalar();

            cmd.Connection.Close();

            return ret;

        }

 

 

        public DataSet ExecuteDataSet(DbCommand cmd, Trans t)
        {

            cmd.Connection = t.DbConnection;

            cmd.Transaction = t.DbTrans;

            DbProviderFactory dbfactory = DbProviderFactories.GetFactory(dbProviderName);

            DbDataAdapter dbDataAdapter = dbfactory.CreateDataAdapter();

            dbDataAdapter.SelectCommand = cmd;

            DataSet ds = new DataSet();

            dbDataAdapter.Fill(ds);

            return ds;

        }

 

        public DataTable ExecuteDataTable(DbCommand cmd, Trans t)
        {

            cmd.Connection = t.DbConnection;

            cmd.Transaction = t.DbTrans;

            DbProviderFactory dbfactory = DbProviderFactories.GetFactory(dbProviderName);

            DbDataAdapter dbDataAdapter = dbfactory.CreateDataAdapter();

            dbDataAdapter.SelectCommand = cmd;

            DataTable dataTable = new DataTable();

            dbDataAdapter.Fill(dataTable);

            return dataTable;

        }

 

        public DbDataReader ExecuteReader(DbCommand cmd, Trans t)
        {

            cmd.Connection = t.DbConnection;

            cmd.Transaction = t.DbTrans;

            DbDataReader reader = cmd.ExecuteReader();

            DataTable dt = new DataTable();

            return reader;

        }

        public int ExecuteNonQuery(DbCommand cmd, Trans t)
        {

            cmd.Connection = t.DbConnection;

            cmd.Transaction = t.DbTrans;

            int ret = cmd.ExecuteNonQuery();

            return ret;

        }

 

        public object ExecuteScalar(DbCommand cmd, Trans t)
        {

            cmd.Connection = t.DbConnection;

            cmd.Transaction = t.DbTrans;

            object ret = cmd.ExecuteScalar();

            return ret;

        }

 

    }

 

    public class Trans : IDisposable
    {

        private DbConnection conn;

        private DbTransaction dbTrans;

        public DbConnection DbConnection
        {

            get { return this.conn; }

        }

        public DbTransaction DbTrans
        {

            get { return this.dbTrans; }

        }

 

 

        /*

         * Chaos              The pending changes from more highly isolated transactions cannot be overwritten.

              ReadCommitted    Volatile data cannot be read during the transaction, but can be modified.

              ReadUncommitted Volatile data can be read and modified during the transaction.

              RepeatableRead   Volatile data can be read but not modified during the transaction. New data can be added during the transaction.

              Serializable           Volatile data can be read but not modified, and no new data can be added during the transaction.

              Snapshot              Volatile data can be read. Before a transaction modifies data, it verifies if another transaction has changed the data after it was initially read. If the data has been updated, an error is raised. This allows a transaction to get to the previously committed value of the data.

 

When you try to promote a transaction that was created with this isolation level, an InvalidOperationException is thrown with the error message "Transactions with IsolationLevel Snapshot cannot be promoted".

              Unspecified          A different isolation level than the one specified is being used, but the level cannot be determined. An exception is thrown if this value is set.

     */

        public Trans(IsolationLevel isolationLevel)
        {

            conn = DbHelper.CreateConnection();

            conn.Open();

 

            dbTrans = conn.BeginTransaction(isolationLevel);

        }

 

        public Trans()
        {

            conn = DbHelper.CreateConnection();

            conn.Open();

 

            dbTrans = conn.BeginTransaction();

        }

 

        public Trans(string connectionString)
        {

            conn = DbHelper.CreateConnection(connectionString);

            conn.Open();

            dbTrans = conn.BeginTransaction();

        }

        public void Commit()
        {

            dbTrans.Commit();

            this.Colse();

        }

 

        public void RollBack()
        {

            dbTrans.Rollback();

            this.Colse();

        }

 

        public void Dispose()
        {

            this.Colse();

        }

 

        public void Colse()
        {

            if (conn.State == ConnectionState.Open)
            {

                conn.Close();

            }

        }

    }

}

 

 

/*

 



    DbHelper db = new DbHelper();

    DbCommand cmd = db.GetSqlStringCommond("insert t1 (id)values('haha')");

    db.ExecuteNonQuery(cmd);

 



    DbHelper db = new DbHelper();

    DbCommand cmd = db.GetStoredProcCommond("t1_insert");

    db.AddInParameter(cmd, "@id", DbType.String, "heihei");

    db.ExecuteNonQuery(cmd);

 



    DbHelper db = new DbHelper();

    DbCommand cmd = db.GetSqlStringCommond("select * from t1");

    DataSet ds = db.ExecuteDataSet(cmd);

 



    DbHelper db = new DbHelper();

    DbCommand cmd = db.GetSqlStringCommond("t1_findall");

    DataTable dt = db.ExecuteDataTable(cmd);

 



    DbHelper db = new DbHelper();

    DbCommand cmd = db.GetStoredProcCommond("t2_insert");

    db.AddInParameter(cmd, "@timeticks", DbType.Int64, DateTime.Now.Ticks);

    db.AddOutParameter(cmd, "@outString", DbType.String, 20);

    db.AddReturnParameter(cmd, "@returnValue", DbType.Int32);

 

    db.ExecuteNonQuery(cmd);

 

    string s = db.GetParameter(cmd, "@outString").Value as string;//out parameter

    int r = Convert.ToInt32(db.GetParameter(cmd, "@returnValue").Value);//return value

 



  DbHelper db = new DbHelper();

    DbCommand cmd = db.GetStoredProcCommond("t2_insert");

    db.AddInParameter(cmd, "@timeticks", DbType.Int64, DateTime.Now.Ticks);

    db.AddOutParameter(cmd, "@outString", DbType.String, 20);

    db.AddReturnParameter(cmd, "@returnValue", DbType.Int32);

 

    using (DbDataReader reader = db.ExecuteReader(cmd))

    {

        dt.Load(reader);

    }       

    string s = db.GetParameter(cmd, "@outString").Value as string;//out parameter

    int r = Convert.ToInt32(db.GetParameter(cmd, "@returnValue").Value);//return value

 



pubic void DoBusiness()

{

    using (Trans t = new Trans())

    {

        try

        {

            D1(t);

            throw new Exception();

            D2(t);

            t.Commit();

        }

        catch

        {

            t.RollBack();

        }

    }

}

public void D1(Trans t)

{

    DbHelper db = new DbHelper();

    DbCommand cmd = db.GetStoredProcCommond("t2_insert");

    db.AddInParameter(cmd, "@timeticks", DbType.Int64, DateTime.Now.Ticks);

    db.AddOutParameter(cmd, "@outString", DbType.String, 20);

    db.AddReturnParameter(cmd, "@returnValue", DbType.Int32);

 

    if (t == null) db.ExecuteNonQuery(cmd);

    else db.ExecuteNonQuery(cmd,t);

 

    string s = db.GetParameter(cmd, "@outString").Value as string;//out parameter

    int r = Convert.ToInt32(db.GetParameter(cmd, "@returnValue").Value);//return value

}

public void D2(Trans t)

{

    DbHelper db = new DbHelper();

    DbCommand cmd = db.GetSqlStringCommond("insert t1 (id)values('..')");       

    if (t == null) db.ExecuteNonQuery(cmd);

    else db.ExecuteNonQuery(cmd, t);

}

}

*/