using System;
using System.Collections;
using System.Data;
using System.Text;
using System.IO;
using System.Data.SqlClient;
using NSDBUtil;
using System.Configuration;
namespace NSDBUtil
{
    public enum TblOpType { Select, Insert, Delete, Update };
    public enum CmdOpType { ExecuteNonQuery, ExecuteReader, ExecuteScalar, ExecuteReaderReturnDataTable, ExecuteReaderReturnDataTableCollection };
}
public class DBUtil
{
    public static object DBOp(string ConnString, TblOpType tblOperation, string strSelectField, string strTableName, string strWhereClause, string strOrderClause, string[,] strFieldArray, string[] strWhereParameterArray, CmdOpType cmdType){
        string ConnectionString=  ConfigurationManager.ConnectionStrings[ConnString].ToString();
        string strCommandText,
               strSetOperation = "",
               strInsertField = "",
               strValues = "";

        string[] whereParamName = null;
        int intWhereArrayRows = (strWhereParameterArray != null && strWhereParameterArray.Length != 0 ? strWhereParameterArray.Length : 0);
        if (strWhereClause != "" && intWhereArrayRows > 0) {
            whereParamName = new string[intWhereArrayRows];
            for (int i = 0; i < whereParamName.Length; ++i)
				whereParamName[i] = "@where_" + i;
            strWhereClause = string.Format(strWhereClause, whereParamName);
        }
        int intFieldArrayRows = (strFieldArray != null && strFieldArray.Rank != 0 ? strFieldArray.Length / strFieldArray.Rank : 0);
        if (tblOperation == TblOpType.Update && strFieldArray.Length > 0){
            for (int i = 0; i < intFieldArrayRows; ++i)
                strSetOperation += (i > 0 ? ", " : "") + strFieldArray[i, 0] + "=" + (strFieldArray[i, 1] == "NULL" ? "NULL" : "@setOrValue_" + i + "_" + strFieldArray[i, 0]);
        }
        else if (tblOperation == TblOpType.Insert && strFieldArray.Length > 0)
        {
            for (int i = 0; i < intFieldArrayRows; ++i)
            {
                strInsertField += (i > 0 ? ", " : "") + strFieldArray[i, 0];
                strValues += (i > 0 ? ", " : "") + (strFieldArray[i, 1] == "NULL" ? "NULL" : "@setOrValue_" + i + "_" + strFieldArray[i, 0]);
            }
        }

        switch (tblOperation)
        {
            case TblOpType.Select:
                strCommandText = "SELECT " + strSelectField + " FROM " + strTableName + (strWhereClause == "" ? "" : " WHERE " + strWhereClause) + (strOrderClause == "" ? "" : " ORDER BY " + strOrderClause);
                break;
            case TblOpType.Delete:
                strCommandText = "DELETE FROM " + strTableName + (strWhereClause == "" ? "" : " WHERE " + strWhereClause);
                break;
            case TblOpType.Update:
                strCommandText = "UPDATE " + strTableName + " SET " + strSetOperation + (strWhereClause == "" ? "" : " WHERE " + strWhereClause);
                break;
            case TblOpType.Insert:
                strCommandText = "INSERT INTO " + strTableName + "(" + strInsertField + ")VALUES (" + strValues + ")";
                break;
            default:
                strCommandText = "";
                break;
        }

        using (SqlConnection connection = new SqlConnection(ConnectionString))
        {

            using (SqlCommand command = new SqlCommand(strCommandText, connection))
            {
                for (int i = 0; i < intFieldArrayRows; ++i)
                    if (strFieldArray[i, 1] != "NULL")
                        command.Parameters.AddWithValue("@setOrValue_" + i + "_" + strFieldArray[i, 0], strFieldArray[i, 1]);
                for (int i = 0; i < intWhereArrayRows; ++i)
                    command.Parameters.AddWithValue(whereParamName[i], strWhereParameterArray[i]);
                command.CommandTimeout = 0;
                connection.Open();
                object sqlResult;
                switch (cmdType)
                {
                    case CmdOpType.ExecuteNonQuery:
                        sqlResult = command.ExecuteNonQuery();
                        break;
                    case CmdOpType.ExecuteScalar:
                        sqlResult = command.ExecuteScalar();
                        break;
                    case CmdOpType.ExecuteReader:
                        sqlResult = command.ExecuteReader();
                        break;
                    case CmdOpType.ExecuteReaderReturnDataTable:
                        sqlResult = command.ExecuteReader();
                        DataTable dt = new DataTable();
                        dt.Load((SqlDataReader)sqlResult);
                        return dt;

                    case CmdOpType.ExecuteReaderReturnDataTableCollection: 
                        SqlDataAdapter adapter = new SqlDataAdapter(command);
                        DataSet set = new DataSet(); 
                        adapter.SelectCommand = command; 
                        adapter.Fill(set);
                       DataTableCollection dtc =set.Tables; 
                        return dtc;  

                    default:
                        sqlResult = null;
                        break;
                }
                return sqlResult;

            }
        }
    }

    public static object DBOp(string ConnString, string sql, string[] strParameterArray, CmdOpType cmdType)
    {
        try
        {
            string ConnectionString = ConfigurationManager.ConnectionStrings[ConnString].ToString();
            string[] paramName = null;
            int intParameterArrayRows = (strParameterArray != null && strParameterArray.Length != 0 ? strParameterArray.Length : 0);
            if (sql != "" && intParameterArrayRows > 0)
            {
                paramName = new string[intParameterArrayRows];
                for (int i = 0; i < paramName.Length; ++i)
                    paramName[i] = "@param_" + i;
                sql = string.Format(sql, paramName);
            }

            using (SqlConnection connection = new SqlConnection(ConnectionString))
            {

                using (SqlCommand command = new SqlCommand(sql, connection))
                {
                    for (int i = 0; i < intParameterArrayRows; ++i)
                        command.Parameters.AddWithValue(paramName[i], strParameterArray[i]);
                    command.CommandTimeout = 0;
                    connection.Open();
                    object sqlResult;
                    switch (cmdType)
                    {
                        case CmdOpType.ExecuteNonQuery:
                            sqlResult = command.ExecuteNonQuery();
                            break;
                        case CmdOpType.ExecuteScalar:
                            sqlResult = command.ExecuteScalar();
                            break;
                        case CmdOpType.ExecuteReader:
                            sqlResult = command.ExecuteReader();
                            break;
                        case CmdOpType.ExecuteReaderReturnDataTable:
                            sqlResult = command.ExecuteReader();
                            DataTable dt = new DataTable();
                            dt.Load((SqlDataReader)sqlResult);
                            return dt;

                        case CmdOpType.ExecuteReaderReturnDataTableCollection:
                            SqlDataAdapter adapter = new SqlDataAdapter(command);
                            DataSet set = new DataSet();
                            adapter.SelectCommand = command;
                            adapter.Fill(set);
                            DataTableCollection dtc = set.Tables;
                            return dtc;  
                        default:
                            sqlResult = null;
                            break;
                    }
                    return sqlResult;
                }
            }
        }
        catch (Exception ex)
        {
            throw ex;
        }
    }
    public static object DBOpQ(string ConnString, string sql, Queue paramQ, CmdOpType cmdType)
    {
        string ConnectionString = ConfigurationManager.ConnectionStrings[ConnString].ToString();
        string[] paramName = null;
        int intParameterArrayRows = (paramQ != null && paramQ.Count != 0 ? paramQ.Count : 0);
        if (sql != "" && intParameterArrayRows > 0)
        {
            paramName = new string[intParameterArrayRows];
            for (int i = 0; i < paramName.Length; ++i)
                paramName[i] = "@param_" + i;
            sql = string.Format(sql, paramName);
        }
        using (SqlConnection connection = new SqlConnection(ConnectionString))
        {

            using (SqlCommand command = new SqlCommand(sql, connection))
            {

                for (int i = 0; i < intParameterArrayRows; ++i)
                {
                    command.Parameters.AddWithValue(paramName[i], paramQ.Dequeue());
                }
                command.CommandTimeout = 0;
                connection.Open();
                object sqlResult;
                switch (cmdType)
                {
                    case CmdOpType.ExecuteNonQuery:
                        sqlResult = command.ExecuteNonQuery();
                        break;
                    case CmdOpType.ExecuteScalar:
                        sqlResult = command.ExecuteScalar();
                        break;
                    case CmdOpType.ExecuteReader:
                        sqlResult = command.ExecuteReader();
                        break;
                    case CmdOpType.ExecuteReaderReturnDataTable:
                        sqlResult = command.ExecuteReader();
                        DataTable dt = new DataTable();
                        dt.Load((SqlDataReader)sqlResult);
                        return dt;
                    default:
                        sqlResult = null;
                        break;
                }
                return sqlResult;
            }
        }
    }
    public static string DataTabletoHTMLTable(DataTable dt)
    {
        StringBuilder sb = new StringBuilder("<table><tr>");
        foreach (DataColumn col in dt.Columns)
            sb.AppendFormat("<td>{0}</td>", col.Caption);
        sb.Append("</tr>");
        foreach (DataRow row in dt.Rows)
        {
            sb.Append("<tr>");
            foreach (DataColumn col in dt.Columns)
                sb.AppendFormat("<td>{0}</td>", row[col].ToString());
            sb.Append("</tr>");
        }
        sb.Append("</table>");
        return sb.ToString();
    }
    public static string ToStringAsXml(DataTable dt)
    {
        StringWriter sw = new StringWriter();
        if (dt.TableName == "") dt.TableName = "Dummy";
        dt.WriteXml(sw, XmlWriteMode.IgnoreSchema);
        return sw.ToString();
    }
    public static DataTable ToDataTableFromXML(string dtXML)
    {
        StringReader stream = new StringReader(dtXML);
        DataSet ds = new DataSet();
        ds.ReadXml(stream);
        return (ds.Tables.Count > 0 ? ds.Tables[0] : null);
    }
    public static string DataTableToString(DataTable dt)
    {
        StringBuilder sb = new StringBuilder("");
        foreach (DataColumn col in dt.Columns)
            sb.AppendFormat("{0}    ", col.Caption);

        sb.Append("<br/>");

        foreach (DataRow row in dt.Rows)
        { 
            foreach (DataColumn col in dt.Columns)
                sb.AppendFormat("{0}    ", row[col].ToString().Replace ("<br/>",";"));

            if (dt.Rows.Count>1) sb.Append("<br/>");   
        }
        return sb.ToString();
    }

}