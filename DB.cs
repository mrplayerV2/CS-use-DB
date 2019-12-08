using System;
using System.Data;
using System.Configuration;
using System.Web;
using System.Web.Security;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.Web.UI.WebControls.WebParts;
using System.Web.UI.HtmlControls;
using System.Data.SqlClient;

public class DB
{
    public DB(){
       
    }
    public static DataTable GetDataTable(string SQL, string ConnString)
    {
        DataTable dt = new DataTable();
        using (SqlConnection sc = new SqlConnection(ConfigurationManager.ConnectionStrings[ConnString].ToString()))
        {
            using (SqlCommand cmd = new SqlCommand(SQL, sc))
            {
                using (SqlDataAdapter da = new SqlDataAdapter(cmd))
                {
                    try
                    {
                        sc.Open();
                        da.Fill(dt);
                        sc.Close();
                    }
                    catch
                    {
                    }
                }
            }
        }
        return dt;
    }
    public static DataTable GetDataTableWithTran(SqlCommand Cmd, string ConnString)
    {
        DataTable dt = new DataTable();
        using (SqlConnection sc = new SqlConnection(ConfigurationManager.ConnectionStrings[ConnString].ToString()))
        {
            sc.Open(); 
            //using (var trans = sc.BeginTransaction(IsolationLevel.ReadUncommitted ))
            using (var trans = sc.BeginTransaction( ))
            {
                try
                {
                    using (var cmd = Cmd)
                    {
                        cmd.Connection = sc;
                        cmd.Transaction = trans;
                        using (SqlDataAdapter da = new SqlDataAdapter(cmd))
                        {
                            da.Fill(dt);
                        }   
                    }  

                    trans.Commit();
                }
                catch (Exception ex)
                {
                    if (trans != null) trans.Rollback();
                    throw ex; 
                }
            }
             
            sc.Close(); 
        }
        return dt;
    }
    public static DataTable GetDataTable(SqlCommand cmd, string ConnString)
    {
        DataTable dt = new DataTable();
        using (SqlConnection sc = new SqlConnection(ConfigurationManager.ConnectionStrings[ConnString].ToString()))
        {
            cmd.Connection = sc;
            using (SqlDataAdapter da = new SqlDataAdapter(cmd))
            {
                try
                {
                    sc.Open();
                    da.Fill(dt);
                    sc.Close();
                }
                catch (Exception ex)
                {
                    throw ex ; 
                }
            }
        }
        return dt;
    }
    public static void ExecuteNonQuery(string SQL, string ConnString)
    {
        using (SqlConnection sc = new SqlConnection(ConfigurationManager.ConnectionStrings[ConnString].ToString()))
        {
            using (SqlCommand cmd = new SqlCommand(SQL, sc))
            {
                try
                {
                    sc.Open();
                    cmd.ExecuteNonQuery();
                    sc.Close();
                }
                catch
                {
                }
            }
        }
    }
    public static void ExecuteNonQuery(SqlCommand cmd, string ConnString)
    {
        using (SqlConnection sc = new SqlConnection(ConfigurationManager.ConnectionStrings[ConnString].ToString()))
        {
            cmd.Connection = sc;
            try
            {
                sc.Open();
                cmd.ExecuteNonQuery();
                sc.Close();
            }
            catch
            {
            }
            finally
            {
            }
        }
    }
}
