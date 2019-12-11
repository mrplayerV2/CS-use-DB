public static void CreateAccess(string DBPath)
{
        if (File.Exists(DBPath))
         {
            throw new Exception("err!");
        }
        DBPath = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + DBPath;
       
        ADOX.CatalogClass cat = new ADOX.CatalogClass();
        
        cat.Create(DBPath);
}


public static void CompactAccess(string DBPath)
{
        if (File.Exists(DBPath))
		{
            throw new Exception("err!");
        }
        string temp = DateTime.Now.ToShortDateString() + ".bak";
        temp = DBPath.Substring(0, DBPath.LastIndexOf("\\") + 1) + temp;
        string temp2 = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + temp;
        string DBPath2 = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + DBPath;
        JRO.JetEngineClass jt = new JRO.JetEngineClass();
        jt.CompactDatabase(DBPath2, temp2);
        File.Copy(temp, DBPath, true);
        File.Delete(temp);
}
    
   
public void BackUpDB(string oldDBPath, string newDBPath)
{
		if (!File.Exists(oldDBPath))
		{
            throw new Exception("err！");
		}
		try
		{
            File.Copy(oldDBPath, newDBPath, true);
		}
		catch (IOException ixp)
		{
            throw new Exception(ixp.ToString());
		}
}
