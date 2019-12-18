using DbAccess;
using System.Collections.Generic;

public class DatabaseSchema
{
	public List<TableSchema> Tables = new List<TableSchema>();

	public List<ViewSchema> Views = new List<ViewSchema>();
}
