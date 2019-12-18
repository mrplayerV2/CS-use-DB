using DbAccess;
using System.Collections.Generic;

public class IndexSchema
{
	public string IndexName;

	public bool IsUnique;

	public List<IndexColumn> Columns;
}
