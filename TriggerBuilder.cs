using DbAccess;
using System.Collections.Generic;
using System.Text;

public static class TriggerBuilder
{
	public static IList<TriggerSchema> GetForeignKeyTriggers(TableSchema dt)
	{
		IList<TriggerSchema> list = new List<TriggerSchema>();
		foreach (ForeignKeySchema foreignKey in dt.ForeignKeys)
		{
			new StringBuilder();
			list.Add(GenerateInsertTrigger(foreignKey));
			list.Add(GenerateUpdateTrigger(foreignKey));
			list.Add(GenerateDeleteTrigger(foreignKey));
		}
		return list;
	}

	private static string MakeTriggerName(ForeignKeySchema fks, string prefix)
	{
		return prefix + "_" + fks.TableName + "_" + fks.ColumnName + "_" + fks.ForeignTableName + "_" + fks.ForeignColumnName;
	}

	public static TriggerSchema GenerateInsertTrigger(ForeignKeySchema fks)
	{
		TriggerSchema triggerSchema = new TriggerSchema();
		triggerSchema.Name = MakeTriggerName(fks, "fki");
		triggerSchema.Type = TriggerType.Before;
		triggerSchema.Event = TriggerEvent.Insert;
		triggerSchema.Table = fks.TableName;
		string text = "";
		if (fks.IsNullable)
		{
			text = " NEW." + fks.ColumnName + " IS NOT NULL AND";
		}
		triggerSchema.Body = "SELECT RAISE(ROLLBACK, 'insert on table " + fks.TableName + " violates foreign key constraint " + triggerSchema.Name + "') WHERE" + text + " (SELECT " + fks.ForeignColumnName + " FROM " + fks.ForeignTableName + " WHERE " + fks.ForeignColumnName + " = NEW." + fks.ColumnName + ") IS NULL; ";
		return triggerSchema;
	}

	public static TriggerSchema GenerateUpdateTrigger(ForeignKeySchema fks)
	{
		TriggerSchema triggerSchema = new TriggerSchema();
		triggerSchema.Name = MakeTriggerName(fks, "fku");
		triggerSchema.Type = TriggerType.Before;
		triggerSchema.Event = TriggerEvent.Update;
		triggerSchema.Table = fks.TableName;
		string name = triggerSchema.Name;
		string text = "";
		if (fks.IsNullable)
		{
			text = " NEW." + fks.ColumnName + " IS NOT NULL AND";
		}
		triggerSchema.Body = "SELECT RAISE(ROLLBACK, 'update on table " + fks.TableName + " violates foreign key constraint " + name + "') WHERE" + text + " (SELECT " + fks.ForeignColumnName + " FROM " + fks.ForeignTableName + " WHERE " + fks.ForeignColumnName + " = NEW." + fks.ColumnName + ") IS NULL; ";
		return triggerSchema;
	}

	public static TriggerSchema GenerateDeleteTrigger(ForeignKeySchema fks)
	{
		TriggerSchema triggerSchema = new TriggerSchema();
		triggerSchema.Name = MakeTriggerName(fks, "fkd");
		triggerSchema.Type = TriggerType.Before;
		triggerSchema.Event = TriggerEvent.Delete;
		triggerSchema.Table = fks.ForeignTableName;
		string name = triggerSchema.Name;
		if (!fks.CascadeOnDelete)
		{
			triggerSchema.Body = "SELECT RAISE(ROLLBACK, 'delete on table " + fks.ForeignTableName + " violates foreign key constraint " + name + "') WHERE (SELECT " + fks.ColumnName + " FROM " + fks.TableName + " WHERE " + fks.ColumnName + " = OLD." + fks.ForeignColumnName + ") IS NOT NULL; ";
		}
		else
		{
			triggerSchema.Body = "DELETE FROM [" + fks.TableName + "] WHERE " + fks.ColumnName + " = OLD." + fks.ForeignColumnName + "; ";
		}
		return triggerSchema;
	}
}