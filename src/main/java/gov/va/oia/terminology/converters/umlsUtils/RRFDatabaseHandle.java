package gov.va.oia.terminology.converters.umlsUtils;

import gov.va.oia.terminology.converters.sharedUtils.ConsoleUtil;
import gov.va.oia.terminology.converters.umlsUtils.sql.ColumnDefinition;
import gov.va.oia.terminology.converters.umlsUtils.sql.DataType;
import gov.va.oia.terminology.converters.umlsUtils.sql.TableDefinition;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;

public class RRFDatabaseHandle
{
	Connection connection_;

	/**
	 * If file provided, created or opened at that path.  If file is null, an in-memory db is created.
	 * Returns false if the database already existed, true if it was newly created.
	 */
	public boolean createOrOpenDatabase(File dbFile) throws ClassNotFoundException, SQLException
	{
		boolean createdNew = true;
		if (dbFile != null)
		{
			File temp = new File(dbFile.getParentFile(), dbFile.getName() + ".h2.db");
			if (temp.exists())
			{
				createdNew = false;
			}
		}
		Class.forName("org.h2.Driver");
		if (dbFile == null)
		{
			connection_ = DriverManager.getConnection("jdbc:h2:mem:");
		}
		else
		{
			connection_ = DriverManager.getConnection("jdbc:h2:" + dbFile.getAbsolutePath() +";LOG=0;CACHE_SIZE=512000;LOCK_MODE=0;");
		}
		return createdNew;
	}
	
	public void createTable(TableDefinition td) throws SQLException
	{
		Statement s = connection_.createStatement();
		
		StringBuilder sql = new StringBuilder();
		sql.append("CREATE TABLE " + td.getTableName() + " (");
		for (ColumnDefinition cd : td.getColumns())
		{
			sql.append(cd.asH2());
			sql.append(",");
		}
		sql.setLength(sql.length() - 1);
		sql.append(")");
		
		s.executeUpdate(sql.toString());
	}
	
	public Connection getConnection()
	{
		return connection_;
	}
	
	public void shutdown() throws SQLException
	{
		connection_.close();
	}
	
	/**
	 * Create a set of tables that from an XML file that matches the schema DatabaseDefinition.xsd
	 */
	public List<TableDefinition> loadTableDefinitionsFromXML(InputStream is) throws Exception
	{
		SAXBuilder builder = new SAXBuilder();
		Document d = builder.build(is);
		Element root = d.getRootElement();

		ArrayList<TableDefinition> tables = new ArrayList<>();
		for (Element table : root.getChildren())
		{
			TableDefinition td = new TableDefinition(table.getAttributeValue("name"));
			for (Element column : table.getChildren())
			{
				Integer size = null;
				if (column.getAttributeValue("size") != null)
				{
					size = Integer.parseInt(column.getAttributeValue("size"));
				}
				Boolean allowNull = null;
				if (column.getAttributeValue("allowNull") != null)
				{
					allowNull = Boolean.valueOf(column.getAttributeValue("allowNull"));
				}
				td.addColumn(new ColumnDefinition(column.getAttributeValue("name"), new DataType(column.getAttributeValue("type"), size, allowNull)));
			}
			tables.add(td);
			createTable(td);
		}
		is.close();
		return tables;
	}
	
	public void loadDataIntoTable(TableDefinition td, UMLSFileReader data) throws SQLException, IOException
	{
		ConsoleUtil.println("Creating table " + td.getTableName());
		StringBuilder insert = new StringBuilder();
		insert.append("INSERT INTO ");
		insert.append(td.getTableName());
		insert.append("(");
		for (ColumnDefinition cd : td.getColumns())
		{
			insert.append(cd.getColumnName());
			insert.append(",");
		}
		insert.setLength(insert.length() - 1);
		insert.append(") VALUES (");
		for (int i = 0; i < td.getColumns().size(); i++)
		{
			insert.append("?,");
		}
		insert.setLength(insert.length() - 1);
		insert.append(")");

		PreparedStatement ps = connection_.prepareStatement(insert.toString());

		ConsoleUtil.println("Loading table " + td.getTableName());

		int rowCount = 0;
		while (data.hasNextRow())
		{
			List<String> cols = data.getNextRow();
			if (cols.size() != td.getColumns().size())
			{
				throw new RuntimeException("Data length mismatch!");
			}

			ps.clearParameters();
			int psIndex = 1;
			
			for (String s : cols)
			{
				DataType colType = td.getColumns().get(psIndex - 1).getDataType();
				if (colType.isBoolean())
				{
					if (s == null || s.length() == 0)
					{
						ps.setNull(psIndex, Types.BOOLEAN);
					}
					else
					{
						ps.setBoolean(psIndex, Boolean.valueOf(s));
					}
				}
				else if (colType.isInteger())
				{
					if (s == null || s.length() == 0)
					{
						ps.setNull(psIndex, Types.INTEGER);
					}
					else
					{
						ps.setInt(psIndex, Integer.parseInt(s));
					}
				}
				else if (colType.isLong())
				{
					if (s == null || s.length() == 0)
					{
						ps.setNull(psIndex, Types.BIGINT);
					}
					else
					{
						ps.setLong(psIndex, Long.parseLong(s));
					}
				}
				else if (colType.isString())
				{
					if (s == null || s.length() == 0)
					{
						ps.setNull(psIndex, Types.VARCHAR);
					}
					else
					{
						ps.setString(psIndex, s);
					}
				}
				else
				{
					throw new RuntimeException("Unsupported data type");
				}
				psIndex++;
			}
			ps.execute();
			rowCount++;
			if (rowCount % 10 == 0)
			{
				ConsoleUtil.showProgress();
			}
		}
		ps.close();
		data.close();
		ConsoleUtil.println("Loaded " + rowCount + " rows");
	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException
	{
		RRFDatabaseHandle rrf = new RRFDatabaseHandle();
		rrf.createOrOpenDatabase(new File("/mnt/SSD/scratch/h2Test"));
		
		TableDefinition td = new TableDefinition("Test");
		td.addColumn(new ColumnDefinition("testcol", new DataType("STRING", 50, true)));
		
		rrf.createTable(td);
		
		rrf.shutdown();
	}
}
