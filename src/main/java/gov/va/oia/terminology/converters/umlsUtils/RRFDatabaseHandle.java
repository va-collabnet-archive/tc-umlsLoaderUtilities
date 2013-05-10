package gov.va.oia.terminology.converters.umlsUtils;

import gov.va.oia.terminology.converters.umlsUtils.sql.ColumnDefinition;
import gov.va.oia.terminology.converters.umlsUtils.sql.DataType;
import gov.va.oia.terminology.converters.umlsUtils.sql.TableDefinition;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class RRFDatabaseHandle
{
	Connection connection_;

	/**
	 * If file provided, created there, otherwise, in memory.
	 * 
	 */
	public void createDatabase(File dbFile) throws ClassNotFoundException, SQLException
	{
		Class.forName("org.h2.Driver");
		if (dbFile == null)
		{
			connection_ = DriverManager.getConnection("jdbc:h2:mem:");
		}
		else
		{
			connection_ = DriverManager.getConnection("jdbc:h2:" + dbFile.getAbsolutePath() +";LOG=0;CACHE_SIZE=512000;LOCK_MODE=0;");
		}
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

	public static void main(String[] args) throws ClassNotFoundException, SQLException
	{
		RRFDatabaseHandle rrf = new RRFDatabaseHandle();
		rrf.createDatabase(new File("/mnt/SSD/scratch/h2Test"));
		
		TableDefinition td = new TableDefinition("Test");
		td.addColumn(new ColumnDefinition("testcol", new DataType("STRING", 50, true)));
		
		rrf.createTable(td);
		
		rrf.shutdown();
	}
}
