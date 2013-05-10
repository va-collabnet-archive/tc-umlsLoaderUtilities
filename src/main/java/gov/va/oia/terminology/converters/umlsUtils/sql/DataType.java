package gov.va.oia.terminology.converters.umlsUtils.sql;

public class DataType
{
	private static short STRING = 0;
	private static short INTEGER = 1;
	private static short LONG = 2;
	private static short BOOLEAN = 3;

	private short type_;
	private int dataSize_ = -1;
	private boolean allowsNull_;
	
	public DataType(String type, Integer size, Boolean allowsNull)
	{
		if (type.equals("STRING"))
		{
			type_ = STRING;
		}
		else if (type.equals("INTEGER"))
		{
			type_ = INTEGER;
		}
		else if (type.equals("LONG"))
		{
			type_ = LONG;
		}
		else if (type.equals("BOOLEAN"))
		{
			type_ = BOOLEAN;
		}
		else
		{
			throw new RuntimeException("oops");
		}
		
		if (size != null)
		{
			dataSize_ = size;
		}
		
		if (allowsNull == null) 
		{
			allowsNull_ = true;
		}
		else
		{
			allowsNull_ = allowsNull.booleanValue();
		}
	}
	
	public boolean isString()
	{
		return type_ == STRING;
	}
	
	public boolean isBoolean()
	{
		return type_ == BOOLEAN;
	}
	
	public boolean isInteger()
	{
		return type_ == INTEGER;
	}
	
	public boolean isLong()
	{
		return type_ == LONG;
	}
	
	public String asH2()
	{
		StringBuilder sb = new StringBuilder();
		if (type_ == STRING)
		{
			sb.append("VARCHAR ");
			sb.append("(" + dataSize_ + ") ");
		}
		else if (type_ == INTEGER)
		{
			sb.append("INT ");
		}
		else if (type_ == LONG)
		{
			sb.append("BIGINT ");
		}
		else if (type_ == BOOLEAN)
		{
			sb.append("BOOLEAN ");
		}
		else
		{
			throw new RuntimeException("not implemented");
		}
		
		if (!allowsNull_)
		{
			sb.append("NOT NULL");
		}
		return sb.toString();
	}
}
