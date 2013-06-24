package gov.va.oia.terminology.converters.umlsUtils;

import gov.va.oia.terminology.converters.sharedUtils.ConsoleUtil;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;

public class Relationship
{
	private String name1;
	private String niceName1;
	private String name2;
	private String niceName2;
	
	private String name1SnomedCode;
	private String name2SnomedCode;
	private String name1RelType;
	private String name2RelType;
	
	private boolean isRela;
	
	private Boolean swap; 
	
	public Relationship(boolean isRela)
	{
		this.isRela = isRela;
	}
		
	public void addNiceName(String name, String niceName)
	{
		if (name.equals(name1))
		{
			if (niceName1 != null)
			{
				throw new RuntimeException("Oops");
			}
			niceName1 = niceName;
		}
		else if (name.equals(name2))
		{
			if (niceName2 != null)
			{
				throw new RuntimeException("Oops");
			}
			niceName2 = niceName;
		}
		else if (name1 == null && name2 == null)
		{
			if (niceName1 != null)
			{
				throw new RuntimeException("Oops");
			}
			name1 = name;
			niceName1 = niceName;
		}
		else
		{
			throw new RuntimeException("Oops");
		}
	}
	
	public void addRelInverse(String name, String inverseRelName)
	{
		if (name1 == null && name2 == null)
		{
			name1 = name;
			name2 = inverseRelName;
		}
		else if (name.equals(name1))
		{
			if (name2 == null)
			{
				name2 = inverseRelName;
			}
			else if (!name2.equals(inverseRelName))
			{
				throw new RuntimeException("oops");
			}
		}
		else if (name.equals(name2))
		{
			if (name1 == null)
			{
				name1 = inverseRelName;
			}
			else if (!name1.equals(inverseRelName))
			{
				throw new RuntimeException("oops");
			}
		}
		else
		{
			throw new RuntimeException("oops");
		}
	}
	
	public void addSnomedCode(String name, String code)
	{
		if (name.equals(name1))
		{
			if (name1SnomedCode == null)
			{
				name1SnomedCode = code;
			}
			else
			{
				throw new RuntimeException("oops");
			}
		}
		else if (name.equals(name2))
		{
			if (name2SnomedCode == null)
			{
				name2SnomedCode = code;
			}
			else
			{
				throw new RuntimeException("oops");
			}
		}
		else
		{
			throw new RuntimeException("oops");
		}
	}
	
	public void addRelType(String name, String type)
	{
		if (name.equals(name1))
		{
			if (name1RelType == null)
			{
				name1RelType = type;
			}
			else
			{
				throw new RuntimeException("oops");
			}
		}
		else if (name.equals(name2))
		{
			if (name2RelType == null)
			{
				name2RelType = type;
			}
			else
			{
				throw new RuntimeException("oops");
			}
		}
		else
		{
			throw new RuntimeException("oops");
		}
	}
	
	public String getFSNName()
	{
		return swap ? name2 : name1;
	}
	
	public String getNiceName()
	{
		return swap ? niceName2 : niceName1;
	}
	
	public String getInverseFSNName()
	{
		return swap ? name1 : name2;
	}
	
	public String getInverseNiceName()
	{
		return swap ? niceName1 : niceName2;
	}
	
	public String getRelSnomedCode()
	{
		return swap ? name2SnomedCode : name1SnomedCode;
	}
	
	public String getInverseRelSnomedCode()
	{
		return swap ? name1SnomedCode : name2SnomedCode;
	}
	
	public String getRelType()
	{
		return swap ? name2RelType : name1RelType;
	}
	
	public String getInverseRelType()
	{
		return swap ? name1RelType : name2RelType;
	}
	
	public boolean getIsRela()
	{
		return isRela;
	}
	public void setSwap(Connection c, String tablePrefix) throws SQLException
	{
		if (swap != null)
		{
			throw new RuntimeException("Swap already set!");
		}
		if (name1 == null && name2 != null)
		{
			swap = true;
		}
		else if (name2 == null && name1 != null)
		{
			swap = false;
		}
		else if (name1.equals(name2))
		{
			swap = false;
		}
		else if (name1.equals("RN") || name2.equals("RN"))  //narrower as primary
		{
			swap = name2.equals("RN");
		}
		else if (name1.equals("AQ") || name2.equals("AQ"))  //allowed qualifier as primary
		{
			swap = name2.equals("AQ");
		}
		else if (name1.equals("CHD") || name2.equals("CHD"))  //parent as primary
		{
			swap = name2.equals("CHD");
		}
		else
		{
			//Use the primary assignments above, to figure out the more detailed assignments (where possible)
			Statement s = c.createStatement();
			ResultSet rs = s.executeQuery("Select distinct REL from " + tablePrefix + "REL where RELA='" + name1 + "'");
			while (rs.next())
			{
				if (rs.getString("REL").equals("RO"))
				{
					//ignore these - they sometimes occur in tandem with a directional one below
					continue;
				}
				if (swap != null)
				{
					throw new RuntimeException("too many results on rel " + name1);
				}
				String rel = rs.getString("REL");
				if (new HashSet<String>(Arrays.asList(new String[] {"RB", "RN", "QB", "AQ", "PAR", "CHD"})).contains(rel))
				{
					if (rel.equals("RN") || rel.equals("AQ") || rel.equals("CHD"))
					{
						swap = false;
					}
					else 
					{
						swap = true;
					}
				}
			}
			rs.close();
			s.close();
			
			if (swap == null)
			{
				if (name1.startsWith("inverse_") || name2.startsWith("inverse_"))  //inverse_ things as secondary
				{
					swap = name1.startsWith("inverse_");
				}
				else if (name1.startsWith("has_") || name2.startsWith("has_"))  //has_ things as secondary
				{
					swap = name1.startsWith("has_");
				}
				else if (name1.startsWith("may_be") || name2.startsWith("may_be"))  //may_be X as primary
				{
					swap = name2.startsWith("may_be");
				}
				else if (name1.contains("_from") || name2.contains("_from"))  //X_from as primary
				{
					swap = name2.contains("_from");
				}
				else if (name1.contains("_by") || name2.contains("_by"))  //X_by as primary
				{
					swap = name2.contains("_by");
				}
				else if (name1.contains("_in_") || name2.contains("_in_"))  //X_in_ as primary
				{
					swap = name2.contains("_in_");
				}
				else if (name1.endsWith("_in") || name2.endsWith("_in"))  //X_in as primary
				{
					swap = name2.endsWith("_in");
				}
				else if (name1.contains("_is") || name2.contains("_is"))  //X_is as primary
				{
					swap = name2.contains("_is");
				}
				else if (name1.startsWith("is_") || name2.startsWith("is_"))  //is_ as primary
				{
					swap = name2.startsWith("is_");
				}
				else if (name1.contains("_has") || name2.contains("_has"))  //X_has as secondary
				{
					swap = name1.contains("_has");
				}
			}
		}
		if (swap == null)
		{
			ConsoleUtil.println("No rel direction preference specified for " + name1 + "/" + name2 + " - using " + name1 + " as primary");
			swap = false;
		}
	}
}
