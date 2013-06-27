package gov.va.oia.terminology.converters.umlsUtils.propertyTypes;

import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_Refsets;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.Property;

/**
 * Properties from the DTS ndf load which are treated as alternate IDs within the workbench.
 * @author Daniel Armbrust
 */
public class PT_Refsets extends BPT_Refsets
{
	//TODO get rid of enum
	public enum Refsets
	{
		ALL("All Concepts"),
		CUI_CONCEPTS("All CUI Concepts"),
		AUI_CONCEPTS("All AUI Concepts");

		String niceName;
		private Refsets(String niceName)
		{
			// Don't know the owner yet - will be autofilled when we add this to the parent, below.
			this.niceName = niceName;
		}

		public String getPropertyName()
		{
			return niceName;
		}
	}
	
	public PT_Refsets(String terminologyName)
	{
		super(terminologyName);
		for (Refsets mm : Refsets.values())
		{
			//owner autofiled by addProperty call
			addProperty(new Property(null, mm.getPropertyName()));
		}
	}
}
