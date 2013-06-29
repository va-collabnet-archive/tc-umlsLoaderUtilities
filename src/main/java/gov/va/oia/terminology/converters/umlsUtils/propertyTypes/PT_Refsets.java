package gov.va.oia.terminology.converters.umlsUtils.propertyTypes;

import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_Refsets;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.Property;

/**
 * @author Daniel Armbrust
 */
public class PT_Refsets extends BPT_Refsets
{
	public Property ALL;
	public Property CUI_CONCEPTS;
	public Property AUI_CONCEPTS;
	
	public PT_Refsets(String terminologyName)
	{
		super(terminologyName);
		//owner autofiled by addProperty call
		ALL = addProperty("All " + terminologyName + " Concepts");
		CUI_CONCEPTS = addProperty("All " + terminologyName + " CUI Concepts");
		AUI_CONCEPTS = addProperty("All " + terminologyName + " AUI Concepts");
	}
}
