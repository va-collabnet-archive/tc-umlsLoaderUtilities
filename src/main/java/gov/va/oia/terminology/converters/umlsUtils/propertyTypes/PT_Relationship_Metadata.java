package gov.va.oia.terminology.converters.umlsUtils.propertyTypes;

import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.PropertyType;

/**
 * @author Daniel Armbrust
 */
public class PT_Relationship_Metadata extends PropertyType
{
	public PT_Relationship_Metadata()
	{
		super("Relationship Metadata");
		addProperty("Inverse FSN");
		addProperty("Inverse Preferred Name");
		addProperty("Inverse Description");
		addProperty("General Rel Type");
		addProperty("Inverse General Rel Type");
		addProperty("Snomed Code");
		addProperty("Inverse Snomed Code");
	}
}
