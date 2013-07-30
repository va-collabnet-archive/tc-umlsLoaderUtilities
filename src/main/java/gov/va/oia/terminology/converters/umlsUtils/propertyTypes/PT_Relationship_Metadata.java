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
		addProperty("Source AUI", null, "The AUI that defines the actual source of the relationship.  Required since multiple AUI atoms are combined to create a single WB concept.");
		addProperty("Target AUI", null, "The AUI that defines the actual target of the relationship.  Required since multiple AUI atoms are combined to create a single WB concept.");
	}
}
