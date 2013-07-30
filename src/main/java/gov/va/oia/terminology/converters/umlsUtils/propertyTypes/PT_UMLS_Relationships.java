package gov.va.oia.terminology.converters.umlsUtils.propertyTypes;

import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_Relations;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.Property;

/**
 * @author Daniel Armbrust
 */
public class PT_UMLS_Relationships extends BPT_Relations
{
	public Property UMLS_ATOM;
	
	public PT_UMLS_Relationships()
	{
		super("UMLS");
		UMLS_ATOM = addProperty("has_UMLS_atom", null, "Relationship to link CUI and AUI UMLS concepts");
	}
}
