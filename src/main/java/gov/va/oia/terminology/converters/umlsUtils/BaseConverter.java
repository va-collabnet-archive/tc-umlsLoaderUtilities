package gov.va.oia.terminology.converters.umlsUtils;

import gov.va.oia.terminology.converters.sharedUtils.ConsoleUtil;
import gov.va.oia.terminology.converters.sharedUtils.EConceptUtility;
import gov.va.oia.terminology.converters.sharedUtils.EConceptUtility.DescriptionType;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_ContentVersion;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_Descriptions;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_Refsets;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_Relations;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.ConceptCreationNotificationListener;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.Property;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.PropertyType;
import gov.va.oia.terminology.converters.sharedUtils.stats.ConverterUUID;
import gov.va.oia.terminology.converters.umlsUtils.propertyTypes.PT_Refsets;
import gov.va.oia.terminology.converters.umlsUtils.propertyTypes.PT_Relationship_Metadata;
import gov.va.oia.terminology.converters.umlsUtils.propertyTypes.PT_SAB_Metadata;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import org.dwfa.cement.ArchitectonicAuxiliary;
import org.dwfa.util.id.Type3UuidFactory;
import org.ihtsdo.etypes.EConcept;
import org.ihtsdo.tk.dto.concept.component.TkComponent;
import org.ihtsdo.tk.dto.concept.component.refex.type_uuid.TkRefexUuidMember;
import org.ihtsdo.tk.dto.concept.component.relationship.TkRelationship;

public abstract class BaseConverter
{
	protected String namespaceSeed_;
	protected String terminologyName_;
	
	protected PropertyType ptAttributes_;
	protected PropertyType ptIds_;
	protected BPT_Refsets ptRefsets_;
	protected BPT_ContentVersion ptContentVersion_;
	protected PropertyType ptSTypes_;
	protected PropertyType ptSuppress_;
	protected PropertyType ptLanguages_;
	protected PropertyType ptSourceRestrictionLevels_;
	protected PropertyType ptSABs_;
	protected PropertyType ptDescriptions_;
	protected PropertyType ptRelationships_;
	protected PropertyType ptRelationshipQualifiers_;
	protected HashMap<String, Relationship> nameToRel_ = new HashMap<>();
	protected HashMap<String, UUID> semanticTypes_ = new HashMap<>();
	protected EConceptUtility eConcepts_;
	protected DataOutputStream dos_;
	protected RRFDatabaseHandle db_;
	protected String tablePrefix_;
	protected File outputDirectory_;
	private boolean appendToOutputFile_;
	private boolean isRxNorm;
	protected String sab_;
	
	protected UUID metaDataRoot_;
	
	PreparedStatement satRelStatement_;
	
	private HashSet<UUID> loadedRels_ = new HashSet<>();
	private HashSet<UUID> skippedRels_ = new HashSet<>();
	
	protected BaseConverter(String sab, String terminologyName, String pathPrefix, RRFDatabaseHandle db, String tablePrefix, File outputDirectory, boolean appendToOutputFile, PropertyType ids, PropertyType attributes) throws Exception
	{
		terminologyName_ = terminologyName;
		namespaceSeed_ = "gov.va.med.term.RRF." + tablePrefix + "." + sab;
		tablePrefix_ = tablePrefix;
		isRxNorm = tablePrefix_.equals("RXN");
		db_ = db;
		ptIds_ = ids;
		ptAttributes_ = attributes;
		outputDirectory_ = outputDirectory;
		appendToOutputFile_ = appendToOutputFile;
		sab_ = sab;

		File binaryOutputFile = new File(outputDirectory_, "RRF-" + tablePrefix + ".jbin");

		dos_ = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(binaryOutputFile, appendToOutputFile_)));
		eConcepts_ = new EConceptUtility(namespaceSeed_, pathPrefix + " Path", dos_);

		UUID archRoot = ArchitectonicAuxiliary.Concept.ARCHITECTONIC_ROOT_CONCEPT.getPrimoridalUid();
		metaDataRoot_ = ConverterUUID.createNamespaceUUIDFromString("metadata");
		eConcepts_.createAndStoreMetaDataConcept(metaDataRoot_, terminologyName_ + " Metadata", false, archRoot, dos_);

		loadCommonMetaData();
		loadCustomMetaData();

		ConsoleUtil.println("Metadata Statistics");
		for (String s : eConcepts_.getLoadStats().getSummary())
		{
			ConsoleUtil.println(s);
		}

		eConcepts_.clearLoadStats();
		satRelStatement_ = db_.getConnection().prepareStatement("select * from " + tablePrefix_ + "SAT where " + (isRxNorm ? "RXAUI" : "METAUI") 
				+ "= ? and STYPE='RUI' and SAB='" + sab_ + "'");
	}
	
	protected void finish() throws IOException, SQLException
	{
		checkRelationships();
		satRelStatement_.close();
		eConcepts_.storeRefsetConcepts(ptRefsets_, dos_);
		dos_.close();
		ConsoleUtil.println("Load Statistics");
		for (String s : eConcepts_.getLoadStats().getSummary())
		{
			ConsoleUtil.println(s);
		}

		// this could be removed from final release. Just added to help debug editor problems.
		ConsoleUtil.println("Dumping UUID Debug File");
		ConverterUUID.dump(new File(outputDirectory_, "UuidDebugMap.txt"), appendToOutputFile_);
		ConsoleUtil.writeOutputToFile(new File(outputDirectory_, "ConsoleOutput.txt").toPath(), appendToOutputFile_);
		ConsoleUtil.clearOutputCache();
		ConverterUUID.clearCache();
		ConverterUUID.disableUUIDMap_ = false;  //re-enable it in case someone else uses it and expects default behavior
	}
	
	public static void clearTargetFiles(File outputDirectory)
	{
		new File(outputDirectory, "UuidDebugMap.txt").delete();
		new File(outputDirectory, "ConsoleOutput.txt").delete();
		new File(outputDirectory, "RRF.jbin").delete();
	}
	
	protected abstract void loadCustomMetaData() throws Exception;
	protected abstract void addCustomRefsets() throws Exception;
	
	private void loadCommonMetaData() throws Exception
	{
		ptRefsets_ = new PT_Refsets(terminologyName_);
		addCustomRefsets();
		ptContentVersion_ = new BPT_ContentVersion();
		final PropertyType sourceMetadata = new PT_SAB_Metadata();
		final PropertyType relationshipMetadata = new PT_Relationship_Metadata();

		eConcepts_.loadMetaDataItems(Arrays.asList(ptIds_, ptRefsets_, ptContentVersion_, sourceMetadata, relationshipMetadata), metaDataRoot_, dos_);
		
		//dynamically add more attributes from *DOC
		{
			ConsoleUtil.println("Creating attribute types");
			Statement s = db_.getConnection().createStatement();
			//extra logic at the end to keep NDC's from any sab when processing RXNorm
			ResultSet rs = s.executeQuery("SELECT VALUE, TYPE, EXPL from " + tablePrefix_ + "DOC where DOCKEY = 'ATN' and VALUE in (select distinct ATN from " 
					+ tablePrefix_ + "SAT where SAB='" + sab_ + "'" + (isRxNorm ? " or ATN='NDC'" : "") + ")");
			while (rs.next())
			{
				
				String abbreviation = rs.getString("VALUE");
				String type = rs.getString("TYPE");
				String expansion = rs.getString("EXPL");

				if (!type.equals("expanded_form"))
				{
					throw new RuntimeException("Unexpected type in the attribute data within DOC: '" + type + "'");
				}

				String preferredName = null;
				String description = null;
				if (expansion.length() > 30)
				{
					description = expansion;
				}
				else
				{
					preferredName = expansion;
				}
				
				ptAttributes_.addProperty(abbreviation, preferredName, description);
			}
			rs.close();
			s.close();
		}
		eConcepts_.loadMetaDataItems(ptAttributes_, metaDataRoot_, dos_);
		
		//STYPE values
		ptSTypes_= new PropertyType("STYPEs"){};
		{
			ConsoleUtil.println("Creating STYPE types");
			Statement s = db_.getConnection().createStatement();
			ResultSet rs = s.executeQuery("SELECT DISTINCT VALUE, TYPE, EXPL FROM " + tablePrefix_ + "DOC where DOCKEY like 'STYPE%'");
			while (rs.next())
			{
				String sType = rs.getString("VALUE");
				String type = rs.getString("TYPE");
				String name = rs.getString("EXPL");

				if (!type.equals("expanded_form"))
				{
					throw new RuntimeException("Unexpected type in the attribute data within DOC: '" + type + "'");
				}				
				
				ptSTypes_.addProperty(sType, name, null);
			}
			rs.close();
			s.close();
		}
		eConcepts_.loadMetaDataItems(ptSTypes_, metaDataRoot_, dos_);
		
		//SUPPRESS values
		ConsoleUtil.println("Creating Suppress types");
		ptSuppress_= new PropertyType("Suppress") {};
		{
			Statement s = db_.getConnection().createStatement();
			ResultSet rs = s.executeQuery("SELECT VALUE, TYPE, EXPL FROM " + tablePrefix_ + "DOC where DOCKEY='SUPPRESS'");
			while (rs.next())
			{
				String value = rs.getString("VALUE");
				String type = rs.getString("TYPE");
				String name = rs.getString("EXPL");
				
				if (value == null)
				{
					//there is a null entry, don't care about it.
					continue;
				}

				if (!type.equals("expanded_form"))
				{
					throw new RuntimeException("Unexpected type in the attribute data within DOC: '" + type + "'");
				}				
				
				ptSuppress_.addProperty(value, name, null);
			}
			rs.close();
			s.close();
		}
		eConcepts_.loadMetaDataItems(ptSuppress_, metaDataRoot_, dos_);
		
		// Handle the languages
		{
			ConsoleUtil.println("Creating language types");
			ptLanguages_ = new PropertyType("Languages"){};
			Statement s = db_.getConnection().createStatement();
			ResultSet rs = s.executeQuery("SELECT * from " + tablePrefix_ + "DOC where DOCKEY = 'LAT' and VALUE in (select distinct LAT from " 
					+ tablePrefix_ + "CONSO where SAB='" + sab_ + "')");
			while (rs.next())
			{
				String abbreviation = rs.getString("VALUE");
				String type = rs.getString("TYPE");
				String expansion = rs.getString("EXPL");

				if (!type.equals("expanded_form"))
				{
					throw new RuntimeException("Unexpected type in the language data within DOC: '" + type + "'");
				}

				Property p = ptLanguages_.addProperty(abbreviation, expansion, null);

				if (abbreviation.equals("ENG") || abbreviation.equals("SPA"))
				{
					// use official WB SCT languages
					if (abbreviation.equals("ENG"))
					{
						p.setWBPropertyType(UUID.fromString("c0836284-f631-3c86-8cfc-56a67814efab"));
					}
					else if (abbreviation.equals("SPA"))
					{
						p.setWBPropertyType(UUID.fromString("03615ef2-aa56-336d-89c5-a1b5c4cee8f6"));
					}
					else
					{
						throw new RuntimeException("oops");
					}
				}
			}
			rs.close();
			s.close();
			eConcepts_.loadMetaDataItems(ptLanguages_, metaDataRoot_, dos_);
		}
		
		// And Source Restriction Levels
		{
			ConsoleUtil.println("Creating Source Restriction Level types");
			ptSourceRestrictionLevels_ = new PropertyType("Source Restriction Levels"){};
			PreparedStatement ps = db_.getConnection().prepareStatement("SELECT VALUE, TYPE, EXPL from " + tablePrefix_ + "DOC where DOCKEY=? ORDER BY VALUE");
			ps.setString(1, "SRL");
			ResultSet rs = ps.executeQuery();
			
			String value = null;
			String description = null;
			String uri = null;
			
			//Two entries per SRL, read two rows, create an entry.
			
			while (rs.next())
			{
				String type = rs.getString("TYPE");
				String expl = rs.getString("EXPL");
				
				if (type.equals("expanded_form"))
				{
					description = expl;
				}
				else if (type.equals("uri"))
				{
					uri = expl;
				}
				else
				{
					throw new RuntimeException("oops");
				}
					
				
				if (value == null)
				{
					value = rs.getString("VALUE");
				}
				else
				{
					if (!value.equals(rs.getString("VALUE")))
					{
						throw new RuntimeException("oops");
					}
					
					if (description == null || uri == null)
					{
						throw new RuntimeException("oops");
					}
					
					Property p = ptSourceRestrictionLevels_.addProperty(value, null, description);
					final String temp = uri;
					p.registerConceptCreationListener(new ConceptCreationNotificationListener()
					{
						@Override
						public void conceptCreated(Property property, EConcept concept)
						{
							eConcepts_.addStringAnnotation(concept, temp, ptAttributes_.getProperty("URI").getUUID(), false);
						}
					});
					type = null;
					expl = null;
					value = null;
				}
			}
			rs.close();
			ps.close();

			eConcepts_.loadMetaDataItems(ptSourceRestrictionLevels_, metaDataRoot_, dos_);
		}

		// And Source vocabularies
		{
			ConsoleUtil.println("Creating Source Vocabulary types");
			ptSABs_ = new PropertyType("Source Vocabularies"){};
			
			
			HashSet<String> sabList = new HashSet<>();
			sabList.add(sab_);
			
			Statement s = db_.getConnection().createStatement();
			ResultSet rs;
			if (isRxNorm)
			{
				rs = s.executeQuery("select distinct SAB from RXNSAT where ATN='NDC'");
				while (rs.next())
				{
					sabList.add(rs.getString("SAB"));
				}
				rs.close();
				s.close();
			}
			
			for (String currentSab : sabList)
			{
				s = db_.getConnection().createStatement();
				rs = s.executeQuery("SELECT SON from " + tablePrefix_ + "SAB WHERE (RSAB='" + currentSab + "' or VSAB='" + currentSab + "')");
				if (rs.next())
				{
					String son = rs.getString("SON");

					Property p = ptSABs_.addProperty(currentSab, son, null);
					p.registerConceptCreationListener(new ConceptCreationNotificationListener()
					{
						@Override
						public void conceptCreated(Property property, EConcept concept)
						{
							try
							{
								PreparedStatement getMetadata = db_.getConnection().prepareStatement("Select * from " + tablePrefix_ + "SAB where RSAB = ? or VSAB = ?");

								//lookup the other columns for the row with this newly added RSAB terminology
								getMetadata.setString(1, property.getSourcePropertyNameFSN());
								getMetadata.setString(2, property.getSourcePropertyNameFSN());
								ResultSet rs = getMetadata.executeQuery();
								if (rs.next())  //should be only one result
								{
									for (Property metadataProperty : sourceMetadata.getProperties())
									{
										String columnName = metadataProperty.getSourcePropertyNameFSN();
										String columnValue = rs.getString(columnName);
										if (columnName.equals("SRL"))
										{
											eConcepts_.addUuidAnnotation(concept, ptSourceRestrictionLevels_.getProperty(columnValue).getUUID(),
													metadataProperty.getUUID());
										}
										else
										{
											eConcepts_.addStringAnnotation(concept, columnValue, metadataProperty.getUUID(), false);
										}
									}
								}
								if (rs.next())
								{
									throw new RuntimeException("Too many sabs.  Perhaps you should be using versioned sabs!");
								}
								rs.close();
								getMetadata.close();
							}
							catch (SQLException e)
							{
								throw new RuntimeException("Error loading *SAB", e);
							}
						}
					});
				}
				else
				{
					throw new RuntimeException("Too few? SABs - perhaps you need to use versioned SABs.");
				}
				if (rs.next())
				{
					throw new RuntimeException("Too many SABs - perhaps you need to use versioned SABs.");
				}
				rs.close();
				s.close();
			}

			eConcepts_.loadMetaDataItems(ptSABs_, metaDataRoot_, dos_);
		}

		// And Descriptions
		{
			ConsoleUtil.println("Creating description types");
			ptDescriptions_ = new BPT_Descriptions(terminologyName_);
			Statement s = db_.getConnection().createStatement();
			ResultSet usedDescTypes;
			if (isRxNorm )
			{
				usedDescTypes = s.executeQuery("select distinct TTY from RXNCONSO WHERE SAB='" + sab_ + "'");
			}
			else
			{
				usedDescTypes = s.executeQuery("select distinct TTY from MRRANK WHERE SAB='" + sab_ + "'");
			}

			PreparedStatement ps = db_.getConnection().prepareStatement("select TYPE, EXPL from " + tablePrefix_ + "DOC where DOCKEY='TTY' and VALUE=?");

			while (usedDescTypes.next())
			{
				String tty = usedDescTypes.getString(1);
				ps.setString(1, tty);
				ResultSet descInfo = ps.executeQuery();

				String expandedForm = null;
				final HashSet<String> classes = new HashSet<>();

				while (descInfo.next())
				{
					String type = descInfo.getString("TYPE");
					String expl = descInfo.getString("EXPL");
					if (type.equals("expanded_form"))
					{
						if (expandedForm != null)
						{
							throw new RuntimeException("Expected name to be null!");
						}
						expandedForm = expl;
					}
					else if (type.equals("tty_class"))
					{
						classes.add(expl);
					}
					else
					{
						throw new RuntimeException("Unexpected type in DOC for '" + tty + "'");
					}
				}
				descInfo.close();
				ps.clearParameters();
				Property p = makeDescriptionType(tty, expandedForm, classes);
				p.registerConceptCreationListener(new ConceptCreationNotificationListener()
				{
					@Override
					public void conceptCreated(Property property, EConcept concept)
					{
						for (String tty_class : classes)
						{
							eConcepts_.addStringAnnotation(concept, tty_class, ptAttributes_.getProperty("tty_class").getUUID(), false);
						}
					}
				});
				
			}
			usedDescTypes.close();
			s.close();
			ps.close();
			allDescriptionsCreated();
			
			eConcepts_.loadMetaDataItems(ptDescriptions_, metaDataRoot_, dos_);
		}
		
		// And semantic types
		{
			ConsoleUtil.println("Creating semantic types");
			PropertyType ptSemanticTypes = new PropertyType("Semantic Types"){};
			Statement s = db_.getConnection().createStatement();
			ResultSet rs = s.executeQuery("SELECT distinct TUI, STN, STY from " + tablePrefix_+ "STY");
			while (rs.next())
			{
				final String tui = rs.getString("TUI");
				final String stn = rs.getString("STN");
				String sty = rs.getString("STY");

				Property p = ptSemanticTypes.addProperty(sty);
				semanticTypes_.put(tui, p.getUUID());
				p.registerConceptCreationListener(new ConceptCreationNotificationListener()
				{
					@Override
					public void conceptCreated(Property property, EConcept concept)
					{
						eConcepts_.addAdditionalIds(concept, tui, ptIds_.getProperty("TUI").getUUID(), false);
						eConcepts_.addStringAnnotation(concept, stn, ptAttributes_.getProperty("STN").getUUID(), false);
					}
				});
			}
			rs.close();
			s.close();

			eConcepts_.loadMetaDataItems(ptSemanticTypes, metaDataRoot_, dos_);
		}
		
		loadRelationshipMetadata(relationshipMetadata);
	}
	
	/**
	 * Implementer needs to add an entry to ptDescriptions_ with the data provided...
	 */
	protected abstract Property makeDescriptionType(String fsnName, String preferredName, final Set<String> tty_classes);
	
	/**
	 * Called just before the description set is actually created as eConcepts (in case the implementor needs to rank them all together)
	 */
	protected abstract void allDescriptionsCreated() throws Exception;
	
	protected abstract void processSAT(TkComponent<?> itemToAnnotate, ResultSet rs) throws SQLException;
	
	private void loadRelationshipMetadata(final PropertyType relationshipMetadata) throws Exception
	{
		ConsoleUtil.println("Creating relationship types");
		//Both of these get added as extra attributes on the relationship definition
		HashMap<String, String> snomedCTRelaMappings = new HashMap<>(); //Maps something like 'has_specimen_source_morphology' to '118168003'
		HashMap<String, String> snomedCTRelMappings = new HashMap<>();  //Maps something like '118168003' to 'RO'
		
		nameToRel_ = new HashMap<>();
		
		Statement s = db_.getConnection().createStatement();
		//get the inverses of first, before the expanded forms
		ResultSet rs = s.executeQuery("SELECT DOCKEY, VALUE, TYPE, EXPL FROM " + tablePrefix_ + "DOC where DOCKEY ='REL' or DOCKEY = 'RELA' order by TYPE DESC ");
		while (rs.next())
		{
			String dockey = rs.getString("DOCKEY");
			String value = rs.getString("VALUE");
			String type = rs.getString("TYPE");
			String expl = rs.getString("EXPL");
			if (value == null)
			{
				continue;  //don't need this one
			}
			
			if (type.equals("snomedct_rela_mapping"))
			{
				snomedCTRelaMappings.put(expl,  value);
			}
			else if (type.equals("snomedct_rel_mapping"))
			{
				snomedCTRelMappings.put(value, expl);
			}
			else
			{
				Relationship rel = nameToRel_.get(value);
				if (rel == null)
				{
					if (type.endsWith("_inverse"))
					{
						rel = nameToRel_.get(expl);
						if (rel == null)
						{
							rel = new Relationship(dockey.equals("RELA"));
							nameToRel_.put(value, rel);
							nameToRel_.put(expl, rel);
						}
						else
						{
							throw new RuntimeException("shouldn't happen due to query order");
						}
					}
					else
					{
						//only cases where there is no inverse
						rel = new Relationship(dockey.equals("RELA"));
						nameToRel_.put(value, rel);
					}
				}
				
				if (type.equals("expanded_form"))
				{
					rel.addNiceName(value, expl);
				}
				else if (type.equals("rela_inverse") || type.equals("rel_inverse"))
				{
					rel.addRelInverse(value, expl);
				}
				else
				{
					throw new RuntimeException("Oops");
				}
			}
		}
		
		rs.close();
		s.close();
		
		HashSet<String> actuallyUsedRelsOrRelas = new HashSet<>();
		
		for (Entry<String, String> x : snomedCTRelaMappings.entrySet())
		{
			if (!nameToRel_.containsKey(x.getKey()))
			{
				//metamorphosys doesn't seem to remove these when the sct rel types aren't included - just silently remove them 
				//unless it seems that they should map.
				if (isRxNorm || sab_.startsWith("SNOMEDCT"))
				{
					throw new RuntimeException("ERROR - No rel for " + x.getKey() + ".");
				}
				snomedCTRelMappings.remove(x.getValue());
			}
			else
			{
				nameToRel_.get(x.getKey()).addSnomedCode(x.getKey(), x.getValue());
				String relType = snomedCTRelMappings.remove(x.getValue());
				if (relType != null)
				{
					nameToRel_.get(x.getKey()).addRelType(x.getKey(), relType);
					//Shouldn't need this, but there are some cases where the metadata is inconsistent - with how it is actually used.
					actuallyUsedRelsOrRelas.add(relType);
				}
			}
		}
		
		if (snomedCTRelMappings.size() > 0)
		{
			throw new RuntimeException("oops");
		}
		
		ptRelationships_ = new BPT_Relations(terminologyName_) {};
		ptRelationshipQualifiers_ = new PropertyType("Relationship Qualifiers") {};
		
		s = db_.getConnection().createStatement();
		rs = s.executeQuery("select distinct REL, RELA from " + tablePrefix_ + "REL where SAB='" + sab_ + "'");
		while (rs.next())
		{
			actuallyUsedRelsOrRelas.add(rs.getString("REL"));
			if (rs.getString("RELA") != null)
			{
				actuallyUsedRelsOrRelas.add(rs.getString("RELA"));
			}
		}
		rs.close();
		s.close();
		
		HashSet<Relationship> uniqueRels = new HashSet<>(nameToRel_.values());
		for (final Relationship r : uniqueRels)
		{
			r.setSwap(db_.getConnection(), tablePrefix_);
			
			if (!actuallyUsedRelsOrRelas.contains(r.getFSNName()) && !actuallyUsedRelsOrRelas.contains(r.getInverseFSNName()))
			{
				continue;
			}
			
			Property p;
			if (r.getIsRela())
			{
				p = ptRelationshipQualifiers_.addProperty(r.getFSNName(), r.getNiceName(), null);
			}
			else
			{
				p = ptRelationships_.addProperty(r.getFSNName(), r.getNiceName(), null);
			}
			
			p.registerConceptCreationListener(new ConceptCreationNotificationListener()
			{
				@Override
				public void conceptCreated(Property property, EConcept concept)
				{
					if (r.getInverseFSNName() != null)
					{
						eConcepts_.addDescription(concept, r.getInverseFSNName(), DescriptionType.FSN, false, null, 
								relationshipMetadata.getProperty("Inverse FSN").getUUID(), false);
					}
					
					if (r.getInverseNiceName() != null)
					{
						eConcepts_.addDescription(concept, r.getInverseNiceName(), DescriptionType.SYNONYM, true, null, 
								relationshipMetadata.getProperty("Inverse Name").getUUID(), false);
					}
					
					if (r.getRelType() != null)
					{
						Relationship generalRel = nameToRel_.get(r.getRelType());
						
						eConcepts_.addUuidAnnotation(concept, ptRelationships_.getProperty(generalRel.getFSNName()).getUUID(), 
								relationshipMetadata.getProperty("General Rel Type").getUUID());
					}
					
					if (r.getInverseRelType() != null)
					{
						Relationship generalRel = nameToRel_.get(r.getInverseRelType());
						
						eConcepts_.addUuidAnnotation(concept, ptRelationships_.getProperty(generalRel.getFSNName()).getUUID(), 
								relationshipMetadata.getProperty("Inverse General Rel Type").getUUID());
					}
					
					if (r.getRelSnomedCode() != null)
					{
						eConcepts_.addUuidAnnotation(concept, Type3UuidFactory.fromSNOMED(r.getRelSnomedCode()), 
								relationshipMetadata.getProperty("Snomed Code").getUUID());
					}
					
					if (r.getInverseRelSnomedCode() != null)
					{
						eConcepts_.addUuidAnnotation(concept, Type3UuidFactory.fromSNOMED(r.getInverseRelSnomedCode()), 
								relationshipMetadata.getProperty("Inverse Snomed Code").getUUID());
					}
				}
			});
		}
		
		eConcepts_.loadMetaDataItems(ptRelationships_, metaDataRoot_, dos_);
		eConcepts_.loadMetaDataItems(ptRelationshipQualifiers_, metaDataRoot_, dos_);
	}
	
	protected void processSemanticTypes(EConcept concept, ResultSet rs) throws SQLException
	{
		while (rs.next())
		{
			TkRefexUuidMember annotation = eConcepts_.addUuidAnnotation(concept, semanticTypes_.get(rs.getString("TUI")), ptAttributes_.getProperty("STY").getUUID());
			if (rs.getString("ATUI") != null)
			{
				eConcepts_.addStringAnnotation(annotation, rs.getString("ATUI"), ptAttributes_.getProperty("ATUI").getUUID(), false);
			}
			if (rs.getObject("CVF") != null)  //might be an int or a string
			{
				eConcepts_.addStringAnnotation(annotation, rs.getString("CVF"), ptAttributes_.getProperty("CVF").getUUID(), false);
			}
		}
		rs.close();
	}
	
	
	/**
	 * @param isCUI - true for CUI, false for AUI
	 * @throws SQLException
	 */
	protected void addRelationships(EConcept concept, ResultSet rs, boolean lookedUp2) throws SQLException
	{	
		while (rs.next())
		{
			String cui1 = rs.getString(isRxNorm ? "RXCUI1" : "CUI1");
			String aui1 = rs.getString(isRxNorm ? "RXAUI1" : "AUI1");
			String stype1 = rs.getString("STYPE1");
			String rel = rs.getString("REL");
			String cui2 = rs.getString(isRxNorm ? "RXCUI2" : "CUI2");
			String aui2 = rs.getString(isRxNorm ? "RXAUI2" : "AUI2");
			String stype2 = rs.getString("STYPE2");
			String rela = rs.getString("RELA");
			String rui = rs.getString("RUI");
			String srui = rs.getString("SRUI");
			String sab = rs.getString("SAB");
			String sl = rs.getString("SL");
			String rg = rs.getString("RG");
			String dir = rs.getString("DIR");
			String suppress = rs.getString("SUPPRESS");
			String cvf = rs.getObject("CVF") == null ? null : rs.getString("CVF");  //integer or string
			
			
			String targetCui = lookedUp2 ? cui1 : cui2;
			String targetAui = lookedUp2 ? aui1 : aui2;
			
			String sourceCui = lookedUp2 ? cui2 : cui1;
			String sourceAui = lookedUp2 ? aui2 : aui1;
			
			if (!lookedUp2)
			{
				rel = reverseRel(rel);
				rela = reverseRel(rela);
			}
			
			if (isRelPrimary(rel, rela))
			{
				//This can happen when the reverse of the rel equals the rel... sib/sib
				if (relCheckIsRelLoaded(rel, rela, sourceCui + sourceAui, targetCui + targetAui, (targetAui == null ? "CUI" : "AUI")))
				{
					continue;
				}
				UUID targetConcept = ConverterUUID.createNamespaceUUIDFromString((targetAui == null ? "CUI" + targetCui : "AUI" + targetAui), true);
				TkRelationship r = eConcepts_.addRelationship(concept, (rui != null ? ConverterUUID.createNamespaceUUIDFromString("RUI:" + rui) : null),
						targetConcept, ptRelationships_.getProperty(rel).getUUID(), null, null, null);
				
				eConcepts_.addStringAnnotation(r, stype1, ptAttributes_.getProperty("STYPE1").getUUID(), false);
				eConcepts_.addStringAnnotation(r, stype2, ptAttributes_.getProperty("STYPE2").getUUID(), false);
				if (rela != null)
				{
					eConcepts_.addUuidAnnotation(r, ptRelationshipQualifiers_.getProperty(rela).getUUID(), ptAttributes_.getProperty("RELA Label").getUUID());
				}
				if (rui != null)
				{
					eConcepts_.addAdditionalIds(r, rui, ptIds_.getProperty("RUI").getUUID());
					satRelStatement_.clearParameters();
					satRelStatement_.setString(1, rui);
					ResultSet nestedRels = satRelStatement_.executeQuery();
					processSAT(r, nestedRels);
				}
				if (!isRxNorm && srui != null)
				{
					eConcepts_.addStringAnnotation(r, srui, ptAttributes_.getProperty("SRUI").getUUID(), false);
				}
				eConcepts_.addUuidAnnotation(r, ptSABs_.getProperty(sab).getUUID(), ptAttributes_.getProperty("SAB").getUUID());
				if (!isRxNorm && sl != null && !sl.equals(sab))  //I don't  think this ever actually happens
				{
					eConcepts_.addUuidAnnotation(r, ptSABs_.getProperty(sab).getUUID(), ptAttributes_.getProperty("SL").getUUID());
				}
				if (rg != null)
				{
					eConcepts_.addStringAnnotation(r, rg, ptAttributes_.getProperty("RG").getUUID(), false);
				}
				if (dir != null)
				{
					eConcepts_.addStringAnnotation(r, dir, ptAttributes_.getProperty("DIR").getUUID(), false);
				}
				if (suppress != null)
				{
					eConcepts_.addUuidAnnotation(r, ptSuppress_.getProperty(suppress).getUUID(), ptAttributes_.getProperty("SUPPRESS").getUUID());
				}
				if (cvf != null)
				{
					if (isRxNorm)
					{
						
					
						if (cvf.equals("4096"))
						{
							//TODO
							//eConcepts_.addRefsetMember(cpcRefsetConcept_, r.getPrimordialComponentUuid(), null, true, null);
						}
						else
						{
							throw new RuntimeException("Unexpected value in RXNSAT cvf column '" + cvf + "'");
						}
					}
					else
					{
						eConcepts_.addStringAnnotation(r, cvf, ptAttributes_.getProperty("CVF").getUUID(), false);
					}
				}
				relCheckLoadedRel(rel, rela, sourceCui + sourceAui, targetCui + targetAui, (targetAui == null ? "CUI" : "AUI"));
			}
			else
			{
				relCheckSkippedRel(rel, rela, sourceCui + sourceAui, targetCui + targetAui, (targetAui == null ? "CUI" : "AUI"));
			}
		}
		rs.close();
	}
	
	private boolean isRelPrimary(String relName, String relaName)
	{
		if (relaName != null)
		{
			return nameToRel_.get(relaName).getFSNName().equals(relaName);
		}
		else
		{
			return nameToRel_.get(relName).getFSNName().equals(relName);
		}
	}
	
	private String reverseRel(String eitherRelType)
	{
		if (eitherRelType == null)
		{
			return null;
		}
		Relationship r = nameToRel_.get(eitherRelType);
		if (r.getFSNName().equals(eitherRelType))
		{
			return r.getInverseFSNName();
		}
		else if (r.getInverseFSNName().equals(eitherRelType))
		{
			return r.getFSNName();
		}
		else
		{
			throw new RuntimeException("gak");
		}
		
	}
	
	private UUID hashRelationship(String rel, String rela, String source, String target, String codeType)
	{
		return UUID.nameUUIDFromBytes(new String(rel + rela + source + target + codeType).getBytes());
	}
	
	private void relCheckLoadedRel(String rel, String rela, String source, String target, String codeType)
	{
		UUID hash = hashRelationship(rel, rela, source, target, codeType);
		loadedRels_.add(hash);
		skippedRels_.remove(hash);
	}
	
	private boolean relCheckIsRelLoaded(String rel, String rela, String source, String target, String codeType)
	{
		return loadedRels_.contains(hashRelationship(rel, rela, source, target, codeType));
	}

	/**
	 * Call this when a rel wasn't added because the rel was listed with the inverse name, rather than the primary name. 
	 */
	private void relCheckSkippedRel(String rel, String rela, String source, String target, String codeType)
	{
		//Get the primary rel name, add it to the skip list
		String primary = nameToRel_.get(rel).getFSNName();
		String primaryExtended = null;
		if (rela != null)
		{
			primaryExtended = nameToRel_.get(rela).getFSNName();
		}
		
		//also reverse the cui2 / cui1
		skippedRels_.add(hashRelationship(primary, primaryExtended, target, source, codeType));
	}
	
	private void checkRelationships()
	{
		//if the inverse relationships all worked properly, skipped should be empty when loaded is subtracted from it.
		for (UUID uuid : loadedRels_)
		{
			skippedRels_.remove(uuid);
		}
		
		if (skippedRels_.size() > 0)
		{
			ConsoleUtil.printErrorln("Relationship design error - " +  skippedRels_.size() + " were skipped that should have been loaded");
		}
		else
		{
			ConsoleUtil.println("Yea! - no missing relationships!");
		}
	}
}
