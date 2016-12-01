package com.datamelt.hadoop.data;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Logger;
import com.datamelt.util.RowFieldCollection;

/**
 * represents a record or row of data.
 * 
 * details of the fields, the fields that make up the key and the field that is used
 * as the source for calculations are defined in a properties file.
 * 
 * the properties file will be processed in the static block of this class.
 * 
 * 
 * @author	uwe geercken, last update: 2016-11-29
 *
 */
public class DataRecord
{
	// the name of the property file which defines properties for this
	// data processing process
	public static final String 			PROPERTY_FILENAME				= "config.properties";
	
	// names (keys) of properties in the files
	public static final String 			PROPERTY_HEADERFIELDS			= "headerfields";
	public static final String 			PROPERTY_HEADERFIELD_TYPES		= "headerfieldtypes";
	public static final String 			PROPERTY_KEYFIELDS				= "keyfields";
	public static final String 			PROPERTY_CALCULATIONFIELD		= "calculationfield";
	public static final String 			PROPERTY_VALUES_SEPARATOR		= ",";
	public static final String 			PROPERTY_KEY_EXTENSION_TYPE		= "type";
	public static final String 			TYPE_UNDEFINED					= "[undefined]";
	public static final String			DEFAULT_DATE_FORMAT				= "yyyy-MM-dd";
	
	// if the key is constructed of multiple fields this separator is used
	private static final String			KEY_SEPERATOR					=";";
	// the names of the fields that make up the header row
	private static String[]				headerFields					= null;
	// the name of the fields that make up the key
	private static String[]				keyFields						= null;
	// the index numbers of the key fields 
	private static int[]				keyFieldIndexes					= null;
	// holds the properties from a file which is inside the jar library
	private static Properties			properties						= new Properties();
	// for logging
	private static Logger 				logger 							= Logger.getLogger(DataRecord.class);
	// for date formating
	private static SimpleDateFormat 	sdf 							= new SimpleDateFormat(DEFAULT_DATE_FORMAT);
	
	// static block is executed once for this class
	// which is important as it is used in the mapping phase
	static 
	{
		try
		{
			// load properties from the properties file (in the jar file)
			loadProperties();

			// get the header fields from the property and
			// split them in an array of fields
			String headerFieldsValue = getProperty(PROPERTY_HEADERFIELDS);
			if(headerFieldsValue!=null)
			{
				headerFields = headerFieldsValue.split(PROPERTY_VALUES_SEPARATOR);
			}
			else
			{
				logger.error("no header fields defined in the properties file [" + PROPERTY_FILENAME + "]");
			}
			
			// load the key fields from the property and
			// split them in an array of fields
			String allKeyFields = getProperty(PROPERTY_KEYFIELDS);
			if(allKeyFields!=null)
			{
				// split the value from the properties file into individual values
				keyFields = allKeyFields.split(PROPERTY_VALUES_SEPARATOR);
				
				// the key fields are all string values. we try to find the relevant index in the array for the field
				keyFieldIndexes = new int[keyFields.length];
				for(int i=0;i<keyFields.length;i++)
				{
					int keyFieldIndex = findFieldIndexByFieldName(keyFields[i]);
					if(keyFieldIndex>-1)
					{
						keyFieldIndexes[i] = keyFieldIndex;
					}
					else
					{
						logger.error("key field [" + keyFields[i] + "] does not exist in the provided list of header fields");
					}
				}
			}
			else
			{
				logger.error("no key fields defined in the properties file [" + PROPERTY_FILENAME + "]");
			}
		}
		catch(Exception ex)
		{
			logger.error("error initializing EpxRecord class from static block");
		}
	}
		
	/**
	 * construct a key for this data record based on the
	 * key fields defined in the properties file
	 * 
	 * @return 				key for the current data record
	 * @throws Exception	exception if a defined key field is not existing
	 */
	public static String getKey(RowFieldCollection collection) throws Exception
	{
		StringBuffer key = new StringBuffer();
		if(keyFieldIndexes!=null)
		{
			for(int i=0;i<keyFieldIndexes.length;i++)
			{
				if(keyFieldIndexes[i]<collection.getFields().size())
				{
					// get a field from the collection
					Object fieldValue = collection.getFieldValue(keyFieldIndexes[i]);
					
					// if the field is of type date, then format it with the default format
					if(fieldValue instanceof Date)
					{
						key.append(sdf.format(fieldValue));
					}
					else
					{
						key.append(fieldValue);
					}
					if(i<keyFieldIndexes.length-1)
					{
						key.append(KEY_SEPERATOR);
					}
				}
				else
				{
					logger.error("the field [" + keyFields[i] + "] does not exist in the data record");
				}
			}
		}
		else
		{
			logger.error("no key fields defined - can not construct key");
		}
		return key.toString();
	}
			
	/**
	 * load properties from file. the file is located in the jar library
	 * created from this package of classes
	 * 
	 * @throws Exception
	 */
	private static void loadProperties() throws Exception
    {
		InputStream inputStream = DataRecord.class.getResourceAsStream(PROPERTY_FILENAME);
		// this also works:
		//InputStream inputStream = DataRecord.class.getResourceAsStream("/com/datamelt/hadoop/data/" + PROPERTY_FILENAME);
		if (inputStream != null)
		{
			try
			{
				logger.info("loading DataRecord properties from file [" + PROPERTY_FILENAME +"]");
				properties.load(inputStream);
			}
			catch(Exception ex)
			{
				logger.error("error loading properties file [" + PROPERTY_FILENAME + "]");
			}
		}
		else
		{
			logger.error("error loading properties file [" + PROPERTY_FILENAME + "]");
		}
    }
	
	/**
	 * get a property by specifying its key
	 * 
	 * @param propertyKey	a key in the properties file
	 * @return				the properties value
	 */
	public static String getProperty(String propertyKey)
	{
		return properties.getProperty(propertyKey);
	}
	
	/**
	 * for each field of a row/record there needs to be a definition of its type
	 * 
	 * after the property for the header fields has been read, this method
	 * is used to retrieve the property of the type of the field, by specifying
	 * the name of the field.
	 * 
	 * types can be string, integer, long, double, float, date, boolean, bigdecimal
	 * 
	 * @param field			the name of a field as specified in the header fields property
	 * @return				the properties value or type undefined
	 */
	public static String getPropertyFieldType(String field)
	{
		String fieldType = properties.getProperty(PROPERTY_HEADERFIELDS + "." + field + "." + PROPERTY_KEY_EXTENSION_TYPE);
		if(fieldType!=null && !fieldType.trim().equals(""))
		{
			return fieldType;
		}
		else
		{
			return TYPE_UNDEFINED;
		}
	}
	
	/**
	 * find the number that corresponds to the field specified
	 * by its name
	 * 
	 * @param fieldName
	 * @return
	 */
	private static int findFieldIndexByFieldName(String fieldName)
	{
		int index = -1;
		if(keyFields!=null && keyFields.length>0)
		{
			for(int i=0;i<headerFields.length;i++)
			{
				String headerFieldName = headerFields[i];
				if(headerFieldName!=null && headerFieldName.toLowerCase().equals(fieldName.toLowerCase()))
				{
					index = i;
					break;
				}
			}
		}
		return index;
	}

}
