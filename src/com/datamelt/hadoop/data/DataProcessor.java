/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */ 
package com.datamelt.hadoop.data;

import java.io.File;

import org.apache.log4j.Logger;
import java.io.IOException;
import java.net.URI;
import java.util.zip.ZipFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.datamelt.rules.engine.BusinessRulesEngine;
import com.datamelt.util.RowField;
import com.datamelt.util.RowFieldCollection;
import com.datamelt.util.Splitter;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * used to process data using mapreduce. contains classes for mapping
 * and reducing.
 * 
 * for filtering data records, the business rule engine JaRE is used. Rules
 * are stored externally in a zip file, so nothing is hardcoded here. Rules
 * and (complex) logic can be designed in a web application.
 * 
 * @author	uwe geercken, last update: 2016-11-29
 *
 */
public class DataProcessor
{
	private static Logger logger = Logger.getLogger(DataProcessor.class);
	
	public static final String DISTCACHE_RULES_PROJECTFILE		= "rulesprojectfile";
	public static final String RULEENGINE_LIBRARY 				= "jare0.79.jar";
	
	private static final double DEFAULT_CALCULATIONFIELD_VALUE 	= 1.0;
	
	private static BusinessRulesEngine bre;
	
	/**
	 * mapper part to prepare data for the reduce phase by assigning
	 * key and value pairs.
	 * 
	 * @author uwe geercken, last update: 2016-11-29
	 *
	 */
	public static class DataMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
    {
		// array containing the header fields
		private static String[] headerFields;
		// array containing the field types
		private static String[] headerFieldTypes;
		
		/**
		 * get the project zip file for the ruleengine
		 * from the distributed cache
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			// set the log level - the default is INFO
			//DataProcessor.logger.setLevel(Level.DEBUG);
			
			// get the definition of the header fields
    		headerFields = DataRecord.getProperty(DataRecord.PROPERTY_HEADERFIELDS).split(",");
    		logger.debug("property file defines [" + headerFields.length + "] header fields");
    		// get the definition of types for each header field
    		
    		// we have as many types as we have fields
    		headerFieldTypes = new String[headerFields.length];
    		// loop over header fields and get the corresponding types
    		for(int i=0;i<headerFields.length;i++)
			{
    			// get the field type
    			headerFieldTypes[i] = DataRecord.getPropertyFieldType(headerFields[i]);
    			
    			// if the property was not found we log the error and set the
    			// type to "string" as a default
    			if(headerFieldTypes[i].equals(DataRecord.TYPE_UNDEFINED))
    			{
    				logger.error("undefined field type for field [" + headerFields[i] +"] in propertiesfile");
    				// change type to default
    				headerFieldTypes[i] = Splitter.FIELDTYPE_STRING;
    				logger.debug("changed undefined field type for field [" + headerFields[i] +"] to default type [" + Splitter.FIELDTYPE_STRING + "]");
    			}
			}
    		
    		// get files from distributed cache
			URI[] files = context.getCacheFiles();
			logger.debug("found [" + files.length + "] files in distributed cache");

			// indicator if the ruleengine project file was found
			int found=0;
			
			// get the ruleengine project file from the cache
			final String distributedCacheFilename =	context.getConfiguration().get(DISTCACHE_RULES_PROJECTFILE);
			for (URI uri: files) 
			{
				File file = new File(uri.getPath());
				if (file.getName().equals(distributedCacheFilename)) 
				{
					logger.debug("found file in distributed cache [" + file.getName() + "]");
					found = 1;
					initializeRuleEngine(new ZipFile(file.getPath()));
					break;
				}
			}
			if(found==0)
			{
				logger.error("ruleengine project file not found in distributed cache");	
			}
		}
		
		/**
		 * initialize the static rule engine instance with a
		 * a zip file, created with the business rules maintenance tool
		 * 
		 * @param file	a zip file for the rule engine
		 */
		private void initializeRuleEngine(ZipFile file)
		{
			logger.info("Initializing JaRE Business Rule Engine v" + BusinessRulesEngine.getVersion() + " with project file [" + file.getName() + "]");
			try
			{
				bre = new BusinessRulesEngine(file);
				// set preserve execution results to fast, so that the details of the ruleengine
				// results will NOT be stored (for performance reasons and they are not required here)
				bre.setPreserveRuleExcecutionResults(false);
			}
			catch(Exception ex)
			{
				 logger.error(ex.getMessage());
			}
		}
		
		/**
		 * Split the data record in its individual fields, get the definition of the
		 * header fields and create a RowFieldCollection from it. This will then be
		 * passed to the rule engine and when the rule engine runs it evaluates the
		 * rules against the data.
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
        	try
        	{
        		// split data record
        		Splitter splitter = new Splitter(Splitter.TYPE_COMMA_SEPERATED,Splitter.SEPERATOR_TAB);

        		// create a collection of the fields taking into account the header fields and the field types
        		RowFieldCollection collection = new RowFieldCollection(headerFields,splitter.getFields(value.toString(),headerFieldTypes));
        		
        		// run the Rule Engine on the collection we just created for the data record
    	    	bre.run(key.toString(), collection);
    	    	
    	    	// if the run of the rule engine has no groups which failed, write key and value to the context
    	    	if(bre.getNumberOfGroupsFailed()==0)
    	    	{
    	    		// defines a field of the data row
    	    		RowField valueField = null;
    	    		try
    	    		{
    	    			// get the field that contains the value for calculations
    	    			valueField = collection.getField(DataRecord.getProperty(DataRecord.PROPERTY_CALCULATIONFIELD));
    	    		}
    	    		catch(Exception ex)
    	    		{
    	    			// value field was not found
    	        		logger.debug("calculation field undefined in properties file");
    	    		}
    	    		
    	    		// if no keyfields have been defined, use the key defined by the map process
    	    		String rowKey = DataRecord.getKey(collection);
    	    		if(rowKey==null || rowKey.equals(""))
    	    		{
    	        		logger.debug("keyfields undefined in properties file");
    	    			rowKey = key.toString();
    	    		}
    	    		
            		logger.debug("row field collection for key [" + rowKey + "] consists of [" + collection.getNumberOfFields() + "] fields");
            		
    	    		// if no value field is defined, then simply count the data record as one (1)
    	    		if(valueField==null)
    	    		{
    	    			// get the constructed key and value for the current row of data
        	    		context.write(new Text(rowKey), new DoubleWritable(DEFAULT_CALCULATIONFIELD_VALUE));
        	    		logger.debug("map: writing to context: key=[" + rowKey + "], value=[" + DEFAULT_CALCULATIONFIELD_VALUE +"]");
    	    		}
    	    		else
    	    		{
    	    			// get the constructed key and value for the current row of data
        	    		context.write(new Text(rowKey), new DoubleWritable((Double)valueField.getValue()));
        	    		logger.debug("map: writing to context: key=[" + rowKey + "], value=[" + valueField.getValue() + "]");
    	    		}
    	    	}
    	    	
    	    	// clear the results of the ruleengine execution before the next data arrives
    	    	bre.getRuleExecutionCollection().clear();
        	}
        	catch(Exception ex)
        	{
        		logger.error(ex.getMessage());
        	}
        }
    }

	/**
	 * reduce class. sum up values for the defined key
	 * 
	 * @author uwe geercken, last update: 2016-11-29
	 *
	 */
    public static class DataReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
    {
    	/**
    	 * reduce the keys and values from the mapping phase
    	 * 
    	 * values will be summed in the reduce phase
    	 */
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
        {
            double sum = 0;
            // sum up values
            for (DoubleWritable value : values)
            {
            	sum += value.get();
            }
            
            logger.debug("reduce: writing to context: key=[" + key + "], value=[" + sum +"]");
            
            // write to the context as key and double
            context.write(key, new DoubleWritable(sum));
        }
    }
    
    
    public static void main(String[] args) throws Exception
    {
    	Configuration conf = new Configuration();
        // set the output separator
    	conf.set("mapreduce.output.textoutputformat.separator", ";");
        
        Job job = Job.getInstance(conf, "data processor");
        
        job.setJarByClass(DataProcessor.class);
        job.setMapperClass(DataMapper.class);
        job.setReducerClass(DataReducer.class);
        job.setCombinerClass(DataReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // we use the JaRE business rule engine for evaluating the data
        job.addArchiveToClassPath(new Path(RULEENGINE_LIBRARY));
        
        // the ruleengine project file - put it in the distributed cache
        Path rulesFile = new Path(args[2]);
        job.addCacheFile(rulesFile.toUri());
        job.getConfiguration().set(DISTCACHE_RULES_PROJECTFILE, rulesFile.getName());
        
        // input and output path of the job
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);

    }
}
