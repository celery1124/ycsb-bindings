/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved. 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.workloads;

import java.util.Properties;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.ExponentialGenerator;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.ConstantIntegerGenerator;
import com.yahoo.ycsb.generator.HotspotIntegerGenerator;
import com.yahoo.ycsb.generator.HistogramGenerator;
import com.yahoo.ycsb.generator.NumberGenerator;
import com.yahoo.ycsb.generator.ScrambledZipfianGenerator;
import com.yahoo.ycsb.generator.SkewedLatestGenerator;
import com.yahoo.ycsb.generator.UniformLongGenerator;
import com.yahoo.ycsb.generator.ZipfianGenerator;
import com.yahoo.ycsb.measurements.Measurements;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
/**
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
 */

/**
 * The core benchmark scenario. Represents a set of clients doing simple CRUD operations. The relative 
 * proportion of different kinds of operations, and other properties of the workload, are controlled
 * by parameters specified at runtime.
 * 
 * Properties to control the client:
 * <UL>
 * <LI><b>fieldcount</b>: the number of fields in a record (default: 10)
 * <LI><b>fieldlength</b>: the size of each field (default: 100)
 * <LI><b>readallfields</b>: should reads read all fields (true) or just one (false) (default: true)
 * <LI><b>writeallfields</b>: should updates and read/modify/writes update all fields (true) or just one (false) (default: false)
 * <LI><b>readproportion</b>: what proportion of operations should be reads (default: 0.95)
 * <LI><b>updateproportion</b>: what proportion of operations should be updates (default: 0.05)
 * <LI><b>insertproportion</b>: what proportion of operations should be inserts (default: 0)
 * <LI><b>scanproportion</b>: what proportion of operations should be scans (default: 0)
 * <LI><b>readmodifywriteproportion</b>: what proportion of operations should be read a record, modify it, write it back (default: 0)
 * <LI><b>requestdistribution</b>: what distribution should be used to select the records to operate on - uniform, zipfian, hotspot, or latest (default: uniform)
 * <LI><b>maxscanlength</b>: for scans, what is the maximum number of records to scan (default: 1000)
 * <LI><b>scanlengthdistribution</b>: for scans, what distribution should be used to choose the number of records to scan, for each scan, between 1 and maxscanlength (default: uniform)
 * <LI><b>insertorder</b>: should records be inserted in order by key ("ordered"), or in hashed order ("hashed") (default: hashed)
 * </ul> 
 */
public class ReplayWorkload extends Workload
{

	/**
	 * The name of the database table to run queries against.
	 */
	public static final String TABLENAME_PROPERTY="table";

	/**
	 * The default name of the database table to run queries against.
	 */
	public static final String TABLENAME_PROPERTY_DEFAULT="usertable";

	public static String table;


	/**
	 * The name of the property for the number of fields in a record.
	 */
	public static final String FIELD_COUNT_PROPERTY="fieldcount";
	
	/**
	 * Default number of fields in a record. 
         * 
         * This default value is set to 1 when sizefromtrace is set to 1
	 */
	public static final String FIELD_COUNT_PROPERTY_DEFAULT="10";

	int fieldcount;

	private List<String> fieldnames;

	/**
	 * The name of the property for the field length distribution. Options are "uniform", "zipfian" (favoring short records), "constant", and "histogram".
	 * 
	 * If "uniform", "zipfian" or "constant", the maximum field length will be that specified by the fieldlength property.  If "histogram", then the
	 * histogram will be read from the filename specified in the "fieldlengthhistogram" property.
	 */
	public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY="fieldlengthdistribution";
	/**
	 * The default field length distribution.
	 */
	public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "constant";

	/**
	 * The name of the property for the length of a field in bytes.
	 */
	public static final String FIELD_LENGTH_PROPERTY="fieldlength";
	/**
	 * The default maximum length of a field in bytes.
	 */
	public static final String FIELD_LENGTH_PROPERTY_DEFAULT="100";

	/**
	 * The name of a property that specifies the filename containing the field length histogram (only used if fieldlengthdistribution is "histogram").
	 */
	public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY = "fieldlengthhistogram";
	/**
	 * The default filename containing a field length histogram.
	 */
	public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT = "hist.txt";

	/**
	 * Generator object that produces field lengths.  The value of this depends on the properties that start with "FIELD_LENGTH_".
	 */
	NumberGenerator fieldlengthgenerator;
	
	/**
	 * The name of the property for deciding whether to read one field (false) or all fields (true) of a record.
	 */
	public static final String READ_ALL_FIELDS_PROPERTY="readallfields";
	
	/**
	 * The default value for the readallfields property.
	 */
	public static final String READ_ALL_FIELDS_PROPERTY_DEFAULT="true";

	boolean readallfields;

	/**
	 * The name of the property for deciding whether to write one field (false) or all fields (true) of a record.
	 */
	public static final String WRITE_ALL_FIELDS_PROPERTY="writeallfields";
	
	/**
	 * The default value for the writeallfields property.
	 */
	public static final String WRITE_ALL_FIELDS_PROPERTY_DEFAULT="false";

	boolean writeallfields;

	/**
	 * The name of the property for deciding if the DB acts like a CACHE (true), default is false.
	 */
	public static final String AS_CACHE_PROPERTY="ascache";
	
	/**
	 * The default value for the ascache property.
	 */
	public static final String AS_CACHE_PROPERTY_DEFAULT="false";

	boolean ascache;

	/**
	 * The name of the property for deciding if use the timestamp/delays from the tracefile, default is false.
	 */
	public static final String WITH_TIMESTAMP_PROPERTY="withtimestamp";
	
	/**
	 * The default value for the withtimestamp property.
	 */
	public static final String WITH_TIMESTAMP_PROPERTY_DEFAULT="false";

	boolean withtimestamp;

	// EBG - 20160604 - Variable to keep the previous timestamp
	// long prevtimestamp;
	double prevtimestamp;
	
	// EBG - 20160613
	/**
	 * The name of the property for deciding if use the sleep times are precomputed in the tracefile, default is true.
	 */
	public static final String WITH_SLEEP_PROPERTY="withsleep";
	
	/**
	 * The default value for the withsleep property.
	 */
	public static final String WITH_SLEEP_PROPERTY_DEFAULT="true";

	boolean withsleep;

  /**
   * The name of the property for deciding whether to check all returned
   * data against the formation template to ensure data integrity.
   */
  public static final String DATA_INTEGRITY_PROPERTY = "dataintegrity";
  
  /**
   * The default value for the dataintegrity property.
   */
  public static final String DATA_INTEGRITY_PROPERTY_DEFAULT = "false";

  /**
   * Set to true if want to check correctness of reads. Must also
   * be set to true during loading phase to function.
   */
  private boolean dataintegrity;

  /**
   * Response values for data integrity checks.
   * Need to be multiples of 1000 to match bucket offsets of
   * measurements/OneMeasurementHistogram.java.
   */
  private final int DATA_INT_MATCH = 0;
  private final int DATA_INT_DEVIATE = 1000;
  private final int DATA_INT_UNEXPECTED_NULL = 2000;


	/**
	 * The name of the property for the proportion of transactions that are reads.
	 */
	public static final String READ_PROPORTION_PROPERTY="readproportion";
	
	/**
	 * The default proportion of transactions that are reads.	
	 */
	public static final String READ_PROPORTION_PROPERTY_DEFAULT="0.95";

	/**
	 * The name of the property for the proportion of transactions that are updates.
	 */
	public static final String UPDATE_PROPORTION_PROPERTY="updateproportion";
	
	/**
	 * The default proportion of transactions that are updates.
	 */
	public static final String UPDATE_PROPORTION_PROPERTY_DEFAULT="0.05";

	/**
	 * The name of the property for the proportion of transactions that are inserts.
	 */
	public static final String INSERT_PROPORTION_PROPERTY="insertproportion";
	
	/**
	 * The default proportion of transactions that are inserts.
	 */
	public static final String INSERT_PROPORTION_PROPERTY_DEFAULT="0.0";

	/**
	 * The name of the property for the proportion of transactions that are scans.
	 */
	public static final String SCAN_PROPORTION_PROPERTY="scanproportion";
	
	/**
	 * The default proportion of transactions that are scans.
	 */
	public static final String SCAN_PROPORTION_PROPERTY_DEFAULT="0.0";
	
	/**
	 * The name of the property for the proportion of transactions that are read-modify-write.
	 */
	public static final String READMODIFYWRITE_PROPORTION_PROPERTY="readmodifywriteproportion";
	
	/**
	 * The default proportion of transactions that are scans.
	 */
	public static final String READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT="0.0";
	
	/**
	 * The name of the property for the the distribution of requests across the keyspace. Options are "uniform", "zipfian" and "latest"
	 */
	public static final String REQUEST_DISTRIBUTION_PROPERTY="requestdistribution";
	
	/**
	 * The default distribution of requests across the keyspace
	 */
	public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT="uniform";

	/**
	 * The name of the property for the max scan length (number of records)
	 */
	public static final String MAX_SCAN_LENGTH_PROPERTY="maxscanlength";
	
	/**
	 * The default max scan length.
	 */
	public static final String MAX_SCAN_LENGTH_PROPERTY_DEFAULT="1000";
	
	/**
	 * The name of the property for the scan length distribution. Options are "uniform" and "zipfian" (favoring short scans)
	 */
	public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY="scanlengthdistribution";
	
	/**
	 * The default max scan length.
	 */
	public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT="uniform";
	
	/**
	 * The name of the property for the order to insert records. Options are "ordered" or "hashed"
	 */
	public static final String INSERT_ORDER_PROPERTY="insertorder";
	
	/**
	 * Default insert order.
	 */
	public static final String INSERT_ORDER_PROPERTY_DEFAULT="hashed";
	
	/**
   * Percentage data items that constitute the hot set.
   */
  public static final String HOTSPOT_DATA_FRACTION = "hotspotdatafraction";
  
  /**
   * Default value of the size of the hot set.
   */
  public static final String HOTSPOT_DATA_FRACTION_DEFAULT = "0.2";
  
  /**
   * Percentage operations that access the hot set.
   */
  public static final String HOTSPOT_OPN_FRACTION = "hotspotopnfraction";
  
  /**
   * Default value of the percentage operations accessing the hot set.
   */
  public static final String HOTSPOT_OPN_FRACTION_DEFAULT = "0.8";
	
	NumberGenerator keysequence;

	DiscreteGenerator operationchooser;

	NumberGenerator keychooser;

	Generator fieldchooser;

	CounterGenerator transactioninsertkeysequence;
	
	NumberGenerator scanlength;
	
	boolean orderedinserts;

	int recordcount;

	BufferedReader tracefile;

    private Measurements _measurements = Measurements.getMeasurements();
      
    /**agregado jose viteri 28/10/2016
         * 
         * 
         * 
         */
         
        /**
     * The name of the property to decide if the size of the objects are read from the trace, Default value false.
     */
    public static final String SIZE_FROM_TRACE="sizefromtrace";
     
    /**
     * The default value of the size from trace property
     */
    public static final String SIZE_FROM_TRACE_DEFAULT="false";
 
    boolean sizefromtrace; 
    
    
    
	protected static NumberGenerator getFieldLengthGenerator(Properties p) throws WorkloadException{
		NumberGenerator fieldlengthgenerator;
		String fieldlengthdistribution = p.getProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY, FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
		int fieldlength=Integer.parseInt(p.getProperty(FIELD_LENGTH_PROPERTY,FIELD_LENGTH_PROPERTY_DEFAULT));
		String fieldlengthhistogram = p.getProperty(FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY, FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT);
		if(fieldlengthdistribution.compareTo("constant") == 0) {
			fieldlengthgenerator = new ConstantIntegerGenerator(fieldlength);
		} else if(fieldlengthdistribution.compareTo("uniform") == 0) {
			fieldlengthgenerator = new UniformLongGenerator(1, fieldlength);
		} else if(fieldlengthdistribution.compareTo("zipfian") == 0) {
			fieldlengthgenerator = new ZipfianGenerator(1, fieldlength);
		} else if(fieldlengthdistribution.compareTo("histogram") == 0) {
			try {
				fieldlengthgenerator = new HistogramGenerator(fieldlengthhistogram);
			} catch(IOException e) {
				throw new WorkloadException("Couldn't read field length histogram file: "+fieldlengthhistogram, e);
			}
		} else {
			throw new WorkloadException("Unknown field length distribution \""+fieldlengthdistribution+"\"");
		}
		return fieldlengthgenerator;
	}
	
	/**
	 * Initialize the scenario. 
	 * Called once, in the main client thread, before any operations are started.
	 */
	public void init(Properties p) throws WorkloadException
	{
		table = p.getProperty(TABLENAME_PROPERTY,TABLENAME_PROPERTY_DEFAULT);
                
                
                
                //agregado 10/11/2016 jose viteri
                sizefromtrace = Boolean.parseBoolean(p.getProperty(SIZE_FROM_TRACE,SIZE_FROM_TRACE_DEFAULT));
                
                // agregado jose viteri 16/11/16
                //set fieldCount   
                if (sizefromtrace){
                    fieldcount = 1 ;
                }else{
                    fieldcount=Integer.parseInt(p.getProperty(FIELD_COUNT_PROPERTY,FIELD_COUNT_PROPERTY_DEFAULT));
                }
                
		
                
		fieldnames = new ArrayList<String>();
    		for (int i = 0; i < fieldcount; i++) {
    		    fieldnames.add("field" + i);
    		}
		fieldlengthgenerator = ReplayWorkload.getFieldLengthGenerator(p);
		
		double readproportion=Double.parseDouble(p.getProperty(READ_PROPORTION_PROPERTY,READ_PROPORTION_PROPERTY_DEFAULT));
		double updateproportion=Double.parseDouble(p.getProperty(UPDATE_PROPORTION_PROPERTY,UPDATE_PROPORTION_PROPERTY_DEFAULT));
		double insertproportion=Double.parseDouble(p.getProperty(INSERT_PROPORTION_PROPERTY,INSERT_PROPORTION_PROPERTY_DEFAULT));
		double scanproportion=Double.parseDouble(p.getProperty(SCAN_PROPORTION_PROPERTY,SCAN_PROPORTION_PROPERTY_DEFAULT));
		double readmodifywriteproportion=Double.parseDouble(p.getProperty(READMODIFYWRITE_PROPORTION_PROPERTY,READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT));
		recordcount=Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));
		if(recordcount == 0)
		    recordcount = Integer.MAX_VALUE;
		String requestdistrib=p.getProperty(REQUEST_DISTRIBUTION_PROPERTY,REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
		int maxscanlength=Integer.parseInt(p.getProperty(MAX_SCAN_LENGTH_PROPERTY,MAX_SCAN_LENGTH_PROPERTY_DEFAULT));
		String scanlengthdistrib=p.getProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY,SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
		
		int insertstart=Integer.parseInt(p.getProperty(INSERT_START_PROPERTY,INSERT_START_PROPERTY_DEFAULT));
		
		readallfields=Boolean.parseBoolean(p.getProperty(READ_ALL_FIELDS_PROPERTY,READ_ALL_FIELDS_PROPERTY_DEFAULT));
		writeallfields=Boolean.parseBoolean(p.getProperty(WRITE_ALL_FIELDS_PROPERTY,WRITE_ALL_FIELDS_PROPERTY_DEFAULT));
                
		// EBG - 2016-06-04
		// Properties for cache behaviour and for using timestamp from tracefile
		ascache=Boolean.parseBoolean(p.getProperty(AS_CACHE_PROPERTY,AS_CACHE_PROPERTY_DEFAULT));
		withtimestamp=Boolean.parseBoolean(p.getProperty(WITH_TIMESTAMP_PROPERTY,WITH_TIMESTAMP_PROPERTY_DEFAULT));
		withsleep=Boolean.parseBoolean(p.getProperty(WITH_SLEEP_PROPERTY,WITH_SLEEP_PROPERTY_DEFAULT));
		
    		dataintegrity = Boolean.parseBoolean(p.getProperty(DATA_INTEGRITY_PROPERTY, DATA_INTEGRITY_PROPERTY_DEFAULT));
    		//Confirm that fieldlengthgenerator returns a constant if data
    		//integrity check requested.
    		if (dataintegrity && !(p.getProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY, FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT)).equals("constant"))
    		{
    		  System.err.println("Must have constant field size to check data integrity.");
    		  System.exit(-1);
    		}

		if (p.getProperty(INSERT_ORDER_PROPERTY,INSERT_ORDER_PROPERTY_DEFAULT).compareTo("hashed")==0)
		{
			orderedinserts=false;
		}
		else if (requestdistrib.compareTo("exponential")==0)
		{
                    double percentile = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY,
                                                                         ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT));
                    double frac       = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY,
                                                                         ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT));
                    keychooser = new ExponentialGenerator(percentile, recordcount*frac);
		}
		else
		{
			orderedinserts=true;
		}

		keysequence=new CounterGenerator(insertstart);
		operationchooser=new DiscreteGenerator();
		if (readproportion>0)
		{
			operationchooser.addValue(readproportion,"READ");
		}

		if (updateproportion>0)
		{
			operationchooser.addValue(updateproportion,"UPDATE");
		}

		if (insertproportion>0)
		{
			operationchooser.addValue(insertproportion,"INSERT");
		}
		
		if (scanproportion>0)
		{
			operationchooser.addValue(scanproportion,"SCAN");
		}
		
		if (readmodifywriteproportion>0)
		{
			operationchooser.addValue(readmodifywriteproportion,"READMODIFYWRITE");
		}

		transactioninsertkeysequence=new CounterGenerator(recordcount);
		if (requestdistrib.compareTo("uniform")==0)
		{
			keychooser=new UniformLongGenerator(0,recordcount-1);
		}
		else if (requestdistrib.compareTo("zipfian")==0)
		{
			//it does this by generating a random "next key" in part by taking the modulus over the number of keys
			//if the number of keys changes, this would shift the modulus, and we don't want that to change which keys are popular
			//so we'll actually construct the scrambled zipfian generator with a keyspace that is larger than exists at the beginning
			//of the test. that is, we'll predict the number of inserts, and tell the scrambled zipfian generator the number of existing keys
			//plus the number of predicted keys as the total keyspace. then, if the generator picks a key that hasn't been inserted yet, will
			//just ignore it and pick another key. this way, the size of the keyspace doesn't change from the perspective of the scrambled zipfian generator
			
			int opcount=Integer.parseInt(p.getProperty(Client.OPERATION_COUNT_PROPERTY));
			int expectednewkeys=(int)(((double)opcount)*insertproportion*2.0); //2 is fudge factor
			
			keychooser=new ScrambledZipfianGenerator(recordcount+expectednewkeys);
		}
		else if (requestdistrib.compareTo("latest")==0)
		{
			keychooser=new SkewedLatestGenerator(transactioninsertkeysequence);
		}
		else if (requestdistrib.equals("hotspot")) 
		{
      			double hotsetfraction = Double.parseDouble(p.getProperty(
       			   HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
      			double hotopnfraction = Double.parseDouble(p.getProperty(
      			    HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
      			keychooser = new HotspotIntegerGenerator(0, recordcount - 1, 
      			    hotsetfraction, hotopnfraction);
    		}
		else
		{
			throw new WorkloadException("Unknown request distribution \""+requestdistrib+"\"");
		}

		fieldchooser=new UniformLongGenerator(0,fieldcount-1);
		
		if (scanlengthdistrib.compareTo("uniform")==0)
		{
			scanlength=new UniformLongGenerator(1,maxscanlength);
		}
		else if (scanlengthdistrib.compareTo("zipfian")==0)
		{
			scanlength=new ZipfianGenerator(1,maxscanlength);
		}
		else
		{
			throw new WorkloadException("Distribution \""+scanlengthdistrib+"\" not allowed for scan length");
		}
    		//Verify if "tracefile" properties is present.
		if (!(p.getProperty("tracefile")!= null))
    		{
    		  System.err.println("Must include a tracefile name in the workload properties file.");
    		  System.exit(-1);
    		}
		String traceFilename=p.getProperty("tracefile");
                String[] trace = null;
		try{
			tracefile = new BufferedReader(new FileReader(traceFilename));
                        // EBG - 20160405 - TO DO - IMPROVE
                        // I read the tracefile, take the first timestamp, and then reset the BufferedReader
			trace = tracefile.readLine().split(",");
                	double firsttimestamp = Double.valueOf(trace[2]);
			prevtimestamp = (long) (firsttimestamp*1000);
			prevtimestamp = firsttimestamp*1000;
			tracefile = new BufferedReader(new FileReader(traceFilename));
			System.out.println(prevtimestamp);

                }catch(Exception e){
                        e.printStackTrace();
                        File tmpDir = new File(traceFilename);
                        boolean exists = tmpDir.exists();
                        if (!exists) {
                          System.out.printf("Tracefile %s not exist", traceFilename);
                        }
                        else System.out.printf("Tracefile %s exist", traceFilename);
    		        System.err.println("Error with the tracefile.");
    		        System.exit(-1);
                }
	}

	public String buildKeyName(long keynum) {
 		if (!orderedinserts)
 		{
 			keynum=Utils.hash(keynum);
 		}
		return "user"+keynum;
	}
	
  /**
   * Builds a value for a randomly chosen field.
   * modificado 10/11/2016 jose viteri
   */
  private HashMap<String, ByteIterator> buildSingleValue(String key, long fieldSize) {
    HashMap<String,ByteIterator> value = new HashMap<String,ByteIterator>();
  
    String fieldkey = fieldnames.get(Integer.parseInt(fieldchooser.nextString()));
    ByteIterator data;
    // dataintegrity debe ser constante, por lo que no deberia modificarse
    if (dataintegrity) {
      data = new StringByteIterator(buildDeterministicValue(key, fieldkey, fieldSize));
    } else {
      //fill with random data
        data = new RandomByteIterator(fieldSize);
    }
    value.put(fieldkey,data);

    return value;    
  }

  /**
   * Builds values for all fields.
   * modificado 10/11/2016 jose viteri
   */
  private HashMap<String, ByteIterator> buildValues(String key, long fieldSize) {       
    HashMap<String,ByteIterator> values = new HashMap<String,ByteIterator>();
    
    
    for (String fieldkey : fieldnames) {
      ByteIterator data;
      if (dataintegrity) {
        data = new StringByteIterator(buildDeterministicValue(key, fieldkey, fieldSize));
      } else {
          
          data = new RandomByteIterator(fieldSize);      
      }
      values.put(fieldkey,data);
    }
    return values;
  }

  /**
   * Build a deterministic value given the key information.
   * modificado 10/11/2016 jose viteri
   */
  private String buildDeterministicValue(String key, String fieldkey, long size ) {
    int size_int = (int)size;
    StringBuilder sb = new StringBuilder(size_int);
    sb.append(key);
    sb.append(':');
    sb.append(fieldkey);
    while (sb.length() < size_int) {
      sb.append(':');
      sb.append(sb.toString().hashCode());
    }
    sb.setLength(size_int);

    return sb.toString();
  }
  
	/**
         * modificado 10/11/2016 jose viteri
	 * Do one insert operation. Because it will be called concurrently from multiple client threads, this 
	 * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each 
	 * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
	 * effects other than DB operations.
	 */
    public boolean doInsert(DB db, Object threadstate)
	{
                
                String[] trace = lectorTraces();
		String dbkey = trace[1];
		//String dbkey_unpadded = trace[1];
		//String dbkey = "AAAAAAAAAAAAAAAAAAAAAAA".substring(dbkey_unpadded.length()) + dbkey_unpadded;
                
                
                //jose viteri 16/11/16
                //define si se lee desde el trace
                long fieldSize = fieldlengthgenerator.nextValue().intValue();
                if (sizefromtrace){
                    fieldSize = Integer.parseInt(trace[3]);
                }

		HashMap<String, ByteIterator> values = buildValues(dbkey, fieldSize);
                //System.out.println("" + dbkey);
                //System.out.println(fieldSize);
		if (db.insert(table,dbkey,values) != null)
			return true;
		else
			return false;
	}

	/**
         * modificado 10/11/2016 jose viteri
	 * Do one transaction operation. Because it will be called concurrently from multiple client threads, this 
	 * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each 
	 * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
	 * effects other than DB operations.
	 */
	public boolean doTransaction(DB db, Object threadstate)
	{        
                String[] trace = lectorTraces();
		String op = trace[0];
		//String dbkey = trace[1];
                String dbkey_unpadded = trace[1];
                String dbkey = "AAAAAAAAAAAAAAAAAAAAAAA".substring(dbkey_unpadded.length()) + dbkey_unpadded;
		// EBG - 20160604
		// If "withtimestamp" is enabled, pause before sending the next request.
                
                
                //Jose viteri 16/11/16
                //define si se lee desde el trace
                long fieldSize = fieldlengthgenerator.nextValue().intValue();
                if (sizefromtrace){
                    fieldSize = Integer.parseInt(trace[3]);
                }
                
                
                
		long sleeptime = 0;
		if (withtimestamp) {
		   if (withsleep) 
		   {
			// EBG - 20160613
			// If "withsleep" is enabled, the the sleep time directly from the trace file.
			sleeptime = Long.valueOf(trace[2]);
		   }
		   else
		   {
			// EBG - 20160606
			// Calculate the sleep time by subtracting the timestamps from the tracefile.
                	double newtimestamp = (Double.valueOf(trace[2]))*1000;
		   	sleeptime = Math.round(newtimestamp - prevtimestamp);
		   	prevtimestamp = newtimestamp;
		   }
		   //System.out.println("Delay: " + sleeptime);
                   try{	
			Thread.sleep(sleeptime);
	           }catch(InterruptedException e){
                                e.printStackTrace();
                   }
                }

		if (op.compareTo("READ")==0)
		{
                    //modificado jose viteri 16/11/16
			doTransactionRead(db,dbkey, fieldSize);
		}
		else if (op.compareTo("UPDATE")==0)
		{
                    //modificado jose viteri 16/11/16
			doTransactionUpdate(db,dbkey, fieldSize);
		}
		else if (op.compareTo("INSERT")==0)
		{
                    //modificado jose viteri 16/11/16
			doTransactionInsert(db,dbkey, fieldSize);
		}
		else if (op.compareTo("SCAN")==0)
		{
			doTransactionScan(db,dbkey);
		}
		else
		{
                    //modificado jose viteri 16/11/16
			doTransactionReadModifyWrite(db,dbkey, fieldSize);
		}
		
		return true;
	}

  /**
   * modificado 10/11/2016 jose viteri
   * Results are reported in the first three buckets of the histogram under
   * the label "VERIFY". 
   * Bucket 0 means the expected data was returned.
   * Bucket 1 means incorrect data was returned.
   * Bucket 2 means null data was returned when some data was expected. 
   *  //modificado jose viteri 16/11/16
   */
  protected void verifyRow(String key, HashMap<String,ByteIterator> cells, long fieldSize) {
    int matchType = DATA_INT_MATCH;
    if (!cells.isEmpty()) {
      for (Map.Entry<String, ByteIterator> entry : cells.entrySet()) {
        if (!entry.getValue().toString().equals(
            buildDeterministicValue(key, entry.getKey(), fieldSize))) {
          matchType = DATA_INT_DEVIATE;
          break;
        }
      }
    } else {
      //This assumes that null data is never valid
      matchType = DATA_INT_UNEXPECTED_NULL;
    }
    Measurements.getMeasurements().measure("VERIFY", matchType);
  }

    long nextKeynum() {
        long keynum;
        if(keychooser instanceof ExponentialGenerator) {
            do
                {
                    keynum=transactioninsertkeysequence.lastValue() - keychooser.nextValue().intValue();
                }
            while(keynum < 0);
        } else {
            do
                {
                    keynum=keychooser.nextValue().intValue();
                }
            while (keynum > transactioninsertkeysequence.lastValue());
        }
        return keynum;
    }
        //modificado 16/11/2016 jose viteri
	public void doTransactionRead(DB db, String keyname, long fieldSize)
	{
		HashSet<String> fields=null;

		if (!readallfields)
		{
			//read a random field  
			String fieldname=fieldnames.get(Integer.parseInt(fieldchooser.nextString()));

			fields=new HashSet<String>();
			fields.add(fieldname);
		}

    		HashMap<String,ByteIterator> cells =
        	new HashMap<String,ByteIterator>();
		db.read(table,keyname,fields,cells);

		// EBG - 07/12/2015 - If working AS_CACHE and get result is empty, Insert the record. 
    		if (ascache && cells.isEmpty()) {
                    //modificado jose viteri 16/11/16
		   doTransactionInsert(db,keyname, fieldSize);
    		}

    		if (dataintegrity) {
                     //modificado jose viteri 16/11/16
    		  verifyRow(keyname, cells, fieldSize);
    		}
	}
	
        //modificado 16/11/2016 jose viteri
	public void doTransactionReadModifyWrite(DB db, String keynameX, long fieldSize)
	{
		//choose a random key
		long keynum = nextKeynum();

		String keyname = buildKeyName(keynum);

		HashSet<String> fields=null;

		if (!readallfields)
		{
			//read a random field  
			String fieldname=fieldnames.get(Integer.parseInt(fieldchooser.nextString()));

			fields=new HashSet<String>();
			fields.add(fieldname);
		}
		
		HashMap<String,ByteIterator> values;

		if (writeallfields)
		{
		   //new data for all the fields
                    //modificado jose viteri 16/11/16
		   values = buildValues(keyname, fieldSize);
		}
		else
		{
		   //update a random field
                    //modificado jose viteri 16/11/16
		   values = buildSingleValue(keyname, fieldSize);
		}

		//do the transaction

		HashMap<String,ByteIterator> cells =
		    new HashMap<String,ByteIterator>();

		
		long ist=_measurements.getIntendedtartTimeNs();
	    	long st = System.nanoTime();
		db.read(table,keyname,fields,cells);
		
		db.update(table,keyname,values);

		long en=System.nanoTime();

    		if (dataintegrity) {
                     //modificado jose viteri 16/11/16
    		  verifyRow(keyname, cells, fieldSize);
    		}

		_measurements .measure("READ-MODIFY-WRITE", (int)((en-st)/1000));
		_measurements .measureIntended("READ-MODIFY-WRITE", (int)((en-ist)/1000));
	}
	
	public void doTransactionScan(DB db, String startkeyname)
	{
		//choose a random scan length
		int len=scanlength.nextValue().intValue();

		HashSet<String> fields=null;

		if (!readallfields)
		{
			//read a random field  
			String fieldname=fieldnames.get(Integer.parseInt(fieldchooser.nextString()));

			fields=new HashSet<String>();
			fields.add(fieldname);
		}

		db.scan(table,startkeyname,len,fields,new Vector<HashMap<String,ByteIterator>>());
	}
        
        //modificado 16/11/2016 jose viteri
	public void doTransactionUpdate(DB db, String keyname, long fieldSize)
	{
		HashMap<String,ByteIterator> values;

		if (writeallfields)
		{
		   //new data for all the fields
                    //modificado jose viteri 16/11/16
		   values = buildValues(keyname, fieldSize);
		}
		else
		{
		   //update a random field
                    //modificado jose viteri 16/11/16
		   values = buildSingleValue(keyname, fieldSize);
		}

		db.update(table,keyname,values);
	}
        
        //modificado 16/11/2016 jose viteri
	public void doTransactionInsert(DB db, String dbkey, long fieldSize)
	{
                //modificado jose viteri 16/11/16
		HashMap<String, ByteIterator> values = buildValues(dbkey, fieldSize);
		db.insert(table,dbkey,values);
	}
        
        /**
         * jose viteri  
         * 10/11/16
         * funcion lectora de traces
         * 
         */
        
        
        public String[] lectorTraces(){
            
            String[] trace = null;
            synchronized(this){
		try{
                    trace = tracefile.readLine().split(",");
                }catch(Exception e){
                    e.printStackTrace();
                }
            
            }
            return trace;
        }
}


