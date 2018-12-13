package com.chris.kmeans;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class KMeansCombiner  extends
Reducer<IntWritable,DataItem,IntWritable,DataItem>{
	   private static int valuesNumbers=0;  
	      
	    private static Log log =LogFactory.getLog(KMeansCombiner.class);  
	    // the main purpose of the sutup() function is to get the dimension of the original data  
	    @Override
	    public void setup(Context context) throws IOException, InterruptedException{  
	    	valuesNumbers = context.getConfiguration().getInt("valuesNumbers", 0);
	    	log.info("dimension===>"+valuesNumbers);
	    	super.setup(context);
	    	
//	        Path[] caches=context.getLocalCacheFiles();//DistributedCache.getLocalCacheFiles(context.getConfiguration());  
//	        if(caches==null||caches.length<=0){  
//	            log.error("center file does not exist");  
//	            System.exit(1);  
//	        }  
//	        BufferedReader br=new BufferedReader(new FileReader(caches[0].toString()));  
//	        String line;  
//	        while((line=br.readLine())!=null){  
//	            String[] str=line.split("\t");  
//	    
//	            dimension=str.length;  
//	            break;  
//	        }  
//	        try {  
//	            br.close();  
//	        } catch (Exception e) {  
//	            // TODO Auto-generated catch block  
//	            e.printStackTrace();  
//	        }  
	    	
	    	
	    }  
	      
	    @Override
	    public void reduce(IntWritable indexKey,Iterable<DataItem> dataItemValues,Context context) 
	    		throws IOException, InterruptedException{   
	    	int count=0; 
	    	double[] sum=new double[valuesNumbers];  
	    	StringBuffer strBuffer=new StringBuffer();
	    	DataItem newData=new DataItem();
	    	
	        // 对相同的key累加所有的values
	        for(DataItem v:dataItemValues){  
	        	count+=v.getDataCount().get();
	        	String[] str=v.getDataCenter().toString().split("\t");  
	              
	            for(int i=0;i<valuesNumbers;i++){  
	                sum[i]+=Double.parseDouble(str[i]);  
	            }  
	        }  
	        // calculate the new centers  
	        for(int i=0;i<valuesNumbers;i++){  
	        	strBuffer.append(sum[i]+"\t");  
	        }     
	        newData.set(new Text(strBuffer.toString()), new IntWritable(count));  
	        context.write(indexKey, newData);  
	    }  
}









