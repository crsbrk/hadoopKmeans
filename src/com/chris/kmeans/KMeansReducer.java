package com.chris.kmeans;

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

public class KMeansReducer extends
		Reducer<IntWritable, DataItem, NullWritable, Text> {
	private static int valuesNumbers = 0;

	private static Log log = LogFactory.getLog(KMeansReducer.class);

	// the main purpose of the sutup() function is to get the dimension of the
	// original data
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		valuesNumbers = context.getConfiguration().getInt("valuesNumbers", 0);
		log.info("dimension===>" + valuesNumbers);
		System.out.println("dimension===>" + valuesNumbers);
		// Path[] caches=
		// context.getLocalCacheFiles();//DistributedCache.getLocalCacheFiles(context.getConfiguration());
		// if(caches==null||caches.length<=0){
		// log.error("center file does not exist");
		// System.exit(1);
		// }
		// BufferedReader br=new BufferedReader(new
		// FileReader(caches[0].toString()));
		// String line;
		// while((line=br.readLine())!=null){
		// String[] str=line.split("\t");
		// dimension=str.length;
		// break;
		// }
		// try {
		// br.close();
		// } catch (Exception e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		super.setup(context);
	}

	@Override
	protected void reduce(IntWritable indexKey, Iterable<DataItem> dataItemValues,
			Context context) throws IOException, InterruptedException {
		int count = 0;
		double[] sum = new double[valuesNumbers];
		StringBuffer strBuffer = new StringBuffer();
		
		for (DataItem v : dataItemValues) {
			String[] str = v.getDataCenter().toString().split("\t");
			count += v.getDataCount().get();
			for (int i = 0; i < valuesNumbers; i++) {
				sum[i] += Double.parseDouble(str[i]);
			}
		}
		// calculate the new centers

		for (int i = 0; i < valuesNumbers; i++) {
			if (0 == i) {
				double d = Math.floor(sum[0] / count);
				String posi = Long.toHexString((long) d);
				strBuffer.append(posi + "\t");
			} else {
				strBuffer.append(sum[i] / count + "\t");
			}

		}
		context.write(null, new Text(strBuffer.toString()));

	}

}
