package com.chris.kmeans;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.logging.Log;
import java.io.BufferedReader;

import java.io.InputStreamReader;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMmain extends Configured implements Tool {

	private String temp_path = "hdfs://cluster1/kmeans/temp_center/";
	private String dataPath = "hdfs://cluster1/cdr/test/*";// smallkmeansdata";
	// private static final String
	// temp_path="hdfs://localhost:9000/kmeans/temp_center/";
	// private static final String
	// dataPath="hdfs://localhost:9000/cdr/*";//smallkmeansdata";

	private static final int iterTime = 300; //
	private static int iterNum = 0;
	private static final double threadHold = 0.01;

	private static Log log = LogFactory.getLog(KMmain.class);

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		if (args.length < 4) {
			System.out
					.println("usage: kmeans.jar inputdata output centers 571277441:1157632000:1157632026");
			return;
		}
		int ec = -1;
		try {
			ec = ToolRunner.run(new Configuration(), new KMmain(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(ec);

	}

	@Override
	public int run(String[] args) throws Exception {

		boolean flag = true;

		// Configuration conf = getConf();

		//
		dataPath = args[0]; // input data path
		temp_path = args[1];// outut centers path
		String centerFile = args[2]; // "hdfs://cluster1/kmeans/newCenters.txt";
		String stringItems = args[3];// some items
										// "571277441:571277316:1157632000:1157632026"
		String timeRange = args[4];
		// conf.setInt("valueNumber",)
		// set the centers data file
		Path centersFile = new Path(centerFile);// ("hdfs://cluster1/kmeans/newCenters.txt");
												// //stored K point
		// Path centersFile=new
		// Path("hdfs://localhost:9000/kmeans/centers.txt"); //stored K point

		do {
			Configuration conf = new Configuration();
			int valuesNumbers = Utils.readFileFromHDFS(centerFile, conf).get(0)
					.size();
			conf.setInt("valuesNumbers", valuesNumbers);// key numbers
			conf.set("stringItems", stringItems);
			conf.set("timeRange", timeRange);

			if (iterNum == 0) {
				conf.set("pathOfCentersFile", centerFile);
				Utils.deletePath(new Path(temp_path), conf);
			} else {
				conf.set("pathOfCentersFile", temp_path + (iterNum - 1)
						+ "/part-r-00000");
			}
			Job job = new Job(conf, "kmeans job " + iterNum);

			// job.addCacheFile(centersFile.toUri());
			// job.addCacheFile(centersFile.toUri());

			job.setJarByClass(KMmain.class);
			job.setMapperClass(KMeansMapper.class);

			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(DataItem.class);

			job.setNumReduceTasks(1);

			job.setCombinerClass(KMeansCombiner.class);
			job.setReducerClass(KMeansReducer.class);

			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(dataPath));

			FileOutputFormat.setOutputPath(job, new Path(temp_path + iterNum
					+ "/"));

			if (!job.waitForCompletion(true)) {
				System.exit(1); // run error then exit
				flag = false;
			}

			// judge the old center and the new center
			if (iterNum > 1) {
				Path oldCentersFile = new Path(temp_path + (iterNum - 1)
						+ "/part-r-00000");
				Path newCentersFile = new Path(temp_path + iterNum
						+ "/part-r-00000");

				flag = distanceOldCenterNewCenter(oldCentersFile,
						newCentersFile, conf);
			}
			iterNum++;
			System.out.println("Iteration number ===>" + iterNum);

		} while (flag && iterNum <= iterTime);// end of while

		return 0;
	}

	private boolean distanceOldCenterNewCenter(Path oldFile, Path newFile,
			Configuration conf) throws IOException {
		boolean flag = true;
		FileSystem fs1 = FileSystem.get(oldFile.toUri(), conf);
		FileSystem fs2 = FileSystem.get(newFile.toUri(), conf);

		if (!(fs1.exists(oldFile) && fs2.exists(newFile))) {
			log.info("the old centers and new centers should exist at the same time");
			System.exit(1);
		}
		if (fs1.getFileStatus(newFile).getBlockSize() == 0
				|| fs2.getFileStatus(newFile).getBlockSize() == 0) {
			log.info("!!!!the file is empty");
			System.exit(1);
		}
		String line1, line2;
		FSDataInputStream in1 = fs1.open(oldFile);
		FSDataInputStream in2 = fs2.open(newFile);

		InputStreamReader istr1 = new InputStreamReader(in1);
		InputStreamReader istr2 = new InputStreamReader(in2);

		BufferedReader br1 = new BufferedReader(istr1);
		BufferedReader br2 = new BufferedReader(istr2);

		double error = 0.0;
		while ((line1 = br1.readLine()) != null
				&& ((line2 = br2.readLine()) != null)) {
			String[] str1 = line1.split("\t");
			String[] str2 = line2.split("\t");

			for (int i = 0; i < str1.length; i++) {
				if (0 == i) {
					error += (Long.parseLong(str1[0], 16) - Long.parseLong(
							str2[0], 16))
							* (Long.parseLong(str1[0], 16) - Long.parseLong(
									str2[0], 16));
				} else {
					error += (Double.parseDouble(str1[i]) - Double
							.parseDouble(str2[i]))
							* (Double.parseDouble(str1[i]) - Double
									.parseDouble(str2[i]));
				}
			}
		}
		if (error < threadHold) {
			System.out.println("error < threadHold:" + error + "<" + threadHold
					+ "finish loop");
			flag = false;
		}
		return flag;
	}

}
