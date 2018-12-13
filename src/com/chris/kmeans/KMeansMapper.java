package com.chris.kmeans;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

public class KMeansMapper extends
		Mapper<LongWritable, Text, IntWritable, DataItem> {

	private double[][] centers;
	private int dimention_m; // this is the number of k
	private int dimention_n; // this is the number of all values
	private static Log log = LogFactory.getLog(KMeansMapper.class);
	private String strItems;
	private String timeRange;
	private int startTime = 0;
	private int endTime = 0;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		// Path[] caches = context.getLocalCacheFiles();

		strItems = (String) conf.get("stringItems");
		String centerFile = conf.get("pathOfCentersFile");
		String timeRange = conf.get("timeRange");
		log.info("pathOfCenterFile===>" + centerFile + "\nstringItem=> "
				+ strItems + " \ntimeRange=> " + timeRange);
		// if (caches == null || caches.length <= 0) {
		// log.error("center file does not exist");
		// System.exit(1);
		// }

		// FileReader reader = new FileReader(caches[0].toString());
		// BufferedReader br = new BufferedReader(reader);
		String t[] = timeRange.split("-", 0);
		if (t.length == 2) {
			startTime = Integer.parseInt(t[0]);
			endTime = Integer.parseInt(t[1]);
		}
		String line;
		ArrayList<ArrayList<Double>> temp_centers;// = new
													// ArrayList<ArrayList<Double>>();

		// get the file data
		temp_centers = Utils.readFileFromHDFS(centerFile, conf);
		if (temp_centers == null)
		{
			log.error("can not get temp_center!!!!centerFIle=>"+centerFile);
		}
		// ArrayList<Double> center = null;
		// get the file data
		// while ((line = br.readLine()) != null) {
		// center = new ArrayList<Double>();
		// String[] str = line.split("\t");
		//
		// for (String s :str){
		// System.out.println(s);
		// }
		// for (int i = 0; i < str.length; i++) {
		// if (0 == i) {
		// center.add(Long.parseLong(str[0], 16) * 1.0);
		// } else {
		// center.add(Double.parseDouble(str[i]));
		// }
		//
		// }
		// temp_centers.add(center);
		// }
		//
		// br.close();

		// fill the centers
		@SuppressWarnings("unchecked")
		ArrayList<Double>[] newcenters = temp_centers
				.toArray(new ArrayList[] {});
		dimention_m = temp_centers.size();
		dimention_n = newcenters[0].size();

		centers = new double[dimention_m][dimention_n];

		for (int i = 0; i < dimention_m; i++) {
			Double[] temp_double = newcenters[i].toArray(new Double[] {});
			for (int j = 0; j < dimention_n; j++) {
				centers[i][j] = temp_double[j];

			}

		}

		for (int i = 0; i < dimention_m; i++) {
			for (int j = 0; j < dimention_n; j++) {
				log.info(centers[i][j]);

			}
		}
		super.setup(context);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String values[] = line.split("\\|", 0);
		String apriItems[] = strItems.split(":");

		if (values.length != Utils.CDRITEMS || line.contains("||||||")) {
			context.getCounter(Utils.FileRecorder.ErrorRecorder).increment(1);
			return;
		} else if (values[17].isEmpty() || values[18].isEmpty()
				|| values[15].isEmpty()) {
			context.getCounter(Utils.FileRecorder.PositionNullRecorder)
					.increment(1);
			return;
		} else {
			if (startTime > endTime) {
				return;
			}
			// 每一行输入的话单 是否有必要包含所有的频繁 SI
			// every line, should the charge sid contains all frequent si? I
			// guess not
			// boolean containAll = false;
			// int count = 0;
			//
			// for(int i=0; i< apriItems.length;i++){
			//
			// if (values[15].contains(apriItems[i])){
			// count++;
			// };
			// }
			// if(count != apriItems.length){
			// return;
			// }
		}// end else

		String workDay;
		String workNight;
		int dataTime;
		if (values[9].length() >= 8) {
			dataTime = Integer.parseInt(values[9].substring(6, 8));
		} else {
			System.out
					.println("~~~~~~~~~time string error!!!@@@@##$$$$$$$$$" + 123);
			dataTime = 0;
		}

		if (dataTime < startTime || dataTime > endTime) {
			System.out.println("!!!this line not in the time range!!!");
			return;
		}

		String contentChargeID = values[15];

		Map<String, Double> temp = new HashMap<String, Double>();
		// ArrayList<Double> tmp = new ArrayList<Double>();

		for (int i = 0; i < apriItems.length; i++) {
			temp.put(apriItems[i], 0.0);
		}

		String contentIDs[] = contentChargeID.split(":", 0);
		for (String myid : contentIDs) {
			System.out.println(myid);
			String id[] = myid.split("_");

			if (temp.containsKey(id[0])) {
				temp.put(id[0],
						Double.parseDouble(id[1]) + Double.parseDouble(id[2]));
			}

		}

		double[] temp_double = new double[dimention_n];
		String lac = values[17];
		String ci = values[18];
		System.out.println("lac:" + lac + " ci: " + ci);

		if (!values[14].equals("6") && ci.length() < 8) {// 6 mean lte
			System.out.println("rAT=>" + values[14]);
			if (lac.startsWith("9") || lac.startsWith("a"))
				temp_double[0] = Long.parseLong(lac + ci, 16);
			else
				return;
		} else {
			System.out.println("rAT=>" + values[14]);
			if (ci.startsWith("06") || ci.startsWith("6"))
				temp_double[0] = Long.parseLong(ci, 16);
			else
				return;
		}
		for (int i = 1; i < temp_double.length; i++) {
			temp_double[i] = temp.get(apriItems[i - 1]);
		}

		// set the index
		// 4G start with 06,3G start with A,2G start with 2
		double distance = Double.MAX_VALUE;
		double temp_distance = 0.0;
		int index = 0;
		for (int i = 0; i < dimention_m; i++) {
			double[] temp_center = centers[i];
			temp_distance = getEnumDistance(temp_double, temp_center);
			if (temp_distance < distance) {
				index = i;
				distance = temp_distance;
			}
		}
		DataItem newvalue = new DataItem();
		String newLine = "";
		for (int i = 0; i < temp_double.length; i++) {
			newLine += temp_double[i] + "\t";
		}
		newLine = newLine.trim();

		newvalue.set(new Text(newLine), new IntWritable(1));
		System.out.println("====>the map out:index is " + index + ",value is "
				+ value + "<====\n newLine is " + newLine + "\n");

		context.write(new IntWritable(index), newvalue);

	}

	public static double getEnumDistance(double[] source, double[] other) { // get
																			// the
																			// distance
		// must be the same network ,4G lacci compare to 4G,3G position cmp 3G
		// positon
		// 2G to 2G
		String hex1 = Long.toHexString((long) source[0]);
		if (hex1.startsWith("06") || hex1.startsWith("a")
				|| hex1.startsWith("2") || hex1.startsWith("9")) {
			String hex2 = Long.toHexString((long) other[0]);
			if (hex1.indexOf(0) != hex2.indexOf(0)) {
				return Double.MAX_VALUE;
			}
			double distance = 0.0;
			if (source.length != other.length) {
				return Double.MAX_VALUE;
			}
			for (int i = 0; i < source.length; i++) {
				distance += (source[i] - other[i]) * (source[i] - other[i]);
			}
			distance = Math.sqrt(distance);
			return distance;
		}
		return Double.MAX_VALUE;
	}
}
