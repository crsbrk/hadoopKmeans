package com.chris.kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Utils {
	
static String guangzhou_4G_TA[]={
	"2500","2501","2502","2503","2504","2505","2506","2507","2508","2509","250a","250b","250c","250d","250e","250f",
	"2510","2511","2512","2513","2514","2515","2516","2517","2518","2519","251a","251b","251c","251d","251e","251f",
	"2520","2521","2522","2523","2524","2525","2526","2527",
	"25c8","25c9","25ca","25cb","25cc","25cd","25ce","25cf",
	"25d0","25d1","25d2","25d3","25d4","25d5","25d6","25d7","25d8","25d9","25da","25db","25dc","25dd","25de","25df",
	"25e0","25e1","25e2","25e3","25e4","25e5","25e6","25e7","25e8",
	"26d0","26d1","26d2","26d3","26d4","26d5","26d6","26d7","26d8","26d9","26da","26db","26dc","26dd","26de","26df"
};




public static String APRI4ITEMS[]={
	"1157632026",//weixin
	"571277441",//youku
	"1157632000",//qq
	"571277316"//aiqiyi
	
};


public static int CDRITEMS=20;//numbers of a record cdr pieces

public static enum FileRecorder{  
    ErrorRecorder,  
    TotalRecorder,
    PositionNullRecorder
}  

/**
 * delete files in Path
 * */
public static int deletePath(Path p, Configuration c) throws IOException {
	FileSystem hdfs = p.getFileSystem(c);

	if (hdfs.isDirectory(p)) {
		hdfs.delete(p, true);
	}

	return 0;
}

public static ArrayList<ArrayList<Double>> readFileFromHDFS(String path, 
		Configuration conf) throws IOException{
	String s = "";
	String result[] ;
	Path file = new Path(path);
	
	if (path.equals("") || path == null)
		return null;
	
	
	FileSystem fs =new Path(path).getFileSystem(conf);
	ArrayList<ArrayList<Double>> list = new ArrayList<ArrayList<Double>>();
	ArrayList<Double> item ;
	
	 FSDataInputStream getIt = fs.open(file);
     BufferedReader d = new BufferedReader(new InputStreamReader(getIt));
     String id;
     
     while ((s = d.readLine()) != null) {
   	  item = new ArrayList<Double>();
   	  
   	  System.out.println("in readHdfsFIle");
         System.out.println(s);
         
         if(s.length()>0){
       	  result = s.trim().split("\\t");
       	 
       	  
       	  for (int i=0; i<result.length;i++){
       		  Double dou;
       		  if (0 == i){
       			  dou = Long.parseLong(result[0], 16) * 1.0;
       		  }else{
       			  dou = Double.parseDouble(result[i]);
       		  }
       		  	item.add(dou);
       		}//end of for
       	 list.add(item);
         
         }//end of if
        
         
     }//end of while
    
     
     d.close();
     
     
	return list;
}


}
