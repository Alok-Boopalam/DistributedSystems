import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
        
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class Program {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\t");
     //  System.out.println(parts[0]);
        String row = "row"+(long)(Math.random() * 1000000);
       // System.out.println(row);
        Configuration config = HBaseConfiguration.create();
        HTable hTable = new HTable(config, "input");
        Put p = new Put(Bytes.toBytes(row)); 
        p.add(Bytes.toBytes("area"),
        	  Bytes.toBytes("X1"),Bytes.toBytes(parts[0]));
        p.add(Bytes.toBytes("area"),
      	      Bytes.toBytes("X5"),Bytes.toBytes(parts[4]));
        p.add(Bytes.toBytes("area"),
      	      Bytes.toBytes("X6"),Bytes.toBytes(parts[5]));
        p.add(Bytes.toBytes("area"),
      	      Bytes.toBytes("Y1"),Bytes.toBytes(parts[8]));
        p.add(Bytes.toBytes("area"),
      	      Bytes.toBytes("Y2"),Bytes.toBytes(parts[9]));
        p.add(Bytes.toBytes("property"),
      	      Bytes.toBytes("X2"),Bytes.toBytes(parts[1]));
        p.add(Bytes.toBytes("property"),
      	      Bytes.toBytes("X3"),Bytes.toBytes(parts[2]));
        p.add(Bytes.toBytes("property"),
      	      Bytes.toBytes("X4"),Bytes.toBytes(parts[3]));
        p.add(Bytes.toBytes("property"),
      	      Bytes.toBytes("X7"),Bytes.toBytes(parts[6]));
        p.add(Bytes.toBytes("property"),
      	      Bytes.toBytes("X8"),Bytes.toBytes(parts[7]));

        hTable.put(p);
       // System.out.println("data inserted");
      
        
        // closing HTable
        hTable.close();
     
    }
 } 
        
 
        
 public static void main(String[] args) throws Exception {
	 
	 Configuration con = HBaseConfiguration.create();
     HBaseAdmin admin = new HBaseAdmin(con);
     
		
		 if (admin.tableExists("input")) {
			  admin.disableTable("input");
			     admin.deleteTable("input");
		 }
		 if (admin.tableExists("centre")) {
			  admin.disableTable("centre");
			     admin.deleteTable("centre");
		 }
		 if (admin.tableExists("temp")) {
			  admin.disableTable("temp");
			     admin.deleteTable("temp");
		 }
		 
     
      HTableDescriptor inputtable = new HTableDescriptor(TableName.valueOf("input"));


      inputtable.addFamily(new HColumnDescriptor("area"));
      inputtable.addFamily(new HColumnDescriptor("property"));

      admin.createTable(inputtable);
      
      HTableDescriptor centertable = new HTableDescriptor(TableName.valueOf("centre"));
      centertable.addFamily(new HColumnDescriptor("area"));
      centertable.addFamily(new HColumnDescriptor("property"));

      admin.createTable(centertable);
      HTableDescriptor temptable = new HTableDescriptor(TableName.valueOf("temp"));
      temptable.addFamily(new HColumnDescriptor("area"));
      temptable.addFamily(new HColumnDescriptor("property"));

      admin.createTable(temptable);
      System.out.println(" Tables created ");
      try { 
 	 FileReader fileReader = new FileReader(args[0]);
     BufferedReader bfr = new BufferedReader(fileReader);
 
 	 String str1 = null;
 	 int count = 0;
 	 while ((str1 = bfr.readLine())!= null && count<Integer.parseInt(args[1])) {
 		 count++;
          String[] parts = str1.split("\t");
          
          Configuration config = HBaseConfiguration.create();
          HTable hTable = new HTable(config, "centre");
          String row = "row"+count;
          Put p = new Put(Bytes.toBytes(row)); 
          p.add(Bytes.toBytes("area"),
          	  Bytes.toBytes("X1"),Bytes.toBytes(parts[0]));
          p.add(Bytes.toBytes("area"),
        	      Bytes.toBytes("X5"),Bytes.toBytes(parts[4]));
          p.add(Bytes.toBytes("area"),
        	      Bytes.toBytes("X6"),Bytes.toBytes(parts[5]));
          p.add(Bytes.toBytes("area"),
        	      Bytes.toBytes("Y1"),Bytes.toBytes(parts[8]));
          p.add(Bytes.toBytes("area"),
        	      Bytes.toBytes("Y2"),Bytes.toBytes(parts[9]));
          p.add(Bytes.toBytes("property"),
        	      Bytes.toBytes("X2"),Bytes.toBytes(parts[1]));
          p.add(Bytes.toBytes("property"),
        	      Bytes.toBytes("X3"),Bytes.toBytes(parts[2]));
          p.add(Bytes.toBytes("property"),
        	      Bytes.toBytes("X4"),Bytes.toBytes(parts[3]));
          p.add(Bytes.toBytes("property"),
        	      Bytes.toBytes("X7"),Bytes.toBytes(parts[6]));
          p.add(Bytes.toBytes("property"),
        	      Bytes.toBytes("X8"),Bytes.toBytes(parts[7]));

          hTable.put(p);
       //   System.out.println("data inserted");
        
          hTable.close();  
          
 	 } 
 	 
 	 }
      catch(FileNotFoundException ex) {
          System.out.println(
              "Unable to open file");                
      }
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "wordcount");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
   
        
    job.setInputFormatClass(TextInputFormat.class);
    //job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);
   FileInputFormat.addInputPath(job, new Path(args[0]));
    //FileOutputFormat.setOutputPath(job, new Path(args[1]));
  //  FileUtils.deleteDirectory(new File(args[1]));
   job.waitForCompletion(true);
   System.out.println("data inserted");
   int count=0;
   HTable table = new HTable(conf, "input");
   Scan s = new Scan();
   ResultScanner ss = table.getScanner(s);
   for(Result r:ss){
       for(org.apache.hadoop.hbase.KeyValue kv : r.raw()){
         count++;
       }
   }
   while (true)
   {
  	 if (count==7680)
  	 {
  	break;
  	 }
  	 else
  	 {
  		Program fr2 = new Program();
  	   fr2.main(args);
  	 }
  	 
   }
   
   
   Kmeans fr = new Kmeans();
   fr.kmeans(args);
      
   foriterations fr1 = new foriterations();
  boolean x =  fr1.again(args);
  System.out.println(x);
 while (true)
 {
	 if (x==true)
	 {
		 fr.kmeans(args);
		x =  fr1.again(args);
	 }
	 else
	 {
		 break;
	 }
	 
 }
 

 if (admin.tableExists("temp")) {
	  admin.disableTable("temp");
	     admin.deleteTable("temp");
}
 System.out.println("Completed finding the clusters");
 }
        
}


