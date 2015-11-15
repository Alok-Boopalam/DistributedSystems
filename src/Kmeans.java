import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;



public class Kmeans {

	public static class CounterMapper extends TableMapper<Text, Text> {

	    public static enum Counters {
	      ROWS
	    }

	    public void map(ImmutableBytesWritable row, Result value,
	        Context context) throws InterruptedException, IOException {
	      context.getCounter(Counters.ROWS).increment(1);
	      String a = value.toString();
	      //System.out.println(a);
	      
	      String a12;
	      a12 = a.substring(a.indexOf("s={") + 3);
	      a = a12.substring(0, a12.indexOf("/ar"));
	      Configuration cfg= HBaseConfiguration.create();
	      HTable table1 = new HTable(cfg, "input");
	      Get g = new Get(Bytes.toBytes(a));
	      Result result = table1.get(g); 
	     ArrayList<Double> values = new ArrayList();
	      byte[] a1 = result.getValue(Bytes.toBytes("area"),Bytes.toBytes("X1"));
	      byte[] a2  = result.getValue(Bytes.toBytes("area"),Bytes.toBytes("X5"));
	      byte[] a3  = result.getValue(Bytes.toBytes("area"),Bytes.toBytes("X6"));
	      byte[] a4 = result.getValue(Bytes.toBytes("area"),Bytes.toBytes("Y1"));
	      byte[] a5 = result.getValue(Bytes.toBytes("area"),Bytes.toBytes("Y2"));
	      byte[] a6 = result.getValue(Bytes.toBytes("property"),Bytes.toBytes("X2"));
	      byte[] a7 = result.getValue(Bytes.toBytes("property"),Bytes.toBytes("X3"));
	      byte[] a8 = result.getValue(Bytes.toBytes("property"),Bytes.toBytes("X4"));
	      byte[] a9 = result.getValue(Bytes.toBytes("property"),Bytes.toBytes("X7"));
	      byte[] a10 = result.getValue(Bytes.toBytes("property"),Bytes.toBytes("X8"));
	      String val1 = Bytes.toString(a1);Double f1 = Double.parseDouble(val1);values.add(f1);
	      String val2 = Bytes.toString(a2);Double f2 = Double.parseDouble(val2);values.add(f2);
	      String val3 = Bytes.toString(a3);Double f3 = Double.parseDouble(val3);values.add(f3);
	      String val4 = Bytes.toString(a4);Double f4 = Double.parseDouble(val4);values.add(f4);
	      String val5 = Bytes.toString(a5);Double f5 = Double.parseDouble(val5);values.add(f5);
	      String val6 = Bytes.toString(a6);Double f6 = Double.parseDouble(val6);values.add(f6);
	      String val7 = Bytes.toString(a7);Double f7 = Double.parseDouble(val7);values.add(f7);
	      String val8 = Bytes.toString(a8);Double f8 = Double.parseDouble(val8);values.add(f8);
	      String val9 = Bytes.toString(a9);Double f9 = Double.parseDouble(val9);values.add(f9);
	      String val10 = Bytes.toString(a10);Double f10 = Double.parseDouble(val10);values.add(f10);
	     // System.out.println(values.get(0));
	      //System.out.println(values);
	      double fx = 0;
	      String topass = "";
	      TreeMap<String,Double> map = new TreeMap<String,Double>();
	      Configuration conf = context.getConfiguration();
	      String param = conf.get("k");
	      for(int i = 1;i<(Integer.parseInt(param)+1);i++)
	      {
	    	  
	    	  Configuration con= HBaseConfiguration.create();
	    	 String temp = "row"+i;
		      HTable centable = new HTable(con, "centre");
		      Get g1 = new Get(Bytes.toBytes(temp));
		      Result result1 = centable.get(g1); 
		     ArrayList<Double> values1 = new ArrayList();
		      byte[]  a11 = result1.getValue(Bytes.toBytes("area"),Bytes.toBytes("X1"));
		      byte[]  a21  = result1.getValue(Bytes.toBytes("area"),Bytes.toBytes("X5"));
		      byte[]  a31  = result1.getValue(Bytes.toBytes("area"),Bytes.toBytes("X6"));
		      byte[] a41 = result1.getValue(Bytes.toBytes("area"),Bytes.toBytes("Y1"));
		      byte[]  a51 = result1.getValue(Bytes.toBytes("area"),Bytes.toBytes("Y2"));
		      byte[]  a61 = result1.getValue(Bytes.toBytes("property"),Bytes.toBytes("X2"));
		      byte[]  a71 = result1.getValue(Bytes.toBytes("property"),Bytes.toBytes("X3"));
		      byte[]  a81 = result1.getValue(Bytes.toBytes("property"),Bytes.toBytes("X4"));
		      byte[]  a91 = result1.getValue(Bytes.toBytes("property"),Bytes.toBytes("X7"));
		      byte[]  a101 = result1.getValue(Bytes.toBytes("property"),Bytes.toBytes("X8"));
		      String  val11 = Bytes.toString(a11);Double f11 = Double.parseDouble(val11);values1.add(f11);
		      val11 = Bytes.toString(a21);f11 = Double.parseDouble(val11);values1.add(f11);
		      val11 = Bytes.toString(a31);f11 = Double.parseDouble(val11);values1.add(f11);
		      val11 = Bytes.toString(a41);f11 = Double.parseDouble(val11);values1.add(f11);
		      val11 = Bytes.toString(a51);f11 = Double.parseDouble(val11);values1.add(f11);
		      val11 = Bytes.toString(a61);f11 = Double.parseDouble(val11);values1.add(f11);
		      val11 = Bytes.toString(a71);f11 = Double.parseDouble(val11);values1.add(f11);
		      val11 = Bytes.toString(a81);f11 = Double.parseDouble(val11);values1.add(f11);
		      val11 = Bytes.toString(a91);f11 = Double.parseDouble(val11);values1.add(f11);
		      val11 = Bytes.toString(a101);f11 = Double.parseDouble(val11);values1.add(f11);
		     
		      double sum = 0;
		      for(int j = 0;j<10;j++)
		      {
		    	  sum = sum+Math.pow((values.get(j)-values1.get(j)), 2);
		      }
		      sum  = Math.sqrt(sum);
		    
		      map.put(temp, sum);
		  // System.out.println(values);
		// System.out.println(values1); 
		//   System.out.println(sum);
		//System.out.println(temp);
			    
	      }
	      //for finding the minimum distance to centroid centre
	      String str = "";
	      Double min =Double.valueOf(Double.POSITIVE_INFINITY );
	      for(Map.Entry<String,Double> e:map.entrySet()){
	          if(min.compareTo(e.getValue())>0){
	              str=e.getKey();
	              min=e.getValue();
	          }
	      }
	       
	   //System.out.println(min);
	   //System.out.println(str);
	   //Configuration con3= HBaseConfiguration.create();
	   //HTable centable3 = new HTable(con3, "input");
		String vecttopass= ""+val1+","+val2+","+val3+","+val4+","+val5+","+val6+","+val7+","+val8+","+val9+","+val10;
	   // System.out.println(vecttopass);
	      final Text word = new Text(str);
	      final Text one = new Text(vecttopass);
	        context.write(word,one);
		    }
	  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                      ) throws IOException, InterruptedException {
    	 	 String sum = "";
    	 	 Double sum1=0.0;Double sum2=0.0;Double sum3=0.0;Double sum4=0.0;Double sum5=0.0;Double sum6=0.0;Double sum7=0.0;Double sum8=0.0;Double sum9=0.0;Double sum10=0.0;
    	int count = 0;
         for (Text val : values) {
        	 count++;
           String temp = val.toString();
           String[] columns = temp.split(",");
           sum1+=Double.parseDouble(columns[0]);
           sum2+=Double.parseDouble(columns[1]);
           sum3+=Double.parseDouble(columns[2]);
           sum4+=Double.parseDouble(columns[3]);
           sum5+=Double.parseDouble(columns[4]);
           sum6+=Double.parseDouble(columns[5]);
           sum7+=Double.parseDouble(columns[6]);
           sum8+=Double.parseDouble(columns[7]);
           sum9+=Double.parseDouble(columns[8]);
           sum10+=Double.parseDouble(columns[9]);
       //    System.out.println(sum1);
         }
       System.out.println(count);
        sum1=sum1/count;sum2=sum2/count;sum3=sum3/count;sum4=sum4/count;sum5=sum5/count;sum6=sum6/count;sum7=sum7/count;
        sum8=sum8/count;sum9=sum9/count;sum10=sum10/count;
        // System.out.println(sum1);
        Configuration config = HBaseConfiguration.create();
        HTable hTable = new HTable(config, "temp");
        String table = key.toString();
        Put p = new Put(Bytes.toBytes(table)); 
        p.add(Bytes.toBytes("area"),
        	  Bytes.toBytes("X1"),Bytes.toBytes(sum1.toString()));
        p.add(Bytes.toBytes("area"),
      	      Bytes.toBytes("X5"),Bytes.toBytes(sum2.toString()));
        p.add(Bytes.toBytes("area"),
      	      Bytes.toBytes("X6"),Bytes.toBytes(sum3.toString()));
        p.add(Bytes.toBytes("area"),
      	      Bytes.toBytes("Y1"),Bytes.toBytes(sum4.toString()));
        p.add(Bytes.toBytes("area"),
      	      Bytes.toBytes("Y2"),Bytes.toBytes(sum5.toString()));
        p.add(Bytes.toBytes("property"),
      	      Bytes.toBytes("X2"),Bytes.toBytes(sum6.toString()));
        p.add(Bytes.toBytes("property"),
      	      Bytes.toBytes("X3"),Bytes.toBytes(sum7.toString()));
        p.add(Bytes.toBytes("property"),
      	      Bytes.toBytes("X4"),Bytes.toBytes(sum8.toString()));
        p.add(Bytes.toBytes("property"),
      	      Bytes.toBytes("X7"),Bytes.toBytes(sum9.toString()));
        p.add(Bytes.toBytes("property"),
      	      Bytes.toBytes("X8"),Bytes.toBytes(sum10.toString()));

        hTable.put(p);
         
    }
  }

  public static void kmeans(String[] args) throws Exception {    Configuration conf = new Configuration();
  Configuration conf1 = new Configuration();
  conf1.set("k", args[1]);
  Job job = new Job(conf1);
  
  

 // Job job = new Job(conf, "word count");
    //conf.set("test", "123");
    job.setJarByClass(Kmeans.class);
    job.setMapperClass(CounterMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    
    Scan scan = new Scan();
    

    TableMapReduceUtil.initTableMapperJob("input", // input table
        scan, 
        CounterMapper.class, 
        Text.class, 
        Text.class,
        job);
    job.setOutputFormatClass(NullOutputFormat.class);
    
    
   // job.setNumReduceTasks(5); 
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
   
   // FileOutputFormat.setOutputPath(job, newFolderPath);
    
    boolean b = job.waitForCompletion(true);
    if (!b) {
      throw new IOException("error with job!");
    }
  }
}