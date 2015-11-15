import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;


public class foriterations {

	public static boolean again(String[] args) throws IOException
	{
		ArrayList<String> names = new ArrayList();
		
		names.add("X1");names.add("X5");names.add("X6");names.add("Y1");names.add("Y2");
		names.add("X2");names.add("X3");names.add("X4");names.add("X7");names.add("X8");
		boolean flag = false;
		
	for(int i=1;i<(Integer.parseInt(args[1])+1);i++)
	{
		for(int j= 0;j<names.size()/2;j++)
		{
			 String a = names.get(j);
			 String row = "row"+i;
			 Configuration config = HBaseConfiguration.create();
		      HTable table = new HTable(config, "centre");
		      Get g = new Get(Bytes.toBytes(row));
		      Result result = table.get(g); 
		      byte [] value = result.getValue(Bytes.toBytes("area"),Bytes.toBytes(a));
		      String val = Bytes.toString(value);
		      System.out.println(val);
		      Configuration config1 = HBaseConfiguration.create();
		      HTable table1 = new HTable(config1, "temp");
		      Get g1 = new Get(Bytes.toBytes(row));
		      Result result1 = table1.get(g1); 
		      byte [] value1 = result1.getValue(Bytes.toBytes("area"),Bytes.toBytes(a));
		      String val1 = Bytes.toString(value1);
		      System.out.println(val1);
		      double check = Double.parseDouble(val)-Double.parseDouble(val1);
		      if(Math.abs(check)>0.002)
		      {
		    	  flag = true;
		      }
		      
		      Configuration config2 = HBaseConfiguration.create();
		        HTable hTable2 = new HTable(config, "centre");
		        Put p = new Put(Bytes.toBytes(row)); 
		        p.add(Bytes.toBytes("area"),
		        	  Bytes.toBytes(a),Bytes.toBytes(val1));
		        hTable2.put(p);
		     
		}
		
		for(int j= (names.size()/2);j<names.size();j++)
		{
			 String a = names.get(j);
			 String row = "row"+i;
			 Configuration config = HBaseConfiguration.create();
		      HTable table = new HTable(config, "centre");
		      Get g = new Get(Bytes.toBytes(row));
		      Result result = table.get(g); 
		      byte [] value = result.getValue(Bytes.toBytes("property"),Bytes.toBytes(a));
		      String val = Bytes.toString(value);
		      System.out.println(val);
		      Configuration config1 = HBaseConfiguration.create();
		      HTable table1 = new HTable(config1, "temp");
		      Get g1 = new Get(Bytes.toBytes(row));
		      Result result1 = table1.get(g1); 
		      byte [] value1 = result1.getValue(Bytes.toBytes("property"),Bytes.toBytes(a));
		      String val1 = Bytes.toString(value1);
		      System.out.println(val1);
		      double check = Double.parseDouble(val)-Double.parseDouble(val1);
		      if(Math.abs(check)>0.002)
		      {
		    	  flag = true;
		      }
		      
		      Configuration config2 = HBaseConfiguration.create();
		        HTable hTable2 = new HTable(config, "centre");
		        Put p = new Put(Bytes.toBytes(row)); 
		        p.add(Bytes.toBytes("property"),
		        	  Bytes.toBytes(a),Bytes.toBytes(val1));
		        hTable2.put(p);
		      
		      }
		      
		}
		System.out.println(flag);
		return flag;
		
	}
	
	
	}
	
	
	
