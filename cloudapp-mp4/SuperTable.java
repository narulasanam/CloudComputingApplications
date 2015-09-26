import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

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

public class SuperTable{
	private static final String table_name = "powers";

    public static void main(String[] args) throws IOException {

		Configuration con = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(con);
      
        
		
		if(!admin.tableExists(table_name)){
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(table_name));
	        tableDescriptor.addFamily(new HColumnDescriptor("personal"));
	        tableDescriptor.addFamily(new HColumnDescriptor("professional"));
            // Execute the table through admin
			admin.createTable(tableDescriptor);
        }
        
        // Instantiating HTable class
	    HTable hTable = new HTable(con, table_name);
        // Repeat these steps as many times as necessary

	    // Instantiating Put class
        // Hint: Accepts a row name
		Put p = new Put(Bytes.toBytes("row1"));
	    // Add values using add() method
        // Hints: Accepts column family name, qualifier/row name ,value
		p.add(Bytes.toBytes("personal"), Bytes.toBytes("hero"), Bytes.toBytes("superman"));
		p.add(Bytes.toBytes("personal"), Bytes.toBytes("power"), Bytes.toBytes("strength"));
		p.add(Bytes.toBytes("professional"), Bytes.toBytes("name"), Bytes.toBytes("clark"));
		p.add(Bytes.toBytes("professional"), Bytes.toBytes("xp"), Bytes.toBytes("100"));
		hTable.put(p);
		
		p = new Put(Bytes.toBytes("row2"));
		p.add(Bytes.toBytes("personal"), Bytes.toBytes("hero"), Bytes.toBytes("batman"));
		p.add(Bytes.toBytes("personal"), Bytes.toBytes("power"), Bytes.toBytes("money"));
		p.add(Bytes.toBytes("professional"), Bytes.toBytes("name"), Bytes.toBytes("bruce"));
		p.add(Bytes.toBytes("professional"), Bytes.toBytes("xp"), Bytes.toBytes("50"));
		hTable.put(p);
		
		p = new Put(Bytes.toBytes("row3"));
		p.add(Bytes.toBytes("personal"), Bytes.toBytes("hero"), Bytes.toBytes("wolverine"));
		p.add(Bytes.toBytes("personal"), Bytes.toBytes("power"), Bytes.toBytes("healing"));
		p.add(Bytes.toBytes("professional"), Bytes.toBytes("name"), Bytes.toBytes("logan"));
		p.add(Bytes.toBytes("professional"), Bytes.toBytes("xp"), Bytes.toBytes("75"));
        hTable.put(p);
		
		hTable.close();
      //System.out.println("data inserted");
	
     
      // Instantiate the Scan class
	  Scan scan = new Scan();
	  HTable htable = new HTable(con, "powers");
      scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("hero"));

      // Get the scan result
	  ResultScanner scanner = htable.getScanner(scan);

      // Read values from scan result
	  for (Result result = scanner.next(); result != null; result = scanner.next())
		// Print scan result
		System.out.println("Found row : "+result);
 
      // Close the scanner
	  scanner.close();
   
      // Htable closer
	  hTable.close();
   }
}

