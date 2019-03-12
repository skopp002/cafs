package partb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class CreateHbaseTable {

    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        try {
            Connection conn = ConnectionFactory.createConnection(conf);
            Admin hAdmin = conn.getAdmin();

            HTableDescriptor hTableDesc = new HTableDescriptor(
                    TableName.valueOf("Tweets"));
            hTableDesc.addFamily(new HColumnDescriptor("word"));
            hTableDesc.addFamily(new HColumnDescriptor("tweet"));

            hAdmin.createTable(hTableDesc);

            System.out.println("Table created Successfully...");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}