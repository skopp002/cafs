package partb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class SearchHadoopIndex {
    public static void main(String args[]){
        try {
            Path inputDir = new Path("/Users/sunitakoppar/ucr/cs242/single_jsonmrindexout/part-00000");
            Configuration conf = new Configuration();
            FileSystem fs = inputDir.getFileSystem(conf);
            FSDataInputStream inputStream = fs.open(inputDir);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String record;
            while ((record = reader.readLine()) != null) {
                int blankPos = record.indexOf(" ");
                System.out.println(record + "blankPos" + blankPos);
                String keyString = record.substring(0, blankPos);
                String valueString = record.substring(blankPos + 1);
            }
        } catch(Exception e ){
            e.printStackTrace();
        }
    }
}
