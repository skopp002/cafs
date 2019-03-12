package partb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class HadoopIndexer {

    public static class LineIndexMapper extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, Text> {

        private final static Text word = new Text();
        private final static Text location = new Text();

        public void map(LongWritable key, Text val,
                        OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {

            FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            location.set(fileName);

            String line = val.toString();
            //  String resultString = line.replaceAll("[^\\p{L}\\p{Nd}]+", " ");
            String[] itr = line.toLowerCase().split(" ");
//            JsonElement jsonElement = new JsonParser().parse(line);
//            String txt = jsonElement.getAsJsonObject().get("text").toString();
//            System.out.println("Tweet text is"+txt);
            HashMap<String, String> wordMap = new HashMap<>();
            for (String token : itr) {
                wordMap.put(token, fileName);
                //System.out.println("Writing "+ word + " with "+ location);
            }

            wordMap.forEach((k, v) -> {
                try {

                    word.set(k);
                    output.collect(word, location);
                } catch (IOException e) {
                    System.out.println("Error while mapping " + e.getCause());
                }
            });


        }
    }



    public static class LineIndexReducer extends MapReduceBase
            implements Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {

            boolean first = true;
            StringBuilder toReturn = new StringBuilder();
            while (values.hasNext()){
                if (!first)
                    toReturn.append(", ");
                first=false;
                toReturn.append(values.next().toString());
            }

            output.collect(key, new Text(toReturn.toString()));
        }
    }


    /**
     * The actual main() method for our program; this is the
     * "driver" for the MapReduce job.
     */
    public static void main(String[] args) {
        if( args.length !=2 ){
            throw new java.lang.IllegalArgumentException("Usage - java -cp target/cafs-1.0-jar-with-dependencies.jar <input data>  <index output>");

        }
        JobClient client = new JobClient();
        JobConf conf = new JobConf(HadoopIndexer.class);
        long startTime = System.currentTimeMillis();
        conf.setJobName("HadoopIndexer");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.setMapperClass(LineIndexMapper.class);
        conf.setReducerClass(LineIndexReducer.class);

        client.setConf(conf);

        try {
            JobClient.runJob(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Took "+(endTime - startTime)/1000 + " seconds");
        //Last execution took 399.54694815 secs or 399546948150 ns
    }
}
