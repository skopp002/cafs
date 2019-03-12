package partb;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.json.*;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import javax.security.auth.callback.TextInputCallback;

public class HadoopIndexer {

    public static class LineIndexMapper extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, Text>{

        private final static Text word = new Text();
        private final static Text location = new Text();

        public void map(LongWritable key, Text val,
                        OutputCollector<Text, Text> output, Reporter reporter) throws IOException{

            String id;
            String tweet;
            String line = val.toString();
            String[] tuple = line.split("\\n");
            try{
                for(int i=0;i<tuple.length; i++){
                    System.out.println("Line is "+ tuple[i]);
                    if (tuple[i]!= ""){
                        JSONObject obj = new JSONObject((tuple[i].trim()));
                        id = obj.getString("id_str");
                        tweet = obj.getString("text");
                        String[] tweet_words = tweet.split(" ");
                        for(String tw:tweet_words) {
                            if(tw.startsWith("#")) {
                                location.set(tweet);
                                word.set(tw);
                                output.collect(word,location);
                            }
                        }
                    }
                    else {
                        System.out.println("seems like "+ tuple[i]+ " is empty");
                    }
                }
            }catch(org.json.JSONException e){
                System.out.println("Ignoring the issue "+ e.getMessage());
            }
        }
    }

    public static class LineIndexReducer extends MapReduceBase
            implements Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException{

            try{
                boolean first = true;
                StringBuilder toReturn = new StringBuilder();
                while (values.hasNext()){
                    if (!first)
                        toReturn.append(", ");
                    first=false;
                    toReturn.append(values.next().toString());
                }
                System.out.println("Key is "+ key + " value is "+ toReturn.toString());
                output.collect(key, new Text(toReturn.toString()));

            }catch(JSONException e){
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        JobClient client = new JobClient();
        JobConf conf = new JobConf(HadoopIndexer.class);
        long startTime = System.nanoTime();
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
        long endTime = System.nanoTime();
        System.out.println("Took "+(endTime - startTime)/1000000 + " seconds");
        //Last execution took 399.54694815 secs or 399546948150 ns
    }
}