package partb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
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
    private static long DOCUMENT_TOTAL_COUNT=0;
    private static final double DEFAULT_K1 = 1.2;
    private static final double DEFAULT_B = .75;
    private static final double AVG_DOC_LENGTH=140;

    private static double termFrequency(String[] doc, String term) {
        double result = 0;
        for (String word : doc) {
            if (term.equalsIgnoreCase(word))
                result++;
        }
        return result;
    }

    private static double idf(ArrayList<String> docs, String term) {
        double n = 0;
        for (String doc : docs) {
            for (String word : doc.split(" ")) {
                if (term.equalsIgnoreCase(word)) {
                    n++;
                    break;
                }
            }
        }
        return Math.log(1 + (docs.size() - n + 0.5) / (n + 0.5));
    }

    private static double score(double freq, double idf, int docSize) {
        if (freq <= 0) return 0.0;
        double tf = freq * (DEFAULT_K1 + 1) / (freq + DEFAULT_K1 * (1 - DEFAULT_B + DEFAULT_B * docSize / AVG_DOC_LENGTH));
        return tf  * idf;
    }

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
                    if (tuple[i]!= ""){
                        JSONObject obj = new JSONObject((tuple[i].trim()));
                        tweet = obj.getString("text");
                        String[] tweet_words = tweet.split(" ");
                        for(String tw:tweet_words) {
                            if(tw.startsWith("#") && obj.getString("lang").equals("en")) {
                                Double tf = termFrequency(tweet_words, tw);
                                location.set(tf.toString() + "__" + tweet);
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
                /*  System.out.println("Ignoring the issue "+ e.getMessage()+ " line "+ line);
                There seem to be lot of empty lines which cause this issue.
                Sample output of above print line
                Ignoring the issue A JSONObject text must begin with '{' at 0 [character 1 line 1] line
                 */
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
                ArrayList<String> doclist = new ArrayList();
                //All tweets with a given word are looped through to create a list
                while (values.hasNext()){
                    String tweetWithWord = values.next().toString();
                    doclist.add(tweetWithWord);
                }
                System.out.println("Key is "+ key + " value is "+ doclist.toString());
                StringBuilder scoredDocs = new StringBuilder();
                for (String doc : doclist){
                    String[] tfAndDoc = doc.split("__");
                    double tf = Double.parseDouble(tfAndDoc[0]);
                    double idf = idf(doclist,key.toString());
                    double tfidf = score(tf,idf,doc.length());
                    scoredDocs.append(tfidf+ "___"+tfAndDoc[1]);
                    scoredDocs.append("||NextTweet||");
                }
                System.out.println(key + "--------->" + scoredDocs.toString());
                output.collect(key, new Text("--------->" + scoredDocs.toString()+ "||NextTag||"));

            }catch(JSONException e){
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        try {

        JobClient client = new JobClient();
        JobConf conf = new JobConf(HadoopIndexer.class);
        FileSystem fs = FileSystem.get(conf);
        Path pt = new Path(args[0]);
        ContentSummary cs = fs.getContentSummary(pt);
        DOCUMENT_TOTAL_COUNT = cs.getFileCount();
        long startTime = System.nanoTime();
//        conf.set("DocumentCount", String.valueOf(DOCUMENT_TOTAL_COUNT));
        conf.setJobName("HadoopIndexer");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.setMapperClass(LineIndexMapper.class);
        conf.setReducerClass(LineIndexReducer.class);

        client.setConf(conf);


        JobClient.runJob(conf);
        long endTime = System.nanoTime();
        System.out.println("Took "+(endTime - startTime)/1000000 + " seconds");
        } catch (Exception e) {
            e.printStackTrace();
        }

        //Last execution took 399.54694815 secs or 399546948150 ns
    }
}