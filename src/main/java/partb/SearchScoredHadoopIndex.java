package partb;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class SearchScoredHadoopIndex {
    private static Map<String, Map<String, Double>> INDEX; // {term, {docId, score}}
    private Map<Object, DocumentMeta> documents;
//    private Tokenizer tokenizer;
//    private Serializer<Map<String, Map<Object, Double>>> serializer;
//

    public static Map<String, Map<String, Double>> getIndex(){
        return INDEX;
    }

    /**
     * Retrieve search results from a inverted index.
     * @param query to search documents
     * @return the list of SearchResult in default topK
     */
    public List<SearchResult> search(String query) {
        int defaultTopK = 100;
        return this.search(query, defaultTopK);
    }

    /**
     * Retrieve search results from a inverted index.
     * @param query to search documents
     * @param topK the number of maximum documents
     * @return the list of SearchResults in topK
     */
    public List<SearchResult> search(String query, int topK) {
        String[] queries = query.split(" ");
        Map<Object, Double> results = new HashMap<>(); // {docId, score}
        for (String q : queries) {
            if (INDEX.containsKey(q)) {
                Map<String, Double> scores = INDEX.get(q);
                for (Object docId : scores.keySet()) {
                    double score = results.getOrDefault(docId, 0.0);
                    score += scores.get(docId);
                    results.put(docId, score);
                }
            }
        }

        return results.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getValue, Comparator.reverseOrder()))
                .map(e -> new SearchResult(e.getKey(), e.getValue()))
                .limit(topK)
                .collect(Collectors.toList());
    }

    public static class SearchMapper extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, Text> {

        private final static Text word = new Text();
        private final static Text location = new Text();

        public void map(LongWritable key, Text val,
                        OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

               String line = val.toString();
               String[] tweetlist =  line.split("||NextTag||");
               Map<String, Double> tagDocMap = new HashMap();
               try{
                for(int i=0;i<tweetlist.length; i++) {
                    String hashtag = tweetlist[i].split("--------->")[0];
                    String[] tweetsPerHashtag = tweetlist[i].split("--------->")[1].split("||NextTweet||");
                    for(String tweet: tweetsPerHashtag){
                        double score = Double.parseDouble(tweet.split("___")[0]);
                        String doc = tweet.split("___")[1];
                        tagDocMap.put(doc,score);
                    }
                  INDEX.put(hashtag,tagDocMap);
                }
                }catch(org.json.JSONException e){
                System.out.println("Exception while searching "+ e.getMessage());
               }

        }



    }
        public static class SearchReducer extends MapReduceBase
                implements Reducer<Text, Text, Text, Text> {

            public void reduce(Text key, Iterator<Text> values,
                               OutputCollector<Text, Text> output, Reporter reporter)
                    throws IOException {
            }

        }

    public static void main(String[] args) {
        try {
            JobClient client = new JobClient();
            JobConf conf = new JobConf(SearchScoredHadoopIndex.class);
            long startTime = System.nanoTime();
            conf.setJobName("ScoredHadoopSearch");
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(conf, new Path(args[0]));
            FileOutputFormat.setOutputPath(conf, new Path("queryresults.csv"));
            conf.setMapperClass(SearchScoredHadoopIndex.SearchMapper.class);
            client.setConf(conf);
            JobClient.runJob(conf);
            SearchScoredHadoopIndex si = new SearchScoredHadoopIndex();
            List searchResults = si.search(args[1]);
            System.out.println(searchResults.toString());
            long endTime = System.nanoTime();
            System.out.println("Took "+(endTime - startTime)/1000000 + " seconds");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


