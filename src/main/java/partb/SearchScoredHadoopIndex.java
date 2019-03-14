package partb;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class SearchScoredHadoopIndex {

    public  Map<String, Map<String, Double>> buildIndex(String indexPath){
        String lines;
        Map<String, Map<String, Double>> INDEX=new HashMap<>();
        try {
            BufferedReader br  = new BufferedReader(new FileReader(new File(indexPath)));
            while ((lines = br.readLine()) != null) {
                String[] tweetlist = lines.split("\\|\\|NextTag\\|\\|");
                Map<String, Double> tagDocMap = new HashMap();

                for (int i = 0; i < tweetlist.length; i++) {
                    try {
                        if (tweetlist[i].startsWith("--KeyStarts---->")) {
                            String hashtag = (tweetlist[i].split("--DocList------->")[0]).replaceAll("--KeyStarts---->", "").replaceAll("\\t", "").replaceAll("\\#","");
                            String[] tweetsPerHashtag = tweetlist[i].split("--DocList------->")[1].split("\\|\\|NextTweet\\|\\|");
                            for (String tweet : tweetsPerHashtag) {
                                double score = Double.parseDouble(tweet.split("___")[0]);
                                String doc = tweet.split("___")[1];
                                tagDocMap.put(doc, score);
                            }
                            INDEX.put(hashtag, tagDocMap);
                        }
                    }  catch(java.lang.ArrayIndexOutOfBoundsException a){
                    }
                }
            }
        } catch (IOException e){
        }
        return INDEX;
    }

    /**
     * Retrieve search results from a inverted index.
     * @param query to search documents
     * @return the list of SearchResult in default topK
     */
    public List<SearchResult> search(Map<String, Map<String, Double>> index,String query) {
        int defaultTopK = 10;
        return this.search(index, query, defaultTopK);
    }

    /**
     * Retrieve search results from a inverted index.
     * @param query to search documents
     * @param topK the number of maximum documents
     * @return the list of SearchResults in topK
     */
    public List<SearchResult> search(Map<String, Map<String, Double>> INDEX, String query, int topK) {
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

    public static void main(String[] args) {
        try {
            long startTime = System.nanoTime();
            SearchScoredHadoopIndex si = new SearchScoredHadoopIndex();
            Map<String, Map<String, Double>> index = si.buildIndex(args[0]);
            List searchResults = si.search(index, args[1]);
            System.out.println(searchResults.toString());
            long endTime = System.nanoTime();
            System.out.println("Took "+(endTime - startTime)/1000000 + " seconds");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


