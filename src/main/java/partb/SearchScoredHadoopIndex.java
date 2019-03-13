package partb;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SearchScoredHadoopIndex {
    private Map<String, Map<Object, Double>> index; // {term, {docId, score}}
    private Map<Object, DocumentMeta> documents;
    private Tokenizer tokenizer;
    private RelevanceRanker ranker;
    private Serializer<Map<String, Map<Object, Double>>> serializer;
    /**
 * Loading a index from the designate file path.
 * @param fileName to load a index from the local disk
 * @return true if success to load
 */
public boolean load(String fileName) {
    try {
        this.index = this.serializer.deserializing(fileName);
    } catch (Exception e) {
        return false;
    }
    assert this.index != null;
    return true;
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
        List<String> queries = this.tokenizer.tokenizing(query);
        Map<Object, Double> results = new HashMap<>(); // {docId, score}
        for (String q : queries) {
            if (index.containsKey(q)) {
                Map<Object, Double> scores = this.index.get(q);
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
}
