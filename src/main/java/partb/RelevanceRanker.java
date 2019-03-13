package partb;
import java.util.List;


public interface RelevanceRanker {

    /**
     * Calculate a ranking score for given term and documents
     * @param term given term
     * @param doc given document
     * @param docs all documents term list
     * @return calculated score
     */
    double rank(String term, List<String> doc, List<List<String>> docs);

}