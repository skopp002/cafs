package partb;

public class SearchResult implements Comparable<SearchResult> {

    private final Object docId;
    private final double score;

    public SearchResult(Object docId, double score) {
        this.docId = docId;
        this.score = score;
    }

    public Object getDocId() {
        return this.docId;
    }


    public double getScore() {
        return this.score;
    }

    @Override
    public int compareTo(SearchResult s) {
        if (this.score < s.getScore()) return -1;
        if (this.score > s.getScore()) return 1;
        return 0;
    }

    @Override
    public String toString() {
        return "DocumentScore{docId=" + this.docId
                + ", score=" + this.score + "}";
    }

}