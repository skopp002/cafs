package partb;

import java.util.List;

public class DocumentMeta {

    private final Object docId;
    private final List<String> tokenizedWords;

    public DocumentMeta(Object docId, List<String> tokenizedWords) {
        this.docId = docId;
        this.tokenizedWords = tokenizedWords;
    }

    public Object getDocId() {
        return this.docId;
    }

    public List<String> getTokenizedWords() {
        return this.tokenizedWords;
    }

    @Override
    public String toString() {
        return "DocumentMeta{docId=" + this.docId.toString()
                + ", tokenizedWords=" + this.tokenizedWords.toString() + "}";
    }
}