package partb;

import java.util.*;
import java.util.stream.Collectors;

public class HadoopScoredIndexer<T> {

    private Map<String, Map<Object, Double>> index; // {term, {docId, score}}
    private Map<Object, DocumentMeta> documents;

    private DocumentExtractor<T> extractor;
    private Tokenizer tokenizer;
    private RelevanceRanker ranker;
    private Serializer<Map<String, Map<Object, Double>>> serializer;

    /**
     * Constructor to create a Indexer object with default ranker
     * @param tokenizer for term and document
     */
    public HadoopScoredIndexer(Tokenizer tokenizer) {
        this(new BM25Ranker(), tokenizer);
    }

    /**
     * Constructor to create a Indexer object
     * @param ranker for ranking algorithm(BM25, TFIDF, etc..)
     * @param tokenizer for term and document
     */
    public HadoopScoredIndexer(RelevanceRanker ranker, Tokenizer tokenizer) {
        this.documents = new HashMap<>();
        this.serializer = new Serializer<>();
        this.ranker = ranker;
        this.extractor = new DocumentExtractor<>(tokenizer);
        this.tokenizer = tokenizer;

    }

    /**
     * Building a inverted index
     */
    public void build() {
        this.index = new HashMap<>();

        List<List<String>> docs = this.convertDocumentsToStringList();

        for (Object docId : this.documents.keySet()) {
            DocumentMeta doc = this.documents.get(docId);

            for (String word : doc.getTokenizedWords()) {
                Map<Object, Double> scores = this.index.get(word);
                if (scores == null) {
                    scores = new HashMap<>();
                    this.index.put(word, scores);
                }

                double score = scores.getOrDefault(docId, 0.0);
                score += this.ranker.rank(word, doc.getTokenizedWords(), docs);
                scores.put(docId, score);
            }
        }
    }

    /**
     * Adding a doucment.
     * @param doc to add
     * @return true if success to add
     * @throws IllegalArgumentException when the DocumentId is not designated
     */
    public boolean add(T doc) throws IllegalArgumentException {
        boolean allowOverwrite = false;
        return this.add(doc, allowOverwrite);
    }

    /**
     * Adding a doucment.
     * @param doc doc to add
     * @param allowOverwrite true if you want to allow overwrite document for same document id
     * @return true if success to add
     * @throws IllegalArgumentException when the DocumentId is not designated
     */
    public boolean add(T doc, boolean allowOverwrite) throws IllegalArgumentException {
//        if (!this.extractor.isDocument(doc)) return false;

        DocumentMeta meta;
        try {
            meta = this.extractor.extractDocumentMeta(doc);
        } catch (IllegalAccessException e) {
            return false;
        }

        if (!allowOverwrite && this.documents.containsKey(meta.getDocId()))
            throw new IllegalArgumentException(meta.getDocId().toString());

        this.documents.put(meta.getDocId(), meta);
        return true;
    }

    /**
     * Saving a index into local disk.
     * @param fileName to save a index into the local disk
     * @return true if success to save
     */
    public boolean save(String fileName) {
        assert this.index != null;
        try {
            this.serializer.serializing(fileName, this.index);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    private List<List<String>> convertDocumentsToStringList() {
        List<List<String>> docs = new ArrayList<>();
        for (Object docId : this.documents.keySet()) {
            docs.add(this.documents.get(docId).getTokenizedWords());
        }
        return docs;
    }
}
