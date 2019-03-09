import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;



public class SearchIndex {

    /**
     * @param args
     */
    public static void main(String[] args) throws IOException {


        Path indexDir = Paths.get("luceneIndex");
        Directory index = FSDirectory.open(indexDir);

        // Build a Query object
        Query query;
        try {
            query = new QueryParser( "text", new StandardAnalyzer()).parse("trump");
        } catch (ParseException e) {
            e.printStackTrace();
            return;
        }

        int hitsPerPage = 10;
        IndexReader reader = DirectoryReader.open(index);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage);
        searcher.search(query, collector);

        System.out.println("total hits: " + collector.getTotalHits());

        ScoreDoc[] hits = collector.topDocs().scoreDocs;
        for (ScoreDoc hit : hits) {
            Document doc = reader.document(hit.doc);
            System.out.println(doc.get("id") + "  (" + hit.score + ")" + hit.toString());
        }

    }

}