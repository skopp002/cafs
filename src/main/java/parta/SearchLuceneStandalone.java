package parta;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

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

public class SearchLuceneStandalone implements Serializable {

    public static void main (String args[]) {
        if(args.length != 1){
            System.out.println("Enter search term");
        }

        // Build a Query object
        Query query;
        try {
            HashMap<String, String> results = new HashMap();
            Path indexDir = Paths.get("luceneIndex");
            Directory index = FSDirectory.open(indexDir);
            SearchLuceneIndex s = new SearchLuceneIndex();
            query = new QueryParser("text", new StandardAnalyzer()).parse(args[0]);

            int hitsPerPage = 56;
            IndexReader reader = DirectoryReader.open(index);
            IndexSearcher searcher = new IndexSearcher(reader);
            TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage);
            searcher.search(query, collector);

            System.out.println("total hits: " + collector.getTotalHits());

            ScoreDoc[] hits = collector.topDocs().scoreDocs;
            for (ScoreDoc hit : hits) {
                Document doc = reader.document(hit.doc);
                System.out.println(doc.get("id") + "  (" + hit.score + ")" + doc.get("text"));
            }

        } catch (ParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
