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
import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement
public class SearchLuceneIndex implements Serializable {
    private String searchTerm;

    public String getSearchTerm() {
        return searchTerm;
    }

    public void setSearchTerm(String searchTerm) {
        this.searchTerm = searchTerm;
    }

    public HashMap<String, String> searchLuceneIndex() {


        // Build a Query object
        Query query;
        try {
            HashMap<String, String> results = new HashMap();
            Path indexDir = Paths.get("luceneIndex");
            Directory index = FSDirectory.open(indexDir);
            SearchLuceneIndex s = new SearchLuceneIndex();
            query = new QueryParser("text", new StandardAnalyzer()).parse(searchTerm);


            int hitsPerPage = 10;
            IndexReader reader = DirectoryReader.open(index);
            IndexSearcher searcher = new IndexSearcher(reader);
            TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage);
            searcher.search(query, collector);

            System.out.println("total hits: " + collector.getTotalHits());

            ScoreDoc[] hits = collector.topDocs().scoreDocs;
            for (ScoreDoc hit : hits) {
                Document doc = reader.document(hit.doc);
                results.put(doc.get("id"), "  (" + hit.score + ")" + hit.toString());
            }
            return results;
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

    }
}