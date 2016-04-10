package edu.uci.ics.textdb.sandbox.team2luceneexample;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import edu.uci.ics.textdb.sandbox.team2luceneexample.LuceneConstants;

import java.io.IOException;
import java.nio.file.Paths;

import static sun.jvm.hotspot.oops.CellTypeState.top;

/**
 * Created by kishorenarendran on 08/04/16.
 */
public class PokemonSearcher {
    private IndexSearcher indexSearcher;
    private QueryParser queryParser;

    public PokemonSearcher() throws IOException {
        indexSearcher = new IndexSearcher(DirectoryReader.open(
                FSDirectory.open(Paths.get(LuceneConstants.INDEX))));
        queryParser = new QueryParser(LuceneConstants.NAME_FIELD, new StandardAnalyzer());
    }

    public TopDocs performSearch(String queryString, int n) throws IOException,
            ParseException {
        Query query = queryParser.parse(queryString);
        return indexSearcher.search(query, n);
    }

    public Document getDocument(int docId) throws IOException {
        return indexSearcher.doc(docId);
    }

    public static void main(String args[])throws IOException, ParseException {
        PokemonSearcher pokemonSearcher = new PokemonSearcher();
        TopDocs topDocs = pokemonSearcher.performSearch("squirtle", 3);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        for(ScoreDoc scoreDoc: scoreDocs) {
            Document document = pokemonSearcher.getDocument(scoreDoc.doc);
            System.out.println(document.get(LuceneConstants.NAME_FIELD) + " " + document.get(LuceneConstants.ID_FIELD));
        }
    }
}
