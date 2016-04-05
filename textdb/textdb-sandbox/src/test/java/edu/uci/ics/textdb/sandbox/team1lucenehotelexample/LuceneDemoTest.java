package edu.uci.ics.textdb.sandbox.team1lucenehotelexample;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.junit.Before;
import org.junit.Test;

public class LuceneDemoTest {

	private Indexer indexer;
	private Searcher searcher;

	@Before
	public void setUp() throws IOException {
		indexer = new Indexer();
	}

	@Test
	public void testIndexing() throws IOException {
		System.out.println("Building Indexes");
		indexer.rebuildIndexes();
		System.out.println("Finished Indexing");
	}

	@Test
	public void testIndexingAndSearching() throws IOException, ParseException {
		System.out.println("Building Indexes");
		indexer.rebuildIndexes();
		System.out.println("Finished Indexing");

		searcher = new Searcher();
		// perform search on user queries
		// and retrieve the top 10 result
		int maxResults = 100;

		// Modify this to try different queries
		String queryString = "*:*";
		System.out.println("\nperformSearch");

		TopDocs topDocs = searcher.performSearch(queryString, maxResults);

		System.out.println("Results found: " + topDocs.totalHits);
		ScoreDoc[] hits = topDocs.scoreDocs;
		for (int i = 0; i < hits.length; i++) {
			Document doc = searcher.getDocument(hits[i].doc);
			System.out.println(
			        "Id: " + doc.get("id") + ", Name: " + doc.get("name") + " " + ", City: " + doc.get("city"));

		}

		System.out.println("performSearch done");

	}
}
