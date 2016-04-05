package edu.uci.ics.textdb.sandbox.team1lucenehotelexample;

import java.io.IOException;
import java.util.Scanner;

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
		searcher = new Searcher();
	}

	@Test
	public void testIndexing() throws IOException {
		System.out.println("Building Indexes");
		indexer.rebuildIndexes();
		System.out.println("Finished Indexing");
	}

	@Test
	public void testSearching() throws IOException, ParseException {
		// perform search on user queries
		// and retrieve the top 10 result
		int maxResults = 100;

		Scanner reader = new Scanner(System.in); // Reading from System.in
		System.out.println("Enter a search string: ");
		String s = reader.next(); // Scans the next token of the input as an
		                          // int.
		reader.close();
		System.out.println(" performSearch ");

		TopDocs topDocs = searcher.performSearch(s, maxResults);

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
