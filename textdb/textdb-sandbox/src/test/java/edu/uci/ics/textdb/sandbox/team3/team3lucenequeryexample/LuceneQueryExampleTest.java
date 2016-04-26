package edu.uci.ics.textdb.sandbox.team3.team3lucenequeryexample;

import static org.junit.Assert.*;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.sandbox.team3.team3lucenequeryexample.LuceneNgramQueryExample;

public class LuceneQueryExampleTest {
	LuceneNgramQueryExample queryExample;

	@Before
	public void setUp() throws Exception {
		queryExample = new LuceneNgramQueryExample("team3datafile.txt", 3, 3);
		queryExample.buildNGramIndex();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void queryNetworkShouldReturnTwoDocuments() throws Exception{
		//perform search "network"
		String queryText = "data:\"net\" AND data:\"etw\" AND data:\"two\" AND data:\"wor\" AND data:\"ork\" ";
		TopDocs topdoc = queryExample.search(queryText, 100);
		assertEquals(topdoc.totalHits, 2);
		
		ScoreDoc[] scoredocs = topdoc.scoreDocs;
		IndexSearcher searcher = queryExample.getSearcher();
		
		Document matchDoc = searcher.doc(scoredocs[0].doc);
		String matchText = matchDoc.getField("data").stringValue();
		assertTrue(matchText.equals("networkx 1.1.1"));
		
		matchDoc = searcher.doc(scoredocs[1].doc);
		matchText = matchDoc.getField("data").stringValue();
		assertTrue(matchText.equals("net etw twogram index work find "));
		
	}

}
