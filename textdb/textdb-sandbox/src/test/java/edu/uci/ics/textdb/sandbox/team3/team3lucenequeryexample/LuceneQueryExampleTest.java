package edu.uci.ics.textdb.sandbox.team3.team3lucenequeryexample;

import static org.junit.Assert.*;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import edu.uci.ics.textdb.sandbox.team3.team3lucenequeryexample.LuceneQueryExample;

public class LuceneQueryExampleTest {
	LuceneQueryExample queryExample;

	@Before
	public void setUp() throws Exception {
		queryExample = new LuceneQueryExample("team3datafile.txt", 3, 3);
		queryExample.initiateSearcher();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws Exception{
		queryExample.buildNGramIndex();
		
		//perform search "network"
//		String queryText = "data:\"net\" AND data:\"etw\" AND data:\"two\" AND data:\"wor\" AND data: \"ork\" ";
		String queryText = "data:\"net\" AND \"etw\" AND \"two\" AND \"wor\" AND \"ork\" ";
		TopDocs topdoc = queryExample.search(queryText, 100);
		ScoreDoc[] scoredocs = topdoc.scoreDocs;
		
		int hitCount = 0;
		IndexSearcher searcher = queryExample.getSearcher();
		for (ScoreDoc scoredoc: scoredocs) {
			hitCount += 1;
			Document document = searcher.doc(scoredoc.doc);
			String text = document.getField("data").stringValue();
			assertTrue(text.contains("network"));
		}
		assertEquals(hitCount, 26);
	}

}
