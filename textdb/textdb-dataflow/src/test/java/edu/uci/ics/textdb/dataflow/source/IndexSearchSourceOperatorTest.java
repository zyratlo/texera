/**
 * 
 */
package edu.uci.ics.textdb.dataflow.source;


import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.constants.LuceneConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.storage.LuceneDataStore;
import edu.uci.ics.textdb.storage.reader.LuceneDataReader;
import edu.uci.ics.textdb.storage.writer.LuceneDataWriter;

/**
 * @author akshaybetala
 *
 */
public class IndexSearchSourceOperatorTest {

	private IDataWriter dataWriter;
	private IDataReader dataReader;
	private IndexSearchSourceOperator indexSearchSourceOperator;
	private IDataStore dataStore;
	private Analyzer analyzer;
	private Query query;

	@Before
	public void setUp() throws Exception {
		dataStore = new LuceneDataStore(LuceneConstants.INDEX_DIR, TestConstants.SCHEMA_PEOPLE);
		analyzer = new  StandardAnalyzer();
		dataWriter = new LuceneDataWriter(dataStore,analyzer );

		QueryParser queryParser = new QueryParser(
				TestConstants.ATTRIBUTES_PEOPLE.get(0).getFieldName(), analyzer);
		query = queryParser.parse(LuceneConstants.SCAN_QUERY);
		dataReader = new LuceneDataReader(dataStore, query);
		dataWriter.clearData();
		dataWriter.writeData(TestConstants.getSamplePeopleTuples());
	}

	@After
	public void cleanUp() throws Exception {
		dataWriter.clearData();
	}

	public List<ITuple> getTupleCount(String q) throws DataFlowException, ParseException {
		String defaultField = TestConstants.FIRST_NAME;
        QueryParser queryParser = new QueryParser(
                TestConstants.ATTRIBUTES_PEOPLE.get(0).getFieldName(), analyzer);
        query  = queryParser.parse(q);
		dataReader = new LuceneDataReader(dataStore, query);

		IDataReader dataReader = new LuceneDataReader(dataStore, query);
		indexSearchSourceOperator = new IndexSearchSourceOperator(dataReader);
		indexSearchSourceOperator.open();


		List<ITuple> results = new ArrayList<ITuple>();
		ITuple nextTuple = null;
		while ((nextTuple = indexSearchSourceOperator.getNextTuple()) != null) {
			results.add(nextTuple);
		}
		indexSearchSourceOperator.close();
		return results;
	}

	@Test
	public void testTextSearcWithMultipleTokens() throws DataFlowException, ParseException {
		List<ITuple> results = getTupleCount(TestConstants.DESCRIPTION + ":Tall,Brown");
		int numTuples = results.size();
		Assert.assertEquals(3, numTuples);

		for (ITuple tuple : results) {
			String value = (String) tuple.getField(TestConstants.DESCRIPTION).getValue();
			Assert.assertTrue(value.toLowerCase().contains("tall") || value.toLowerCase().contains("brown"));
		}
	}

	@Test
	public void testTextSearchWithSingleToken() throws DataFlowException, ParseException {
		List<ITuple> results = getTupleCount(TestConstants.DESCRIPTION + ":Tall");
		int numTuples = results.size();
		for (ITuple tuple : results) {
			String value = (String) tuple.getField(TestConstants.DESCRIPTION).getValue();
			Assert.assertTrue(value.toLowerCase().contains("tall"));
		}

		Assert.assertEquals(2, numTuples);
	}

	@Test
	public void testStringSearchWithSubstring() throws DataFlowException, ParseException {
		List<ITuple> results = getTupleCount("lin");
		int numTuples = results.size();
		Assert.assertEquals(0, numTuples);
	}

	@Test
	public void testMultipleFields() throws DataFlowException, ParseException {
		List<ITuple> results = getTupleCount(
				TestConstants.DESCRIPTION + ":(Tall,Brown)" + " AND " + TestConstants.LAST_NAME + ":cruise");
		int numTuples = results.size();
		Assert.assertEquals(1, numTuples);

		for (ITuple tuple : results) {
			String descriptionValue = (String) tuple.getField(TestConstants.DESCRIPTION).getValue();
			String lastNameValue = (String) tuple.getField(TestConstants.LAST_NAME).getValue();
			Assert.assertTrue(descriptionValue.toLowerCase().contains("tall")
					|| descriptionValue.toLowerCase().contains("brown"));
			Assert.assertTrue(lastNameValue.toLowerCase().contains("cruise"));
		}
	}
}
