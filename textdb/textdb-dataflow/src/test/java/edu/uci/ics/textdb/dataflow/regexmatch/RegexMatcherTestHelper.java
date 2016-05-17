package edu.uci.ics.textdb.dataflow.regexmatch;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.storage.DataReaderPredicate;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.reader.DataReader;
import edu.uci.ics.textdb.storage.writer.DataWriter;


/**
 * @author zuozhi
 * @author shuying
 * Helper class to quickly create unit test
 */
public class RegexMatcherTestHelper {

	private RegexMatcher regexMatcher;
	private IDataWriter dataWriter;
	private IDataReader dataReader;
	private IDataStore dataStore;
	
	private List<ITuple> results;
    private Analyzer analyzer;
    private Query query;
    private IPredicate dataReaderPredicate;
	
	public RegexMatcherTestHelper(Schema schema, List<ITuple> data) throws Exception {
		dataStore = new DataStore(DataConstants.INDEX_DIR, schema);
		analyzer = new  StandardAnalyzer();
        dataWriter = new DataWriter(dataStore, analyzer);
		dataWriter.clearData();
		dataWriter.writeData(data);	
		results = new ArrayList<ITuple>();
	}
	
	public List<ITuple> getResults() {
		return results;
	}
	
	public Schema getSpanSchema() {
		return regexMatcher.getSpanSchema();
	}

	public void runTest(String regex, String fieldName) throws Exception {
		results.clear();
		QueryParser queryParser = new QueryParser(
                TestConstants.FIRST_NAME, analyzer);
        query = queryParser.parse(DataConstants.SCAN_QUERY);
        dataReaderPredicate = new DataReaderPredicate(dataStore, query, DataConstants.SCAN_QUERY,
				analyzer, Arrays.asList(TestConstants.ATTRIBUTES_PEOPLE[0]));
        dataReader = new DataReader(dataReaderPredicate);

		IPredicate predicate = new RegexPredicate(regex, fieldName);
		ISourceOperator sourceOperator = new ScanBasedSourceOperator(dataReader);


		regexMatcher = new RegexMatcher(predicate, sourceOperator);
		regexMatcher.open();
		ITuple nextTuple = null;
		while ((nextTuple = regexMatcher.getNextTuple()) != null) {
			results.add(nextTuple);
		}
		regexMatcher.close();
	}
	
	public void cleanUp() throws Exception {
		dataWriter.clearData();
	}

}
