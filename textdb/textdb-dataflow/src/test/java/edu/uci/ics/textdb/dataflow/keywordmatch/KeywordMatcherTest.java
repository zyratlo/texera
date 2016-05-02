package edu.uci.ics.textdb.dataflow.keywordmatch;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.constants.LuceneConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.dictionarymatcher.DictionaryMatcher;
import edu.uci.ics.textdb.dataflow.source.IndexSearchSourceOperator;
import edu.uci.ics.textdb.storage.LuceneDataStore;
import edu.uci.ics.textdb.storage.reader.LuceneDataReader;
import edu.uci.ics.textdb.storage.writer.LuceneDataWriter;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Attr;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Prakul
 *
 */


public class KeywordMatcherTest {

    private KeywordMatcher keywordMatcher;
    private IDataWriter dataWriter;
    private IDataReader dataReader;
    private LuceneDataStore dataStore;
    private IndexSearchSourceOperator indexSearchSourceOperator;

    private Analyzer analyzer;
    private Query queryObj;
    private Schema schema;

    @Before
    public void setUp() throws Exception {
        dataStore = new LuceneDataStore(LuceneConstants.INDEX_DIR, TestConstants.SCHEMA_PEOPLE);
        analyzer = new StandardAnalyzer();
        dataWriter = new LuceneDataWriter(dataStore, analyzer);
        dataWriter.clearData();
        dataWriter.writeData(TestConstants.getSamplePeopleTuples());
        schema = dataStore.getSchema();
    }

    @After
    public void cleanUp() throws Exception {
        dataWriter.clearData();
    }

    public List<ITuple> getQueryResults(String query) throws DataFlowException, ParseException {
        String defaultField = TestConstants.ATTRIBUTES_PEOPLE.get(5).getFieldName();

        ArrayList<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(TestConstants.ATTRIBUTES_PEOPLE.get(5));
        Analyzer analyzer = new StandardAnalyzer();
        IPredicate predicate = new KeywordPredicate(query, attributeList, analyzer);
        //ISourceOperator sourceOperator = new IndexSearchSourceOperator();

        QueryParser queryParser = new QueryParser(defaultField, analyzer);
        queryObj = queryParser.parse(query);
        IDataReader dataReader = new LuceneDataReader(dataStore, queryObj);
        indexSearchSourceOperator = new IndexSearchSourceOperator(dataReader);
        //indexSearchSourceOperator.open();

        keywordMatcher = new KeywordMatcher(predicate, indexSearchSourceOperator);
        keywordMatcher.open();

        List<ITuple> results = new ArrayList<ITuple>();
        ITuple nextTuple = null;
        while ((nextTuple = keywordMatcher.getNextTuple()) != null) {
            results.add(nextTuple);
        }
        indexSearchSourceOperator.close();
        return results;
    }


    @Test
    public void testKeywordMatcher() throws Exception {
        String query = "short tall";
        List<ITuple> list = getQueryResults(query);
        Assert.assertEquals(4,list.size());
    }
}
