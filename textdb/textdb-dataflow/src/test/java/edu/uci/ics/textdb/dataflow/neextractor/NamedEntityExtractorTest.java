package edu.uci.ics.textdb.dataflow.neextractor;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;

import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.constants.LuceneConstants;
import edu.uci.ics.textdb.dataflow.neextrator.NamedEntityExtractor;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.LuceneDataStore;
import edu.uci.ics.textdb.storage.reader.LuceneDataReader;
import edu.uci.ics.textdb.storage.writer.LuceneDataWriter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import org.apache.lucene.analysis.Analyzer;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Sam on 16/4/27.
 */
public class NamedEntityExtractorTest {
    private NamedEntityExtractor neextractor;

    private IDataWriter dataWriter;
    private IDataReader dataReader;
    private IDataStore dataStore;

    private Query query;
    private Analyzer analyzer;



    @After
    public void cleanUp() throws Exception {
        dataWriter.clearData();
    }


    /**
     *
     *
     * @param sourceOperator
     * @return
     * @throws Exception
     *
     * @about Using NamedEntityExtractor to get all return result from sourceOperator,
     *          return as a list of tuples
     */
    public List<ITuple> getQueryResults(ISourceOperator sourceOperator) throws Exception {

        neextractor = new NamedEntityExtractor(sourceOperator);
        neextractor.open();
        ITuple nextTuple = null;
        List<ITuple> results = new ArrayList<ITuple>();
        while ((nextTuple = neextractor.getNextTuple()) != null) {
            results.add(nextTuple);
        }
        neextractor.close();
        return results;
    }



    /**
     * Scenario 1: Get next tuple with single return
     * Text : Microsoft is a organization.
     */
    @Test
    public void getNextTupleTest1() throws Exception {
        List<ITuple> data = NEExtractorTestConstants.getTest1Tuple();
        ISourceOperator sourceOperator = getSourceOperator(data.get(0).getSchema(),data);

        List<ITuple> returnedResults = getQueryResults(sourceOperator);

        List<ITuple> expectedResults = NEExtractorTestConstants.getTest1ResultTuples();

        boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);
        //TODO: enable test while finish implementation
       // Assert.assertTrue(contains);
    }

    /**
     * Scenario 2: Get next tuple with multiple return
     * Text: Microsoft, Google and Facebook are organizations
     */
    @Test
    public void getNextTupleTest2() throws Exception {
        List<ITuple> data = NEExtractorTestConstants.getTest2Tuple();
        ISourceOperator sourceOperator = getSourceOperator(data.get(0).getSchema(),data);

        List<ITuple> returnedResults = getQueryResults(sourceOperator);
        List<ITuple> expectedResults = NEExtractorTestConstants.getTest2ResultTuples();

        boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);
        //TODO: enable test while finish implementation
       // Assert.assertTrue(contains);

    }

    /**
     * Scenario 3: Get next tuple with multiple return with different idendities
     * Text: Microsoft, Google and Facebook are organizations and Donald Trump and Barack Obama are persons.
     */
    @Test
    public void getNextTupleTest3() throws Exception {
        List<ITuple> data = NEExtractorTestConstants.getTest3Tuple();
        ISourceOperator sourceOperator = getSourceOperator(data.get(0).getSchema(),data);

        List<ITuple> returnedResults = getQueryResults(sourceOperator);
        List<ITuple> expectedResults = NEExtractorTestConstants.getTest3ResultTuples();

        boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);

        //TODO: enable test while finish implementation
        //Assert.assertTrue(contains);
    }




    /**
     * Scenario 4: Get next tuple with multiple return with different idendities in two sentence(field).
     * Text: Microsoft, Google and Facebook are organizations and Donald Trump and Barack Obama are persons.
     */
    @Test
    public void getNextTupleTest4() throws Exception {
        List<ITuple> data = NEExtractorTestConstants.getTest4Tuple();
        ISourceOperator sourceOperator = getSourceOperator(data.get(0).getSchema(),data);

        List<ITuple> returnedResults = getQueryResults(sourceOperator);

        //TODO:discuss the return type for tuple that have multiple field, how should the span start and end look like?
        
        List<ITuple> expectedResults = NEExtractorTestConstants.getTest3ResultTuples();

        boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);

        //TODO: enable test while finish implementation
        //Assert.assertTrue(contains);
    }


    /**
     *
     * @param schema  The data schema
     * @param data
     * @return
     * @throws Exception
     *
     * @about construct a source operator using given schema and data
     */

    public ISourceOperator getSourceOperator(Schema schema, List<ITuple> data) throws Exception {
        dataStore = new LuceneDataStore(LuceneConstants.INDEX_DIR, schema);
        analyzer = new  StandardAnalyzer();
        dataWriter = new LuceneDataWriter(dataStore, analyzer);
        dataWriter.writeData(data);

        QueryParser queryParser = new QueryParser(NEExtractorTestConstants.ATTRIBUTES_ONE_SENTENCE.get(0).getFieldName(),analyzer);
        query = queryParser.parse(LuceneConstants.SCAN_QUERY);
        dataReader = new LuceneDataReader(dataStore, query);

        ISourceOperator sourceOperator = new ScanBasedSourceOperator(dataReader);
        return sourceOperator;

    }

}
