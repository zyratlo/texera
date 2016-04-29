package edu.uci.ics.textdb.dataflow.neextractor;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;

import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.constants.LuceneConstants;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.TextField;
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
import org.junit.Assert;
import org.junit.Before;
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
     * Scenario 1: Get next tuple with single return
     * Text : Microsoft is a organization.
     */
    @Test
    public void getNextTupleTest1() throws Exception {
        List<ITuple> data = NEExtractorTestConstants.getTest1Tuples();
        IOperator sourceOperator = TestHelper(data.get(0).getSchema(),data);

        NamedEntityExtractor neExtractor = new NamedEntityExtractor(sourceOperator);
        neExtractor.open();

        ITuple iTuple=neExtractor.getNextTuple();
        ITuple targetTuple=NEExtractorTestConstants.getTest1ResultTuple();
      //  Assert.assertTrue(TestUtils.equalTo(iTuple, targetTuple));
        Assert.assertTrue(neExtractor.getNextTuple() == null);
        neExtractor.close();
    }

    /**
     * Scenario 2: Get next tuple with multiple return
     * Text: Microsoft, Google and Facebook are organizations
     */
    @Test
    public void getNextTupleTest2() throws Exception {
        List<ITuple> data = NEExtractorTestConstants.getTest2Tuples();
        IOperator sourceOperator = TestHelper(data.get(0).getSchema(),data);

        NamedEntityExtractor neExtractor = new NamedEntityExtractor(sourceOperator);
        neExtractor.open();

        ITuple iTuple = neExtractor.getNextTuple();
        ITuple targetTuple=NEExtractorTestConstants.getTest2ResultTuple();
     //   Assert.assertTrue(TestUtils.equalTo(iTuple,targetTuple));
        Assert.assertTrue(neExtractor.getNextTuple() == null);
        neExtractor.close();
    }

    /**
     * Scenario 2: Get next tuple with multiple return with different idendities
     * Text: Microsoft, Google and Facebook are organizations and Donald Trump and Barack Obama are persons.
     */
    @Test
    public void getNextTupleTest3() throws Exception {
        List<ITuple> data = NEExtractorTestConstants.getTest3Tuples();
        IOperator sourceOperator = TestHelper(data.get(0).getSchema(),data);

        NamedEntityExtractor neExtractor = new NamedEntityExtractor(sourceOperator);
        neExtractor.open();

        ITuple iTuple = neExtractor.getNextTuple();
        ITuple targetTuple=NEExtractorTestConstants.getTest3ResultTuple();
    //    Assert.assertTrue(TestUtils.equalTo(iTuple,targetTuple));
        Assert.assertTrue(neExtractor.getNextTuple() == null);
        neExtractor.close();
    }




    /**
     * Scenario 2: Get next tuple with multiple return with different idendities in two sentence(field).
     * Text: Microsoft, Google and Facebook are organizations and Donald Trump and Barack Obama are persons.
     */
    @Test
    public void getNextTupleTest4() throws Exception {
        List<ITuple> data = NEExtractorTestConstants.getTest4Tuples();
        IOperator sourceOperator= TestHelper(data.get(0).getSchema(),data);

        NamedEntityExtractor neExtractor = new NamedEntityExtractor(sourceOperator);
        neExtractor.open();

        ITuple iTuple =neExtractor.getNextTuple();
        //TODO: discuss the definition of return field.
        ITuple targetTuple=NEExtractorTestConstants.getTest3ResultTuple();
     //   Assert.assertTrue(TestUtils.equalTo(iTuple,targetTuple));
        Assert.assertTrue(neExtractor.getNextTuple() == null);
        neExtractor.close();
    }



    public IOperator TestHelper(Schema schema, List<ITuple> data) throws Exception {
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
