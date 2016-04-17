
package edu.uci.ics.textdb.dataflow.dictionarymatcher;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.IDictionary;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.constants.LuceneConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.storage.LuceneDataStore;
import edu.uci.ics.textdb.storage.reader.LuceneDataReader;
import edu.uci.ics.textdb.storage.writer.LuceneDataWriter;

/**
 * @author Sudeep [inkudo] and Rajesh [rajesh9625]
 *
 */
public class DictionaryMatcherTest {

    private DictionaryMatcher dictionaryMatcher;
    private LuceneDataStore dataStore;
    private IDataWriter dataWriter;
    private IDataReader dataReader;
    private ScanBasedSourceOperator scanBasedSourceOperator;

    @Before
    public void setUp() throws Exception {

        dataStore = new LuceneDataStore(LuceneConstants.INDEX_DIR, TestConstants.SAMPLE_SCHEMA_PEOPLE);
        dataWriter = new LuceneDataWriter(dataStore);
        dataWriter.clearData();
        dataWriter.writeData(TestConstants.getSamplePeopleTuples());
        dataReader = new LuceneDataReader(dataStore, LuceneConstants.SCAN_QUERY,
                TestConstants.SAMPLE_SCHEMA_PEOPLE.get(0).getFieldName());
        scanBasedSourceOperator = new ScanBasedSourceOperator(dataReader);
    }

    @After
    public void cleanUp() throws Exception {
        dataWriter.clearData();
    }

    @Test
    public void testGetNextTuple() throws Exception {

        ArrayList<String> names = new ArrayList<String>(
                Arrays.asList("bruce", "rajesh", "tom", "9261", "636483", "lee", "brad"));

        IDictionary dict = new Dictionary(names);
        String dataDirectory = LuceneConstants.INDEX_DIR;
        ISourceOperator sourceOperator = new ScanBasedSourceOperator(dataReader);

        dictionaryMatcher = new DictionaryMatcher(dict, sourceOperator);
        dictionaryMatcher.open();
        ITuple nextTuple = null;
        int numTuples = 0;
        while ((nextTuple = dictionaryMatcher.getNextTuple()) != null) {
            System.out.println("numTuples:" + numTuples);
            numTuples++;
        }
        Assert.assertEquals(4, numTuples);
        dictionaryMatcher.close();
    }

}
