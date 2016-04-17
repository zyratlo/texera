
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
import edu.uci.ics.textdb.common.constants.LuceneConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.storage.LuceneDataStore;

/**
 * @author Sudeep [inkudo] and Rajesh [rajesh9625]
 *
 */
public class DictionaryMatcherTest {

    private DictionaryMatcher dictionaryMatcher;
    private LuceneDataStore dataStore;

    @Before
    public void setUp() throws Exception {
        dataStore = new LuceneDataStore(LuceneConstants.INDEX_DIR);
        dataStore.clearData();
        dataStore.storeData(TestConstants.SAMPLE_SCHEMA_PEOPLE, TestConstants.getSamplePeopleTuples());
    }

    @After
    public void cleanUp() throws Exception {
        dataStore.clearData();
    }

    @Test
    public void testGetNextTuple() throws Exception {

        ArrayList<String> names = new ArrayList<String>(
                Arrays.asList("bruce", "rajesh", "tom", "9261", "636483", "lee", "brad"));

        IDictionary dict = new Dictionary(names);
        String dataDirectory = LuceneConstants.INDEX_DIR;
        ISourceOperator sourceOperator = new ScanBasedSourceOperator(dataDirectory,
                new ArrayList<>(TestConstants.SAMPLE_SCHEMA_PEOPLE));

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
