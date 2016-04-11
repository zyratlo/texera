package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.common.constants.LuceneConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.DataStore;

/**
 * Created by chenli on 3/25/16.
 */
public class RegexMatcherTest {
    
    private RegexMatcher regexMatcher;
    private DataStore dataStore;
    
    @Before
    public void setUp() throws Exception{
        dataStore = new DataStore(LuceneConstants.INDEX_DIR);
        dataStore.clearData();
        dataStore.storeData(TestConstants.SAMPLE_SCHEMA, TestConstants.getSampleTuples());
    }
    
    @After
    public void cleanUp() throws Exception{
        dataStore.clearData();
    }
    
    @Test
    public void testGetNextTuple() throws Exception{
        String regex = "s.*"; //matches sandeep and sudeep
        String fieldName = TestConstants.FIRST_NAME;
        IPredicate predicate = new RegexPredicate(regex, fieldName);
        String dataDirectory = LuceneConstants.INDEX_DIR;
        ISourceOperator sourceOperator = new ScanBasedSourceOperator(dataDirectory, TestConstants.SAMPLE_SCHEMA);
        List<ITuple> tuples = TestConstants.getSampleTuples();
        
        regexMatcher = new RegexMatcher(predicate, sourceOperator);
        regexMatcher.open();
        ITuple nextTuple = null;
        int numTuples = 0;
        while((nextTuple = regexMatcher.getNextTuple()) != null){
            boolean contains = TestUtils.contains(tuples, nextTuple);
            Assert.assertTrue(contains);
            numTuples ++;
        }
        Assert.assertEquals(2, numTuples);
        regexMatcher.close();
    }

}