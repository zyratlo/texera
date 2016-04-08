package edu.uci.ics.textdb.dataflow.regexmatch;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;
import edu.uci.ics.textdb.dataflow.constants.LuceneConstants;
import edu.uci.ics.textdb.dataflow.source.SampleDataStore;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.dataflow.source.TestConstants;

/**
 * Created by chenli on 3/25/16.
 */
public class RegexMatcherTest {
    
    private RegexMatcher regexMatcher;
    private SampleDataStore dataStore;
    
    @Before
    public void setUp() throws Exception{
        dataStore = new SampleDataStore();
        dataStore.clearData();
        dataStore.storeData();
    }
    
    @After
    public void cleanUp() throws IOException{
        dataStore.clearData();
    }
    
    @Test
    public void testGetNextTuple() throws Exception{
        String regex = "f.";
        String fieldName = TestConstants.FIRST_NAME;
        IPredicate predicate = new RegexPredicate(regex, fieldName);
        String dataDirectory = LuceneConstants.INDEX_DIR;
        List<String> schema = Arrays.asList(TestConstants.FIRST_NAME, TestConstants.LAST_NAME);
        ISourceOperator sourceOperator = new ScanBasedSourceOperator(dataDirectory, schema);
        sourceOperator.open();
        regexMatcher = new RegexMatcher(predicate, sourceOperator);
        
        ITuple nextTuple = null;
        int counter = 0;
        while((nextTuple = regexMatcher.getNextTuple()) != null){
            assertEquality(TestConstants.SAMPLE_TUPLES.get(counter++), nextTuple);
        }
    }
    
    private void assertEquality(ITuple expectedTuple, ITuple actualTuple) {
        int schemaSize = TestConstants.SAMPLE_SCHEMA.size();
        for (int i = 0; i < schemaSize; i++) {
            Assert.assertEquals(expectedTuple.getField(i), actualTuple.getField(i));
        }
        
    }

}