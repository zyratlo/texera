/**
 * 
 */
package edu.uci.ics.textdb.dataflow.source;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.dataflow.constants.LuceneConstants;

/**
 * @author sandeepreddy602
 *
 */
public class ScanBasedSourceOperatorTest {
    
    private SampleDataStore dataStore;
    private ScanBasedSourceOperator scanBasedSourceOperator;
    
    @Before
    public void setUp() throws Exception{
        dataStore = new SampleDataStore();
        dataStore.clearData();
        dataStore.storeData();
        scanBasedSourceOperator = new ScanBasedSourceOperator(
                LuceneConstants.INDEX_DIR, TestConstants.SAMPLE_SCHEMA);
    }
    
    @After
    public void cleanUp() throws IOException{
        dataStore.clearData();
    }
    
    @Test
    public void testFlow() throws DataFlowException{
        scanBasedSourceOperator.open();
        ITuple nextTuple = null;
        int counter = 0;
        while((nextTuple  = scanBasedSourceOperator.getNextTuple()) != null){
            assertEquality(TestConstants.SAMPLE_TUPLES.get(counter++), nextTuple);
        }
        scanBasedSourceOperator.close();
    }
    
    private void assertEquality(ITuple expectedTuple, ITuple actualTuple) {
        int schemaSize = TestConstants.SAMPLE_SCHEMA.size();
        for (int i = 0; i < schemaSize; i++) {
            Assert.assertEquals(expectedTuple.getField(i), actualTuple.getField(i));
        }
        
    }
}
