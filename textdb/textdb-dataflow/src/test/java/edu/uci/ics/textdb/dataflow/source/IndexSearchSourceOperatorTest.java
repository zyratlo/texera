/**
 * 
 */
package edu.uci.ics.textdb.dataflow.source;

import java.text.ParseException;
import java.util.List;

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
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.LuceneDataStore;
import edu.uci.ics.textdb.storage.reader.LuceneDataReader;
import edu.uci.ics.textdb.storage.writer.LuceneDataWriter;

/**
 * @author akshaybetala
 *
 */
public class IndexSearchSourceOperatorTest {
    
    private IDataWriter dataWriter;
    private IndexSearchSourceOperator indexSearchSourceOperator;
    private IDataStore dataStore;
    
    @Before
    public void setUp() throws Exception{
        dataStore = new LuceneDataStore(LuceneConstants.INDEX_DIR, TestConstants.SAMPLE_SCHEMA_PEOPLE);
        dataWriter = new LuceneDataWriter(dataStore);
        dataWriter.clearData();
        dataWriter.writeData(TestConstants.getSamplePeopleTuples());
    }
    
    @After
    public void cleanUp() throws Exception{
        dataWriter.clearData();
    }
    
    public int getTupleCount(String query) throws DataFlowException, ParseException{
    	String deaultField = TestConstants.FIRST_NAME;
    	IDataReader dataReader = new LuceneDataReader(dataStore,query,deaultField);
        indexSearchSourceOperator = new IndexSearchSourceOperator(dataReader);
        indexSearchSourceOperator.open();
        int numTuples = 0;
        while(indexSearchSourceOperator.getNextTuple() != null){
            numTuples ++;
        }
        indexSearchSourceOperator.close();
        return numTuples;
    }
    
    @Test
    public void testTextSearch1() throws DataFlowException, ParseException{
        int numTuples = getTupleCount(TestConstants.DESCRIPTION+ ":Tall,Brown");
        Assert.assertEquals(3, numTuples);
    }
    
    @Test
    public void testTextSearch2() throws DataFlowException, ParseException{
        int numTuples = getTupleCount(TestConstants.DESCRIPTION+ ":(+Tall,Brown)");
        Assert.assertEquals(3, numTuples);
    }
    
    @Test
    public void testStringMatchFail() throws DataFlowException, ParseException{
        int numTuples = getTupleCount("lin");
        Assert.assertEquals(0, numTuples);
    }
    
    @Test
    public void testMultipleFields() throws DataFlowException, ParseException{
        int numTuples = getTupleCount(TestConstants.DESCRIPTION+ ":(Tall,Brown)" +" AND " +TestConstants.LAST_NAME+ ":cruise");
        Assert.assertEquals(1, numTuples);
    }
}
