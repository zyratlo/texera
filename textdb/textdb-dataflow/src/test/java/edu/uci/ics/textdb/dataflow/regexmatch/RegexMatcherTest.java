package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.constants.LuceneConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.LuceneDataStore;
import edu.uci.ics.textdb.storage.reader.LuceneDataReader;
import edu.uci.ics.textdb.storage.writer.LuceneDataWriter;

/**
 * Created by chenli on 3/25/16.
 */
public class RegexMatcherTest {
    
    private RegexMatcher regexMatcher;
    private IDataWriter dataStore;
    private IDataReader dataReader;
    
    @Before
    public void setUp() throws Exception{
        dataStore = new LuceneDataWriter(LuceneDataStore.DATA_STORE_DIRECTORY);
        dataStore.clearData();
        dataStore.writeData(TestConstants.SAMPLE_SCHEMA_PEOPLE, TestConstants.getSamplePeopleTuples());
        dataReader = new LuceneDataReader(
                LuceneDataStore.DATA_STORE_DIRECTORY, TestConstants.SAMPLE_SCHEMA_PEOPLE, 
                LuceneConstants.SCAN_QUERY, TestConstants.SAMPLE_SCHEMA_PEOPLE.get(0).getFieldName());
    }
    
    @After
    public void cleanUp() throws Exception{
        dataStore.clearData();
    }
    
    @Test
    public void testGetNextTuple() throws Exception{
        String regex = "b.*"; //matches bruce and brad
        String fieldName = TestConstants.FIRST_NAME;
        IPredicate predicate = new RegexPredicate(regex, fieldName);
        String dataDirectory = LuceneDataStore.DATA_STORE_DIRECTORY;
        ISourceOperator sourceOperator = new ScanBasedSourceOperator(dataReader);
        List<ITuple> tuples = TestConstants.getSamplePeopleTuples();
        
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