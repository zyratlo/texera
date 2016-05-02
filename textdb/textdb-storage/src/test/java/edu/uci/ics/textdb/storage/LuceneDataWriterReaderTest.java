package edu.uci.ics.textdb.storage;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.constants.LuceneConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.storage.reader.LuceneDataReader;
import edu.uci.ics.textdb.storage.writer.LuceneDataWriter;

public class LuceneDataWriterReaderTest {
    private IDataWriter dataWriter;
    private IDataReader dataReader;
    private IDataStore dataStore;
    private Analyzer analyzer;
    private Query query;
    
    @Before
    public void setUp() throws ParseException{
        dataStore = new LuceneDataStore(LuceneConstants.INDEX_DIR, TestConstants.SCHEMA_PEOPLE);
        analyzer = new  StandardAnalyzer();
        dataWriter = new LuceneDataWriter(dataStore, analyzer );
        QueryParser queryParser = new QueryParser(
                TestConstants.ATTRIBUTES_PEOPLE[0].getFieldName(), analyzer);
        query = queryParser.parse(LuceneConstants.SCAN_QUERY);
        dataReader = new LuceneDataReader(dataStore, query);
    }
    
    @Test
    public void testReadWriteData() throws Exception{
        dataWriter.clearData();
        List<ITuple> actualTuples = TestConstants.getSamplePeopleTuples();
        dataWriter.writeData(actualTuples);
        Assert.assertEquals(actualTuples.size(), dataStore.getNumDocuments());
        dataReader.open();
        ITuple nextTuple = null;
        int numTuples = 0;
        List<ITuple> returnedTuples = new ArrayList<ITuple>();
        while((nextTuple  = dataReader.getNextTuple()) != null){
            returnedTuples.add(nextTuple);
            numTuples ++;
        }
        Assert.assertEquals(actualTuples.size(), numTuples);
        boolean contains = containsAllResults(actualTuples, returnedTuples);
		Assert.assertTrue(contains);
        dataReader.close();
    }

    public static boolean containsAllResults(List<ITuple> expectedResults, List<ITuple> exactResults) {
        if(expectedResults.size() != exactResults.size())
        	return false;
        if(!(expectedResults.containsAll(exactResults)) || !(exactResults.containsAll(expectedResults)))
        	return false;
        
        return true;
    }
}
