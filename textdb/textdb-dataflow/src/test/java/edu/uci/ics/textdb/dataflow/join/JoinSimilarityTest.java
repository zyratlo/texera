package edu.uci.ics.textdb.dataflow.join;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;

public class JoinSimilarityTest {
    
    public static final String BOOK_TABLE_OUTER = JoinDistanceHelper.BOOK_TABLE_OUTER;
    public static final String BOOK_TABLE_INNER = JoinDistanceHelper.BOOK_TABLE_INNER;
    
    public static final KeywordMatchingType conjunction = KeywordMatchingType.CONJUNCTION_INDEXBASED;
    public static final KeywordMatchingType phrase = KeywordMatchingType.PHRASE_INDEXBASED;

    
    /*
     * The annotators @BeforeClass and @AfterClass are used instead of @Before and @After.
     * 
     * The difference is that:
     *   @Before and @After are executed before and after EACH test case.
     *   @BeforeClass and @AfterClass are executed once before ALL the test begin and ALL the test have finished.
     * 
     * We don't want to create and delete the tables on every test case, 
     *   therefore BeforeClass and AfterClass are better options.
     *   
     */
    @BeforeClass
    public static void setup() throws TextDBException {
        // writes the test tables before ALL tests
        JoinDistanceHelper.createTestTables();
    }
    
    @AfterClass
    public static void cleanUp() throws TextDBException {
        // deletes the test tables after ALL tests
        JoinDistanceHelper.deleteTestTables();
    }

}
