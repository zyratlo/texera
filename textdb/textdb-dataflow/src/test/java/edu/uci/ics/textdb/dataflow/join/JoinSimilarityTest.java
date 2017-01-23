package edu.uci.ics.textdb.dataflow.join;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;
import edu.uci.ics.textdb.dataflow.regexmatch.RegexMatcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import org.junit.Test;

import java.util.List;

public class JoinSimilarityTest {

    public static final String NEWS_TABLE_OUTER = JoinTestHelper.NEWS_TABLE_OUTER;
    public static final String NEWS_TABLE_INNER = JoinTestHelper.NEWS_TABLE_INNER;
    
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
        JoinTestHelper.createTestTables();
    }
    
    @AfterClass
    public static void cleanUp() throws TextDBException {
        // deletes the test tables after ALL tests
        JoinTestHelper.deleteTestTables();
    }

    @After
    public void clear() throws TextDBException {
        JoinTestHelper.clearTestTables();
    }
    
    @Test
    public void test1() throws TextDBException {
        JoinTestHelper.insertToOuter(JoinTestConstants.getNewsTuples().get(0));
        JoinTestHelper.insertToInner(JoinTestConstants.getNewsTuples().get(1));

        RegexMatcher regexMatcherInner = JoinTestHelper.getRegexMatcher(JoinTestHelper.NEWS_TABLE_INNER,
                "D|donald\\sT|trump('s)?", JoinTestConstants.NEWS_BODY);
        RegexMatcher regexMatcherOuter = JoinTestHelper.getRegexMatcher(JoinTestHelper.NEWS_TABLE_OUTER,
                "D|donald\\sT|trump('s)?", JoinTestConstants.NEWS_BODY);

        SimilarityJoinPredicate similarityJoinPredicate = new SimilarityJoinPredicate(JoinTestConstants.NEWS_BODY, 0.5);
        List<ITuple> results = JoinTestHelper.getJoinDistanceResults(
                regexMatcherOuter, regexMatcherInner, similarityJoinPredicate, 0, Integer.MAX_VALUE);

        System.out.print(Utils.getTupleListString(results));

    }
    

}
