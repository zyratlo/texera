package edu.uci.ics.textdb.dataflow.queryrewriter;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by kishorenarendran on 25/04/16.
 * Testing QueryRewriter
 * This test class tests the functionality of the QueryRewrite operator
 * this is done by feeding the class with different kinds of strings to
 * try the query rewriting.
 *
 * This includes query strings like - "newyorkcity", "horseshoe" and other
 * strings which stem from misspelled, missing spaces in search queries
 */
public class QueryRewriterTest {

    /**
     * Tests the QueryRewriter operator with the string "horseshoe"
     * @throws Exception
     */
    @Test
    public void testHorseShoeString() throws Exception {

        String query = "horseshoe";
        List<String> expectedRewrittenStrings = Arrays.asList("hor se shoe", "hors es hoe", "horse shoe", "horses hoe","horseshoe");

        boolean isSame = queryRewriterTestBoilerplate(query, expectedRewrittenStrings);

        Assert.assertTrue(isSame);
    }

    /**
     * Tests the QueryRewriter operator with the string "horse shoe"
     * @throws Exception
     */
    @Test
    public void testHorseSpaceShoeString() throws Exception {

        String query = "horse shoe";
        List<String> expectedRewrittenStrings = Arrays.asList("hor se shoe", "horse shoe");

        boolean isSame = queryRewriterTestBoilerplate(query, expectedRewrittenStrings);

        Assert.assertTrue(isSame);
    }

    /**
     * Tests the QueryRewriter operator with the string "newyork city"
     * @throws Exception
     */
    @Test
    public void testNewYorkCityString() throws Exception {

        String query = "newyork city";
        List<String> expectedRewrittenStrings = Arrays.asList("new york city","newyork city");

        boolean isSame = queryRewriterTestBoilerplate(query, expectedRewrittenStrings);

        Assert.assertTrue(isSame);
    }

    public static boolean queryRewriterTestBoilerplate(String query, List<String> expectedRewrittenStrings) throws Exception {

        QueryRewriter queryRewriter = new QueryRewriter(query);
        queryRewriter.open();
        ITuple resultItuple = queryRewriter.getNextTuple();
        ArrayList<String> rewrittenStrings = new ArrayList<String>((List<String>)resultItuple.getField(QueryRewriter.QUERYLIST).getValue());
        queryRewriter.close();

        return TestUtils.containsAllResults(rewrittenStrings, new ArrayList<String>(expectedRewrittenStrings));
    }
}