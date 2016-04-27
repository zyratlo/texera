package edu.uci.ics.textdb.dataflow.queryrewriter;

import edu.uci.ics.textdb.api.common.ITuple;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by kishorenarendran on 25/04/16.
 * Testing QueryRewriter
 */
public class QueryRewriterTest {

    @Test
    public void testHorseShoeString() throws Exception {
        String query = "horseshoe";

        QueryRewriter queryRewriter = new QueryRewriter(query);
        queryRewriter.open();
        ITuple iTuple = queryRewriter.getNextTuple();
        queryRewriter.close();

        Assert.assertNotNull(iTuple);

        List<String> fuzzySet = (List<String>) iTuple.getField(0).getValue();
        Assert.assertNotNull(fuzzySet);

        List<String> correctSet = Arrays.asList("hor se shoe",
                "hors es hoe",
                "horse shoe",
                "horses hoe",
                "horseshoe");
        Collections.sort(correctSet);
        Collections.sort(fuzzySet);

        //After the QueryRewriter and the FuzzyTokenizer classes are completed

        //Assert.assertEquals(correctSet.size(), fuzzySet.size());
        //Assert.assertEquals(correctSet, fuzzySet);
    }

    @Test
    public void testHorseSpaceShoeString() throws Exception {
        String query = "horse shoe";

        QueryRewriter queryRewriter = new QueryRewriter(query);
        queryRewriter.open();
        ITuple iTuple = queryRewriter.getNextTuple();
        queryRewriter.close();

        Assert.assertNotNull(iTuple);

        List<String> fuzzySet = (List<String>) iTuple.getField(0).getValue();
        Assert.assertNotNull(fuzzySet);

        List<String> correctSet = Arrays.asList("horse shoe");

        Collections.sort(correctSet);
        Collections.sort(fuzzySet);

        //After the QueryRewriter and the FuzzyTokenizer classes are completed

        //Assert.assertEquals(correctSet.size(), fuzzySet.size());
        //Assert.assertEquals(correctSet, fuzzySet);
    }

    @Test
    public void testNewYorkCityString() throws Exception {
        String query = "newyork city";

        QueryRewriter queryRewriter = new QueryRewriter(query);
        queryRewriter.open();
        ITuple iTuple = queryRewriter.getNextTuple();
        queryRewriter.close();

        Assert.assertNotNull(iTuple);

        List<String> fuzzySet = (List<String>) iTuple.getField(0).getValue();
        Assert.assertNotNull(fuzzySet);

        List<String> correctSet = Arrays.asList("new york city",
                "newyork city");
        Collections.sort(correctSet);
        Collections.sort(fuzzySet);

        //After the QueryRewriter and the FuzzyTokenizer classes are completed

        //Assert.assertEquals(correctSet.size(), fuzzySet.size());
        //Assert.assertEquals(correctSet, fuzzySet);
    }
}
