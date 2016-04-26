package edu.uci.ics.textdb.dataflow.queryrewriter;

import edu.uci.ics.textdb.api.common.ITuple;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Arrays;
import java.util.Collections;

/**
 * Created by kishorenarendran on 25/04/16.
 */
public class QueryRewriterTest {

    @Test
    public void testHORSESHOE() throws Exception {
        String query = "horseshoe";

        QueryRewriter qr = new QueryRewriter(query);
        ITuple result = qr.getNextTuple();
        qr.close();

        Assert.assertNotNull(result);

        List<String> fuzzySet = (List<String>) result.getField(0);
        Assert.assertNotNull(fuzzySet);

        List<String> correctSet = Arrays.asList("hor se shoe",
                "hors es hoe",
                "horse shoe",
                "horses hoe",
                "horseshoe");
        Collections.sort(correctSet);
        Collections.sort(fuzzySet);

        Assert.assertEquals(correctSet.size(), fuzzySet.size());
        Assert.assertEquals(correctSet, fuzzySet);
    }

    @Test
    public void testNEWYORK_CITY() throws Exception {
        String query = "newyork city";

        QueryRewriter qr = new QueryRewriter(query);
        ITuple result = qr.getNextTuple();
        qr.close();

        Assert.assertNotNull(result);

        List<String> fuzzySet = (List<String>) result.getField(0);
        Assert.assertNotNull(fuzzySet);

        List<String> correctSet = Arrays.asList("new york city",
                "newyork city");
        Collections.sort(correctSet);
        Collections.sort(fuzzySet);

        Assert.assertEquals(correctSet.size(), fuzzySet.size());
        Assert.assertEquals(correctSet, fuzzySet);
    }
}
