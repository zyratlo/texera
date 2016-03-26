package edu.uci.ics.textdb.dataflow.regexmatch;

import edu.uci.ics.textdb.api.common.IDocument;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;

/**
 * Created by chenli on 3/25/16.
 */
public class RegexMatcherTest {

    @Test
    public void testConstructionWillSucceed() throws Exception {
        RegexMatcher matcher = new RegexMatcher(new ArrayList<IDocument>(),"regex*", null);
    }

}