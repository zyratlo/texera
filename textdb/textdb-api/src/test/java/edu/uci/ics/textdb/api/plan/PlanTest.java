package edu.uci.ics.textdb.api.plan;

import edu.uci.ics.textdb.api.exception.TextDBException;
import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.ISink;

public class PlanTest {
    private Plan plan;
    private ISink root;

    @Before
    public void setUp() {
        root = getSampleSink();
        plan = new Plan(root);
    }

    @Test
    public void testGetRoot() {
        ISink rootReturned = plan.getRoot();
        Assert.assertSame(root, rootReturned);
    }

    private ISink getSampleSink() {
        return new ISink() {

            @Override
            public void processTuples() throws TextDBException {
                // TODO Auto-generated method stub

            }
            
            public void processTuples(boolean multiple) throws TextDBException {
                // TODO Auto-generated method stub

            }

            @Override
            public void open() throws TextDBException {
                // TODO Auto-generated method stub

            }

            @Override
            public void close() throws TextDBException {
                // TODO Auto-generated method stub

            }

            @Override
            public Schema getOutputSchema() {
                // TODO Auto-generated method stub
                return null;
            }
        };
    }
}
