package edu.uci.ics.textdb.api.plan;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

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
            public void processTuples() throws Exception {
                // TODO Auto-generated method stub

            }


            @Override
            public void open() throws Exception {
                // TODO Auto-generated method stub

            }


            @Override
            public void close() throws Exception {
                // TODO Auto-generated method stub

            }
        };
    }
}
