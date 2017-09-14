package edu.uci.ics.textdb.exp.plangen;

import org.junit.Test;

import edu.uci.ics.textdb.api.utils.TestUtils;

public class LogicalPlanJsonSerializationTest {
    
    @Test
    public void testLogicalPlan() throws Exception {
        LogicalPlan logicalPlan = LogicalPlanTest.getLogicalPlan1();
        TestUtils.testJsonSerialization(logicalPlan);
    }
    
    @Test
    public void testOperatorLink() throws Exception {
        OperatorLink operatorLink = new OperatorLink("origin", "destination");
        TestUtils.testJsonSerialization(operatorLink);
    }

}
