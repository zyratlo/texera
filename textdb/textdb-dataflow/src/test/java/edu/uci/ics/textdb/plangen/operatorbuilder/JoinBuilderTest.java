package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.textdb.dataflow.common.JoinDistancePredicate;
import edu.uci.ics.textdb.dataflow.join.Join;

public class JoinBuilderTest {
    
    @Test
    public void testJoinBuilder1() throws Exception {
        
        String joinPredicateType = JoinBuilder.JOIN_CHARACTER_DISTANCE;
        String attributeName = "content";
        String distanceStr = "10";
        
        HashMap<String, String> operatorProperties = new HashMap<>();
        
        operatorProperties.put(JoinBuilder.JOIN_PREDICATE, joinPredicateType);
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, attributeName);
        operatorProperties.put(JoinBuilder.JOIN_DISTANCE, distanceStr);
        
        Join join = JoinBuilder.buildOperator(operatorProperties);
        
        Assert.assertEquals(join.getPredicate().getInnerAttributeName(), attributeName);
        Assert.assertEquals(join.getPredicate().getOuterAttributeName(), attributeName);
        
        Assert.assertTrue(join.getPredicate() instanceof JoinDistancePredicate);
        JoinDistancePredicate joinDistancePredicate = (JoinDistancePredicate) join.getPredicate();
        Assert.assertEquals(joinDistancePredicate.getThreshold().toString(), distanceStr);
        
    }

}
