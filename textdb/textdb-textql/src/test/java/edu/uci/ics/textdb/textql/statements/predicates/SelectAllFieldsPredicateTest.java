package edu.uci.ics.textdb.textql.statements.predicates;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.textdb.textql.planbuilder.beans.PassThroughBean;
import edu.uci.ics.textdb.web.request.beans.OperatorBean;

/**
 * This class contains test cases for the SelectAllFieldsPredicate class.
 * The getOperatorBean method is tested.
 * There are no constructors, getters nor setters to test. 
 * 
 * @author Flavio Bayer
 *
 */
public class SelectAllFieldsPredicateTest {

    /**
     * Test the getOperatorBean method.
     * Build a SelectAllFieldsPredicate, invoke the getOperatorBean and check
     * whether a PassThroughBean with the right attributes is returned.
     */
    @Test
    public void testGetOperatorBean() {
        SelectAllFieldsPredicate selectAllFieldsPredicate = new SelectAllFieldsPredicate();
        OperatorBean projectionBean;
        String operatorId;
        
        operatorId = "xxx";
        projectionBean = selectAllFieldsPredicate.getOperatorBean(operatorId);
        Assert.assertEquals(projectionBean, new PassThroughBean(operatorId, "PassThrough"));

        operatorId = "y0a9";
        projectionBean = selectAllFieldsPredicate.getOperatorBean(operatorId);
        Assert.assertEquals(projectionBean, new PassThroughBean(operatorId, "PassThrough"));
    }
    
}
