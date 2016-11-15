package edu.uci.ics.textdb.plangen.operatorbuilder;

import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.projection.ProjectionOperator;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Created by kishorenarendran on 11/14/16.
 */
public class ProjectionBuilderTest {

    @Test
    public void testProjectionBuilder() throws PlanGenException{

        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        operatorProperties.put(OperatorBuilderUtils.LIMIT, "10");
        operatorProperties.put(OperatorBuilderUtils.OFFSET, "6");
        List<String> expectedAttributeNamesList = Arrays.asList("city", "location", "content");

        ProjectionOperator projectionOperator = ProjectionBuilder.buildOperator(operatorProperties);
        List<String> attributeNamesList = projectionOperator.getPredicate().getProjectionFields();
        Assert.assertEquals(expectedAttributeNamesList.toString(), attributeNamesList.toString());
    }

    @Test(expected = PlanGenException.class)
    public void testInvalidProjectionBuilder() throws PlanGenException {
        HashMap<String, String> operatorProperties = new HashMap<>();
        ProjectionBuilder.buildOperator(operatorProperties);
    }
}
