package edu.uci.ics.textdb.textql.statements;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.textql.statements.CreateViewStatement;
import edu.uci.ics.textdb.textql.statements.SelectExtractStatement;
import edu.uci.ics.textdb.textql.statements.Statement;
import edu.uci.ics.textdb.textql.statements.predicates.ExtractPredicate;
import edu.uci.ics.textdb.textql.statements.predicates.KeywordExtractPredicate;
import edu.uci.ics.textdb.textql.statements.predicates.SelectAllFieldsPredicate;
import edu.uci.ics.textdb.textql.statements.predicates.SelectPredicate;
import edu.uci.ics.textdb.textql.statements.predicates.SelectSomeFieldsPredicate;
import edu.uci.ics.textdb.web.request.beans.OperatorBean;

/**
 * This class contains test cases for the CreateViewStatement.
 * The constructor, getter, setter and bean builder methods are tested.
 * 
 * @author Flavio Bayer
 *
 */
public class CreateViewStatementTest {

    /**
     * Test the class constructor and the getter methods.
     * Call the constructor of CreateViewStatement and test if
     * the returned value by the getter is the same.
     */
    @Test
    public void testConstructorAndGetters() {
        Statement subStatement;
        SelectPredicate selectPredicate;
        ExtractPredicate extractPredicate;
        CreateViewStatement createViewStatement;

        // Tests for the id attribute
        createViewStatement = new CreateViewStatement(null, null);
        Assert.assertEquals(createViewStatement.getId(), null);

        createViewStatement = new CreateViewStatement("statementId", null);
        Assert.assertEquals(createViewStatement.getId(), "statementId");

        createViewStatement = new CreateViewStatement("id6", null);
        Assert.assertEquals(createViewStatement.getId(), "id6");
        
        createViewStatement = new CreateViewStatement("_sid12", null);
        Assert.assertEquals(createViewStatement.getId(), "_sid12");

        // Tests for the subStatement attribute
        createViewStatement = new CreateViewStatement("statementId", null);
        Assert.assertEquals(createViewStatement.getSubStatement(), null);

        subStatement = new SelectExtractStatement("substatementId0", null, null, "source", null, null);
        createViewStatement = new CreateViewStatement("statementId", subStatement);
        Assert.assertEquals(createViewStatement.getSubStatement(), subStatement);

        subStatement = new SelectExtractStatement("substatementId1", null, null, "table", null, null);
        createViewStatement = new CreateViewStatement("statementId", subStatement);
        Assert.assertEquals(createViewStatement.getSubStatement(), subStatement);
        
        selectPredicate = new SelectAllFieldsPredicate();
        subStatement = new SelectExtractStatement("substatementId2", selectPredicate, null, "from", null, null);
        createViewStatement = new CreateViewStatement("statementId", subStatement);
        Assert.assertEquals(createViewStatement.getSubStatement(), subStatement);
        
        selectPredicate = new SelectSomeFieldsPredicate(Arrays.asList("at1", "at0"));
        extractPredicate = new KeywordExtractPredicate(Arrays.asList("a", "b"), "keyword",
                KeywordMatchingType.CONJUNCTION_INDEXBASED.toString());
        subStatement = new SelectExtractStatement("substatementIdX", selectPredicate, extractPredicate, "t", null, null);
        createViewStatement = new CreateViewStatement("statementId", subStatement);
        Assert.assertEquals(createViewStatement.getSubStatement(), subStatement);
    }

    /**
     * Test the setter methods and the getter methods.
     * Call the setter of CreateViewStatement and test if the
     * returned value by the getter is the same.
     */
    @Test
    public void testGettersAndGetters() {
        Statement subStatement;
        SelectPredicate selectPredicate;
        ExtractPredicate extractPredicate;
        CreateViewStatement createViewStatement = new CreateViewStatement(null, null);

        // Tests for the id attribute
        createViewStatement.setId(null);
        Assert.assertEquals(createViewStatement.getId(), null);

        createViewStatement.setId("statementId4");
        Assert.assertEquals(createViewStatement.getId(), "statementId4");
        
        createViewStatement.setId("_sid0");
        Assert.assertEquals(createViewStatement.getId(), "_sid0");

        // Tests for the subStatement attribute
        createViewStatement.setSubStatement(null);
        Assert.assertEquals(createViewStatement.getSubStatement(), null);
        
        selectPredicate = new SelectAllFieldsPredicate();
        subStatement = new SelectExtractStatement("substatementId0", selectPredicate, null, "from", null, null);
        createViewStatement = new CreateViewStatement("statementId", subStatement);
        Assert.assertEquals(createViewStatement.getSubStatement(), subStatement);
        
        selectPredicate = new SelectAllFieldsPredicate();
        subStatement = new SelectExtractStatement("substatementId1", selectPredicate, null, "table", null, null);
        createViewStatement = new CreateViewStatement("statementId", subStatement);
        Assert.assertEquals(createViewStatement.getSubStatement(), subStatement);

        selectPredicate = new SelectSomeFieldsPredicate(Arrays.asList("c", "d"));
        extractPredicate = new KeywordExtractPredicate(Arrays.asList("f0", "f1"), "xxx",
                KeywordMatchingType.PHRASE_INDEXBASED.toString());
        subStatement = new SelectExtractStatement("id", selectPredicate, extractPredicate, "source", null, null);
        createViewStatement.setSubStatement(subStatement);
        Assert.assertEquals(createViewStatement.getSubStatement(), subStatement);
    }

    /**
     * Test the correctness of the generated beans by a CreateViewStatement with
     * a SelectExtractStatement as sub-statement without a SelectPredicate nor
     * ExtractPredicate.
     * Get a graph by calling getInternalOperatorBeans() and getInternalLinkBeans()
     * methods and check if the generated path form the node getInputNodeID() to 
     * the node getOutputNodeID() is correct. Also check whether getInputViews()
     * is returning the correct dependencies.  
     */
    @Test
    public void testCreateViewStatementBeansBuilder00() {
        Statement subStatement = new SelectExtractStatement("id2", null, null, "from", null, null);
        CreateViewStatement createViewStatement = new CreateViewStatement("idx", subStatement);

        List<OperatorBean> expectedGeneratedBeans = Collections.emptyList();
        List<String> dependencies = Arrays.asList(subStatement.getId());

        Assert.assertEquals(createViewStatement.getInputViews(), dependencies);
        StatementTestUtils.assertGeneratedBeans(createViewStatement, expectedGeneratedBeans);
    }

    /**
     * Test the correctness of the generated beans by a CreateViewStatement with
     * a SelectExtractStatement as sub-statement with a
     * SelectSomeFieldsPredicate and a KeywordExtractPredicate.
     * Get a graph by calling getInternalOperatorBeans() and getInternalLinkBeans()
     * methods and check if the generated path form the node getInputNodeID() to 
     * the node getOutputNodeID() is correct. Also check whether getInputViews()
     * is returning the correct dependencies.  
     */
    @Test
    public void testCreateViewStatementBeansBuilder01() {
        SelectPredicate selectPredicate = new SelectSomeFieldsPredicate(Arrays.asList("x", "y"));
        ExtractPredicate extractPredicate = new KeywordExtractPredicate(Arrays.asList("a", "b"), "zzz",
                KeywordMatchingType.SUBSTRING_SCANBASED.toString());
        Statement subStatement = new SelectExtractStatement("id", selectPredicate, extractPredicate, "from", null,
                null);
        CreateViewStatement createViewStatement = new CreateViewStatement("idx", subStatement);

        List<OperatorBean> expectedGeneratedBeans = Collections.emptyList();
        List<String> dependencies = Arrays.asList(subStatement.getId());

        Assert.assertEquals(createViewStatement.getInputViews(), dependencies);
        StatementTestUtils.assertGeneratedBeans(createViewStatement, expectedGeneratedBeans);
    }
}
