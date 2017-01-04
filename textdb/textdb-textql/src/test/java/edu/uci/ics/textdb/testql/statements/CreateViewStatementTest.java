package edu.uci.ics.textdb.testql.statements;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.textdb.textql.statements.CreateViewStatement;
import edu.uci.ics.textdb.textql.statements.SelectExtractStatement;
import edu.uci.ics.textdb.textql.statements.Statement;
import edu.uci.ics.textdb.textql.statements.predicates.ExtractPredicate;
import edu.uci.ics.textdb.textql.statements.predicates.KeywordExtractPredicate;
import edu.uci.ics.textdb.textql.statements.predicates.SelectPredicate;
import edu.uci.ics.textdb.textql.statements.predicates.SelectSomeFieldsPredicate;
import edu.uci.ics.textdb.web.request.beans.OperatorBean;

/**
 * This class contain test cases for the CreateViewStatement. The constructor,
 * getter, setter and bean builder methods are tested.
 * 
 * @author Flavio Bayer
 *
 */
public class CreateViewStatementTest {

    /**
     * Test the class constructor and the getter methods.
     */
    @Test
    public void testConstructorAndGetters() {
        Statement subStatement;
        CreateViewStatement createViewStatement;

        // Tests for the id attribute
        createViewStatement = new CreateViewStatement("idx", null);
        Assert.assertEquals(createViewStatement.getId(), "idx");

        // Tests for the subStatement attribute
        createViewStatement = new CreateViewStatement("idx", null);
        Assert.assertEquals(createViewStatement.getSubStatement(), null);

        SelectPredicate selectPredicate = new SelectSomeFieldsPredicate(Arrays.asList("a", "b"));
        ExtractPredicate extractPredicate = new KeywordExtractPredicate(Arrays.asList("a", "b"), "x", "conjunction");
        subStatement = new SelectExtractStatement("id", selectPredicate, extractPredicate, "from", null, null);
        createViewStatement = new CreateViewStatement("idx", subStatement);
        Assert.assertEquals(createViewStatement.getSubStatement(), subStatement);
    }

    /**
     * Test the setter methods and the getter methods.
     */
    @Test
    public void testGettersAndGetters() {
        Statement subStatement;
        CreateViewStatement createViewStatement = new CreateViewStatement(null, null);

        // Tests for the id attribute
        createViewStatement.setId("idx");
        Assert.assertEquals(createViewStatement.getId(), "idx");

        // Tests for the subStatement attribute
        createViewStatement.setSubStatement(null);
        Assert.assertEquals(createViewStatement.getSubStatement(), null);

        SelectPredicate selectPredicate = new SelectSomeFieldsPredicate(Arrays.asList("a", "b"));
        ExtractPredicate extractPredicate = new KeywordExtractPredicate(Arrays.asList("a", "b"), "x", "conjunction");
        subStatement = new SelectExtractStatement("id", selectPredicate, extractPredicate, "from", null, null);
        createViewStatement.setSubStatement(subStatement);
        Assert.assertEquals(createViewStatement.getSubStatement(), subStatement);
    }

    /**
     * Test the correctness of the generated beans by a CreateViewStatement with
     * a SelectExtractStatement as sub-statement without a SelectPredicate nor
     * ExtractPredicate.
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
     */
    @Test
    public void testCreateViewStatementBeansBuilder01() {
        SelectPredicate selectPredicate = new SelectSomeFieldsPredicate(Arrays.asList("a", "b"));
        ExtractPredicate extractPredicate = new KeywordExtractPredicate(Arrays.asList("a", "b"), "x", "conjunction");
        Statement subStatement = new SelectExtractStatement("id", selectPredicate, extractPredicate, "from", null,
                null);
        CreateViewStatement createViewStatement = new CreateViewStatement("idx", subStatement);

        List<OperatorBean> expectedGeneratedBeans = Collections.emptyList();
        List<String> dependencies = Arrays.asList(subStatement.getId());

        Assert.assertEquals(createViewStatement.getInputViews(), dependencies);
        StatementTestUtils.assertGeneratedBeans(createViewStatement, expectedGeneratedBeans);
    }
}
