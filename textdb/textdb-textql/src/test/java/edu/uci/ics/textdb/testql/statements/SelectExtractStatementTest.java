package edu.uci.ics.textdb.testql.statements;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.textdb.textql.statements.SelectExtractStatement;
import edu.uci.ics.textdb.textql.statements.predicates.ExtractPredicate;
import edu.uci.ics.textdb.textql.statements.predicates.KeywordExtractPredicate;
import edu.uci.ics.textdb.textql.statements.predicates.SelectAllFieldsPredicate;
import edu.uci.ics.textdb.textql.statements.predicates.SelectPredicate;
import edu.uci.ics.textdb.textql.statements.predicates.SelectSomeFieldsPredicate;
import edu.uci.ics.textdb.web.request.beans.KeywordMatcherBean;
import edu.uci.ics.textdb.web.request.beans.OperatorBean;
import edu.uci.ics.textdb.web.request.beans.ProjectionBean;

/**
 * This class contain test cases for the SelectExtractStatement.
 * The constructor, getter, setter and bean builder methods are tested.
 * 
 * @author Flavio Bayer
 *
 */
public class SelectExtractStatementTest {

    /**
     * Test the class constructor and the getter methods.
     */
    @Test
    public void testConstructorAndGetters() {
        SelectExtractStatement selectExtractStatement;

        // Tests for the id attribute
        selectExtractStatement = new SelectExtractStatement("idx", null, null, null, null, null);
        Assert.assertEquals(selectExtractStatement.getId(), "idx");

        // Tests for the selectPredicate attribute
        selectExtractStatement = new SelectExtractStatement("", null, null, null, null, null);
        Assert.assertEquals(selectExtractStatement.getSelectPredicate(), null);

        SelectPredicate selectPredicate00 = new SelectAllFieldsPredicate();
        selectExtractStatement = new SelectExtractStatement("", selectPredicate00, null, null, null, null);
        Assert.assertEquals(selectExtractStatement.getSelectPredicate(), selectPredicate00);

        SelectPredicate selectPredicate01 = new SelectSomeFieldsPredicate(Arrays.asList("a", "b", "c"));
        selectExtractStatement = new SelectExtractStatement("", selectPredicate01, null, null, null, null);
        Assert.assertEquals(selectExtractStatement.getSelectPredicate(), selectPredicate01);

        // Tests for the extractPredicate attribute
        selectExtractStatement = new SelectExtractStatement("", null, null, null, null, null);
        Assert.assertEquals(selectExtractStatement.getSelectPredicate(), null);

        ExtractPredicate extractPredicate00 = new KeywordExtractPredicate(Arrays.asList("a", "v"), "x", "substring");
        selectExtractStatement = new SelectExtractStatement("", null, extractPredicate00, null, null, null);
        Assert.assertEquals(selectExtractStatement.getExtractPredicate(), extractPredicate00);

        // Tests for the fromClause attribute
        selectExtractStatement = new SelectExtractStatement("", null, null, "t1", null, null);
        selectExtractStatement.setFromClause("t1");

        // Tests for the limitClause attribute
        selectExtractStatement = new SelectExtractStatement("", null, null, "t1", null, null);
        Assert.assertEquals(selectExtractStatement.getLimitClause(), null);

        selectExtractStatement = new SelectExtractStatement("", null, null, "t1", 1, null);
        Assert.assertEquals(selectExtractStatement.getLimitClause(), Integer.valueOf(1));

        // Tests for the offsetClause attribute
        selectExtractStatement = new SelectExtractStatement("", null, null, "t1", null, null);
        Assert.assertEquals(selectExtractStatement.getOffsetClause(), null);

        selectExtractStatement = new SelectExtractStatement("", null, null, "t1", null, 5);
        Assert.assertEquals(selectExtractStatement.getOffsetClause(), Integer.valueOf(5));
    }

    /**
     * Test the setter methods and the getter methods.
     */
    @Test
    public void testSettersAndGetters() {
        SelectExtractStatement selectExtractStatement = new SelectExtractStatement();

        // Tests for the id attribute
        selectExtractStatement.setId("idx");
        Assert.assertEquals(selectExtractStatement.getId(), "idx");

        // Tests for the selectPredicate attribute
        selectExtractStatement.setSelectPredicate(null);
        Assert.assertEquals(selectExtractStatement.getSelectPredicate(), null);

        SelectPredicate selectPredicate00 = new SelectAllFieldsPredicate();
        selectExtractStatement.setSelectPredicate(selectPredicate00);
        Assert.assertEquals(selectExtractStatement.getSelectPredicate(), selectPredicate00);

        SelectPredicate selectPredicate01 = new SelectSomeFieldsPredicate(Arrays.asList("a", "b", "c"));
        selectExtractStatement.setSelectPredicate(selectPredicate01);
        Assert.assertEquals(selectExtractStatement.getSelectPredicate(), selectPredicate01);

        // Tests for the extractPredicate attribute
        selectExtractStatement.setExtractPredicate(null);
        Assert.assertEquals(selectExtractStatement.getExtractPredicate(), null);

        ExtractPredicate extractPredicate00 = new KeywordExtractPredicate(Arrays.asList("a", "v"), "x", "substring");
        selectExtractStatement.setExtractPredicate(extractPredicate00);
        Assert.assertEquals(selectExtractStatement.getExtractPredicate(), extractPredicate00);

        // Tests for the fromClause attribute
        selectExtractStatement.setFromClause("t3");
        Assert.assertEquals(selectExtractStatement.getFromClause(), "t3");

        // Tests for the limitClause attribute
        selectExtractStatement.setLimitClause(null);
        Assert.assertEquals(selectExtractStatement.getLimitClause(), null);

        selectExtractStatement.setLimitClause(1);
        Assert.assertEquals(selectExtractStatement.getLimitClause(), Integer.valueOf(1));

        // Tests for the offsetClause attribute
        selectExtractStatement.setOffsetClause(null);
        Assert.assertEquals(selectExtractStatement.getOffsetClause(), null);

        selectExtractStatement.setOffsetClause(1);
        Assert.assertEquals(selectExtractStatement.getOffsetClause(), Integer.valueOf(1));
    }

    /**
     * Test the correctness of the generated beans by a SelectExtractStatement
     * without a SelectPredicate nor ExtractPredicate.
     */
    @Test
    public void testSelectExtractStatementBeansBuilder00() {
        SelectExtractStatement selectExtractStatement = new SelectExtractStatement("id", null, null, "from", null,
                null);

        List<OperatorBean> expectedGeneratedBeans = Collections.emptyList();
        List<String> dependencies = Arrays.asList("from");

        Assert.assertEquals(selectExtractStatement.getInputViews(), dependencies);
        StatementTestUtils.assertGeneratedBeans(selectExtractStatement, expectedGeneratedBeans);
    }

    /**
     * Test the correctness of the generated beans by a SelectExtractStatement
     * with a SelectAllFieldsPredicate.
     */
    @Test
    public void testSelectExtractStatementBeansBuilder01() {
        SelectPredicate selectPredicate = new SelectAllFieldsPredicate();
        SelectExtractStatement selectExtractStatement = new SelectExtractStatement("id", selectPredicate, null, "from",
                null, null);

        List<OperatorBean> expectedGeneratedBeans = Collections.emptyList();
        List<String> dependencies = Arrays.asList("from");

        Assert.assertEquals(selectExtractStatement.getInputViews(), dependencies);
        StatementTestUtils.assertGeneratedBeans(selectExtractStatement, expectedGeneratedBeans);
    }

    /**
     * Test the correctness of the generated beans by a SelectExtractStatement
     * with a SelectSomeFieldsPredicate.
     */
    @Test
    public void testSelectExtractStatementBeansBuilder02() {
        SelectPredicate selectPredicate = new SelectSomeFieldsPredicate(Arrays.asList("a", "b"));
        SelectExtractStatement selectExtractStatement = new SelectExtractStatement("id", selectPredicate, null, "from",
                null, null);

        List<OperatorBean> expectedGeneratedBeans = Arrays
                .asList(new ProjectionBean("", "Projection", "a,b", null, null));
        List<String> dependencies = Arrays.asList("from");

        Assert.assertEquals(selectExtractStatement.getInputViews(), dependencies);
        StatementTestUtils.assertGeneratedBeans(selectExtractStatement, expectedGeneratedBeans);
    }

    /**
     * Test the correctness of the generated beans by a SelectExtractStatement
     * with a KeywordExtractPredicate.
     */
    @Test
    public void testSelectExtractStatementBeansBuilder03() {
        ExtractPredicate extractPredicate = new KeywordExtractPredicate(Arrays.asList("a", "b"), "x", "conjunction");
        SelectExtractStatement selectExtractStatement = new SelectExtractStatement("id", null, extractPredicate, "from",
                null, null);

        List<OperatorBean> expectedGeneratedBeans = Arrays
                .asList(new KeywordMatcherBean("", "KeywordMatcher", "a,b", null, null, "x", "conjunction"));
        List<String> dependencies = Arrays.asList("from");

        Assert.assertEquals(selectExtractStatement.getInputViews(), dependencies);
        StatementTestUtils.assertGeneratedBeans(selectExtractStatement, expectedGeneratedBeans);
    }

    /**
     * Test the correctness of the generated beans by a SelectExtractStatement
     * with a SelectAllFieldsPredicate and a KeywordExtractPredicate.
     */
    @Test
    public void testSelectExtractStatementBeansBuilder04() {
        SelectPredicate selectPredicate = new SelectAllFieldsPredicate();
        ExtractPredicate extractPredicate = new KeywordExtractPredicate(Arrays.asList("a", "b"), "x", "conjunction");
        SelectExtractStatement selectExtractStatement = new SelectExtractStatement("id", selectPredicate,
                extractPredicate, "from", null, null);

        List<OperatorBean> expectedGeneratedBeans = Arrays
                .asList(new KeywordMatcherBean("", "KeywordMatcher", "a,b", null, null, "x", "conjunction"));
        List<String> dependencies = Arrays.asList("from");

        Assert.assertEquals(selectExtractStatement.getInputViews(), dependencies);
        StatementTestUtils.assertGeneratedBeans(selectExtractStatement, expectedGeneratedBeans);
    }

    /**
     * Test the correctness of the generated beans by a SelectExtractStatement
     * with a SelectAllFieldsPredicate and a KeywordExtractPredicate.
     */
    @Test
    public void testSelectExtractStatementBeansBuilder05() {
        SelectPredicate selectPredicate = new SelectSomeFieldsPredicate(Arrays.asList("a", "b"));
        ExtractPredicate extractPredicate = new KeywordExtractPredicate(Arrays.asList("a", "b"), "x", "conjunction");
        SelectExtractStatement selectExtractStatement = new SelectExtractStatement("id", selectPredicate,
                extractPredicate, "from", null, null);

        List<OperatorBean> expectedGeneratedBeans = Arrays.asList(
                new KeywordMatcherBean("", "KeywordMatcher", "a,b", null, null, "x", "conjunction"),
                new ProjectionBean("", "Projection", "a,b", null, null));
        List<String> dependencies = Arrays.asList("from");

        Assert.assertEquals(selectExtractStatement.getInputViews(), dependencies);
        StatementTestUtils.assertGeneratedBeans(selectExtractStatement, expectedGeneratedBeans);
    }
}
