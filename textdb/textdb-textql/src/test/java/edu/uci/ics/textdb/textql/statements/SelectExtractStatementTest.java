package edu.uci.ics.textdb.textql.statements;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
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
 * This class contains test cases for the SelectExtractStatement.
 * The constructor, getter, setter and bean builder methods are tested.
 * 
 * @author Flavio Bayer
 *
 */
public class SelectExtractStatementTest {

    /**
     * Test the class constructor and the getter methods.
     * Call the constructor of CreateViewStatement and test if the
     * returned value by the getter is the same.
     */
    @Test
    public void testConstructorAndGetters() {
        SelectPredicate selectPredicate;
        ExtractPredicate extractPredicate;
        SelectExtractStatement selectExtractStatement;

        // Tests for the id attribute
        selectExtractStatement = new SelectExtractStatement(null, null, null, null, null, null);
        Assert.assertEquals(selectExtractStatement.getId(), null);

        selectExtractStatement = new SelectExtractStatement("idx", null, null, null, null, null);
        Assert.assertEquals(selectExtractStatement.getId(), "idx");
        
        selectExtractStatement = new SelectExtractStatement("_sid08", null, null, null, null, null);
        Assert.assertEquals(selectExtractStatement.getId(), "_sid08");

        // Tests for the selectPredicate attribute
        selectExtractStatement = new SelectExtractStatement("", null, null, null, null, null);
        Assert.assertEquals(selectExtractStatement.getSelectPredicate(), null);

        selectPredicate = new SelectAllFieldsPredicate();
        selectExtractStatement = new SelectExtractStatement(null, selectPredicate, null, null, null, null);
        Assert.assertEquals(selectExtractStatement.getSelectPredicate(), selectPredicate);

        selectPredicate = new SelectSomeFieldsPredicate(Arrays.asList("a", "b", "c"));
        selectExtractStatement = new SelectExtractStatement(null, selectPredicate, null, null, null, null);
        Assert.assertEquals(selectExtractStatement.getSelectPredicate(), selectPredicate);

        selectPredicate = new SelectSomeFieldsPredicate(Arrays.asList("f0", "f1"));
        selectExtractStatement = new SelectExtractStatement(null, selectPredicate, null, null, null, null);
        Assert.assertEquals(selectExtractStatement.getSelectPredicate(), selectPredicate);
        
        // Tests for the extractPredicate attribute
        selectExtractStatement = new SelectExtractStatement(null, null, null, null, null, null);
        Assert.assertEquals(selectExtractStatement.getSelectPredicate(), null);
        
        extractPredicate = new KeywordExtractPredicate(Arrays.asList("x", "y"), "keyword",
                KeywordMatchingType.SUBSTRING_SCANBASED.toString());
        selectExtractStatement = new SelectExtractStatement(null, null, extractPredicate, null, null, null);
        Assert.assertEquals(selectExtractStatement.getExtractPredicate(), extractPredicate);

        extractPredicate = new KeywordExtractPredicate(Arrays.asList("z6", "t4"), "xyz",
                KeywordMatchingType.PHRASE_INDEXBASED.toString());
        selectExtractStatement = new SelectExtractStatement(null, null, extractPredicate, null, null, null);
        Assert.assertEquals(selectExtractStatement.getExtractPredicate(), extractPredicate);

        // Tests for the fromClause attribute
        selectExtractStatement = new SelectExtractStatement(null, null, null, null, null, null);
        selectExtractStatement.setFromClause(null);

        selectExtractStatement = new SelectExtractStatement(null, null, null, "tab", null, null);
        selectExtractStatement.setFromClause("tab");

        selectExtractStatement = new SelectExtractStatement(null, null, null, "t1", null, null);
        selectExtractStatement.setFromClause("t1");

        // Tests for the limitClause attribute
        selectExtractStatement = new SelectExtractStatement(null, null, null, null, null, null);
        Assert.assertEquals(selectExtractStatement.getLimitClause(), null);

        selectExtractStatement = new SelectExtractStatement(null, null, null, null, 0, null);
        Assert.assertEquals(selectExtractStatement.getLimitClause(), Integer.valueOf(0));

        selectExtractStatement = new SelectExtractStatement(null, null, null, null, 8, null);
        Assert.assertEquals(selectExtractStatement.getLimitClause(), Integer.valueOf(8));
        
        selectExtractStatement = new SelectExtractStatement(null, null, null, null, -151, null);
        Assert.assertEquals(selectExtractStatement.getLimitClause(), Integer.valueOf(-151));

        // Tests for the offsetClause attribute
        selectExtractStatement = new SelectExtractStatement(null, null, null, null, null, null);
        Assert.assertEquals(selectExtractStatement.getOffsetClause(), null);
        
        selectExtractStatement = new SelectExtractStatement(null, null, null, null, null, 0);
        Assert.assertEquals(selectExtractStatement.getOffsetClause(), Integer.valueOf(0));

        selectExtractStatement = new SelectExtractStatement(null, null, null, null, null, 562);
        Assert.assertEquals(selectExtractStatement.getOffsetClause(), Integer.valueOf(562));
        
        selectExtractStatement = new SelectExtractStatement(null, null, null, null, null, -98);
        Assert.assertEquals(selectExtractStatement.getOffsetClause(), Integer.valueOf(-98));
    }

    /**
     * Test the setter methods and the getter methods.
     * Call the setter of CreateViewStatement and test if the returned
     * value by the getter is the same.
     */
    @Test
    public void testSettersAndGetters() {
        SelectPredicate selectPredicate;
        ExtractPredicate extractPredicate;
        SelectExtractStatement selectExtractStatement = new SelectExtractStatement();

        // Tests for the id attribute
        selectExtractStatement.setId(null);
        Assert.assertEquals(selectExtractStatement.getId(), null);
        
        selectExtractStatement.setId("idx");
        Assert.assertEquals(selectExtractStatement.getId(), "idx");
        
        selectExtractStatement.setId("_sid9");
        Assert.assertEquals(selectExtractStatement.getId(), "_sid9");

        // Tests for the selectPredicate attribute
        selectExtractStatement.setSelectPredicate(null);
        Assert.assertEquals(selectExtractStatement.getSelectPredicate(), null);

        selectPredicate = new SelectAllFieldsPredicate();
        selectExtractStatement.setSelectPredicate(selectPredicate);
        Assert.assertEquals(selectExtractStatement.getSelectPredicate(), selectPredicate);

        selectPredicate = new SelectSomeFieldsPredicate(Arrays.asList("a", "b", "c"));
        selectExtractStatement.setSelectPredicate(selectPredicate);
        Assert.assertEquals(selectExtractStatement.getSelectPredicate(), selectPredicate);

        selectPredicate = new SelectSomeFieldsPredicate(Arrays.asList("f0", "f1"));
        selectExtractStatement.setSelectPredicate(selectPredicate);
        Assert.assertEquals(selectExtractStatement.getSelectPredicate(), selectPredicate);

        // Tests for the extractPredicate attribute
        selectExtractStatement.setExtractPredicate(null);
        Assert.assertEquals(selectExtractStatement.getExtractPredicate(), null);

        extractPredicate = new KeywordExtractPredicate(Arrays.asList("a", "c"), "search",
                KeywordMatchingType.PHRASE_INDEXBASED.toString());
        selectExtractStatement.setExtractPredicate(extractPredicate);
        Assert.assertEquals(selectExtractStatement.getExtractPredicate(), extractPredicate);
        
        extractPredicate = new KeywordExtractPredicate(Arrays.asList("u", "v"), "news",
                KeywordMatchingType.CONJUNCTION_INDEXBASED.toString());
        selectExtractStatement.setExtractPredicate(extractPredicate);
        Assert.assertEquals(selectExtractStatement.getExtractPredicate(), extractPredicate);

        // Tests for the fromClause attribute
        selectExtractStatement.setFromClause(null);
        Assert.assertEquals(selectExtractStatement.getFromClause(), null);
        
        selectExtractStatement.setFromClause("table");
        Assert.assertEquals(selectExtractStatement.getFromClause(), "table");
        
        selectExtractStatement.setFromClause("t3");
        Assert.assertEquals(selectExtractStatement.getFromClause(), "t3");

        // Tests for the limitClause attribute
        selectExtractStatement.setLimitClause(null);
        Assert.assertEquals(selectExtractStatement.getLimitClause(), null);

        selectExtractStatement.setLimitClause(0);
        Assert.assertEquals(selectExtractStatement.getLimitClause(), Integer.valueOf(0));

        selectExtractStatement.setLimitClause(5);
        Assert.assertEquals(selectExtractStatement.getLimitClause(), Integer.valueOf(5));
        
        selectExtractStatement.setLimitClause(-7);
        Assert.assertEquals(selectExtractStatement.getLimitClause(), Integer.valueOf(-7));

        // Tests for the offsetClause attribute
        selectExtractStatement.setOffsetClause(null);
        Assert.assertEquals(selectExtractStatement.getOffsetClause(), null);

        selectExtractStatement.setOffsetClause(0);
        Assert.assertEquals(selectExtractStatement.getOffsetClause(), Integer.valueOf(0));

        selectExtractStatement.setOffsetClause(-3);
        Assert.assertEquals(selectExtractStatement.getOffsetClause(), Integer.valueOf(-3));

        selectExtractStatement.setOffsetClause(58);
        Assert.assertEquals(selectExtractStatement.getOffsetClause(), Integer.valueOf(58));
    }

    /**
     * Test the correctness of the generated beans by a SelectExtractStatement
     * without a SelectPredicate nor ExtractPredicate.
     * Get a graph by calling getInternalOperatorBeans() and getInternalLinkBeans()
     * methods and check if the generated path form the node getInputNodeID() to 
     * the node getOutputNodeID() is correct. Also check whether getInputViews()
     * is returning the correct dependencies.  
     */
    @Test
    public void testSelectExtractStatementBeansBuilder00() {
        SelectExtractStatement selectExtractStatement = new SelectExtractStatement("id", null, null, "tableX", null,
                null);

        List<OperatorBean> expectedGeneratedBeans = Collections.emptyList();
        List<String> dependencies = Arrays.asList("tableX");

        Assert.assertEquals(selectExtractStatement.getInputViews(), dependencies);
        StatementTestUtils.assertGeneratedBeans(selectExtractStatement, expectedGeneratedBeans);
    }

    /**
     * Test the correctness of the generated beans by a SelectExtractStatement
     * with a SelectAllFieldsPredicate.
     * Get a graph by calling getInternalOperatorBeans() and getInternalLinkBeans()
     * methods and check if the generated path form the node getInputNodeID() to 
     * the node getOutputNodeID() is correct. Also check whether getInputViews()
     * is returning the correct dependencies.  
     */
    @Test
    public void testSelectExtractStatementBeansBuilder01() {
        SelectPredicate selectPredicate = new SelectAllFieldsPredicate();
        SelectExtractStatement selectExtractStatement = new SelectExtractStatement("id", selectPredicate, null, "Table",
                null, null);

        List<OperatorBean> expectedGeneratedBeans = Collections.emptyList();
        List<String> dependencies = Arrays.asList("Table");

        Assert.assertEquals(selectExtractStatement.getInputViews(), dependencies);
        StatementTestUtils.assertGeneratedBeans(selectExtractStatement, expectedGeneratedBeans);
    }

    /**
     * Test the correctness of the generated beans by a SelectExtractStatement
     * with a SelectSomeFieldsPredicate.
     * Get a graph by calling getInternalOperatorBeans() and getInternalLinkBeans()
     * methods and check if the generated path form the node getInputNodeID() to 
     * the node getOutputNodeID() is correct. Also check whether getInputViews()
     * is returning the correct dependencies.  
     */
    @Test
    public void testSelectExtractStatementBeansBuilder02() {
        SelectPredicate selectPredicate = new SelectSomeFieldsPredicate(Arrays.asList("a", "b"));
        SelectExtractStatement selectExtractStatement = new SelectExtractStatement("idX", selectPredicate, null, "from",
                null, null);

        List<OperatorBean> expectedGeneratedBeans = Arrays.asList(
                new ProjectionBean("", "Projection", "a,b", null, null)
            );
        List<String> dependencies = Arrays.asList("from");

        Assert.assertEquals(selectExtractStatement.getInputViews(), dependencies);
        StatementTestUtils.assertGeneratedBeans(selectExtractStatement, expectedGeneratedBeans);
    }

    /**
     * Test the correctness of the generated beans by a SelectExtractStatement
     * with a KeywordExtractPredicate.
     * Get a graph by calling getInternalOperatorBeans() and getInternalLinkBeans()
     * methods and check if the generated path form the node getInputNodeID() to 
     * the node getOutputNodeID() is correct. Also check whether getInputViews()
     * is returning the correct dependencies.  
     */
    @Test
    public void testSelectExtractStatementBeansBuilder03() {
        ExtractPredicate extractPredicate = new KeywordExtractPredicate(Arrays.asList("c", "d"), "word",
                KeywordMatchingType.SUBSTRING_SCANBASED.toString());
        SelectExtractStatement selectExtractStatement = new SelectExtractStatement("id", null, extractPredicate, 
                "TableP9", null, null);

        List<OperatorBean> expectedGeneratedBeans = Arrays.asList(
                new KeywordMatcherBean("", "KeywordMatcher", "c,d", null, null, "word",
                        KeywordMatchingType.SUBSTRING_SCANBASED.toString())
            );
        List<String> dependencies = Arrays.asList("TableP9");

        Assert.assertEquals(selectExtractStatement.getInputViews(), dependencies);
        StatementTestUtils.assertGeneratedBeans(selectExtractStatement, expectedGeneratedBeans);
    }

    /**
     * Test the correctness of the generated beans by a SelectExtractStatement
     * with a SelectAllFieldsPredicate and a KeywordExtractPredicate.
     * Get a graph by calling getInternalOperatorBeans() and getInternalLinkBeans()
     * methods and check if the generated path form the node getInputNodeID() to 
     * the node getOutputNodeID() is correct. Also check whether getInputViews()
     * is returning the correct dependencies.  
     */
    @Test
    public void testSelectExtractStatementBeansBuilder04() {
        SelectPredicate selectPredicate = new SelectAllFieldsPredicate();
        ExtractPredicate extractPredicate = new KeywordExtractPredicate(Arrays.asList("f1"), "keyword", 
                KeywordMatchingType.CONJUNCTION_INDEXBASED.toString());
        SelectExtractStatement selectExtractStatement = new SelectExtractStatement("id", selectPredicate,
                extractPredicate, "source", null, null);

        List<OperatorBean> expectedGeneratedBeans = Arrays.asList(
                    new KeywordMatcherBean("", "KeywordMatcher", "f1", null, null, "keyword", 
                            KeywordMatchingType.CONJUNCTION_INDEXBASED.toString())
                );
        List<String> dependencies = Arrays.asList("source");

        Assert.assertEquals(selectExtractStatement.getInputViews(), dependencies);
        StatementTestUtils.assertGeneratedBeans(selectExtractStatement, expectedGeneratedBeans);
    }

    /**
     * Test the correctness of the generated beans by a SelectExtractStatement
     * with a SelectAllFieldsPredicate and a KeywordExtractPredicate.
     * Get a graph by calling getInternalOperatorBeans() and getInternalLinkBeans()
     * methods and check if the generated path form the node getInputNodeID() to 
     * the node getOutputNodeID() is correct. Also check whether getInputViews()
     * is returning the correct dependencies.  
     */
    @Test
    public void testSelectExtractStatementBeansBuilder05() {
        SelectPredicate selectPredicate = new SelectSomeFieldsPredicate(Arrays.asList("a", "b"));
        ExtractPredicate extractPredicate = new KeywordExtractPredicate(Arrays.asList("a", "b"), "x", 
                KeywordMatchingType.SUBSTRING_SCANBASED.toString());
        SelectExtractStatement selectExtractStatement = new SelectExtractStatement("_sid4", selectPredicate,
                extractPredicate, "from", null, null);

        List<OperatorBean> expectedGeneratedBeans = Arrays.asList(
                new KeywordMatcherBean("", "KeywordMatcher", "a,b", null, null, "x",
                        KeywordMatchingType.SUBSTRING_SCANBASED.toString()),
                new ProjectionBean("", "Projection", "a,b", null, null)
            );
        List<String> dependencies = Arrays.asList("from");

        Assert.assertEquals(selectExtractStatement.getInputViews(), dependencies);
        StatementTestUtils.assertGeneratedBeans(selectExtractStatement, expectedGeneratedBeans);
    }
}
