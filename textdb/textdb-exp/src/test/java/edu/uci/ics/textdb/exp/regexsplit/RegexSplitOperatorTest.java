package edu.uci.ics.textdb.exp.regexsplit;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.constants.TestConstantsChinese;
import edu.uci.ics.textdb.api.constants.TestConstantsRegexSplit;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.exp.source.scan.ScanBasedSourceOperator;
import edu.uci.ics.textdb.exp.source.scan.ScanSourcePredicate;
import edu.uci.ics.textdb.storage.DataWriter;
import edu.uci.ics.textdb.storage.RelationManager;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;

/**
 * @author Zuozhi Wang
 * @author Qinhua Huang
 *
 */
public class RegexSplitOperatorTest {
    
    public static final String REGEX_TABLE = "regex_split_test";
    public static final String CHINESE_TABLE = "regex_split_test_chinese";
    
    @BeforeClass
    public static void setUp() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        
        RelationManager.getRelationManager().deleteTable(REGEX_TABLE);
        relationManager = RelationManager.getRelationManager();
        relationManager.createTable(REGEX_TABLE, "../index/test_tables/" + REGEX_TABLE, 
                TestConstantsRegexSplit.SCHEMA_PEOPLE, LuceneAnalyzerConstants.standardAnalyzerString());
        DataWriter regexDataWriter = relationManager.getTableDataWriter(REGEX_TABLE);
        regexDataWriter.open();
        for (Tuple tuple : TestConstantsRegexSplit.constructSamplePeopleTuples()) {
            regexDataWriter.insertTuple(tuple);
        }
        regexDataWriter.close();
        
        // create the people table and write tuples in Chinese
        relationManager.createTable(CHINESE_TABLE, "../index/test_tables/" + CHINESE_TABLE, 
                TestConstantsChinese.SCHEMA_PEOPLE, LuceneAnalyzerConstants.chineseAnalyzerString());
        DataWriter chineseDataWriter = relationManager.getTableDataWriter(CHINESE_TABLE);
        chineseDataWriter.open();
        for (Tuple tuple : TestConstantsChinese.getSamplePeopleTuples()) {
            chineseDataWriter.insertTuple(tuple);
        }
        chineseDataWriter.close();
    }
    
    @AfterClass
    public static void cleanUp() throws TextDBException {
        RelationManager.getRelationManager().deleteTable(CHINESE_TABLE);
        RelationManager.getRelationManager().deleteTable(REGEX_TABLE);
    }
    
    public static List<Tuple> computeRegexSplitResults( String tableName, String splitAttrName,
            String splitRegex, RegexSplitPredicate.SplitType splitType ) throws TextDBException {
        
        ScanBasedSourceOperator scanSource = new ScanBasedSourceOperator(new ScanSourcePredicate(tableName));
        RegexSplitOperator regexSplit = new RegexSplitOperator(
                new RegexSplitPredicate(splitRegex, splitAttrName, splitType));
        regexSplit.setInputOperator(scanSource);
        
        List<Tuple> results = new ArrayList<>();
        
        regexSplit.open();
        Tuple tuple;
        while((tuple = regexSplit.getNextTuple()) != null) {
            results.add(tuple);
        }
        regexSplit.close();
        return results;
    }
    
    /*
     *  When splitting a field that is not TextField nor StringField, the 
     *  operator will throw an exception.
     */
    @Test(expected = DataFlowException.class)
    public void test1() throws TextDBException {
        String splitRegex = "19";
        String splitAttrName = TestConstantsChinese.DATE_OF_BIRTH;
        List<Tuple> results = computeRegexSplitResults(CHINESE_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.GROUP_RIGHT);
    }
    
    /*
     * If no pattern is found in the field, the operator will not split the field.
     * Instead, it will return the original field as the result.
     */
    @Test
    public void test2() throws TextDBException {
        String splitRegex = "hi";
        String splitAttrName = TestConstantsRegexSplit.DESCRIPTION;
        
        List<String> splitResult = new ArrayList<>();
        splitResult.add("banana");
        splitResult.add("mississippi");
        
        List<Tuple> results = computeRegexSplitResults(REGEX_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.STANDALONE);
        
        List<String> splitStrings = results.stream()
                .map(tuple -> tuple.getField(TestConstantsRegexSplit.DESCRIPTION).getValue().toString())
                .collect(Collectors.toList());
        Assert.assertEquals(splitResult, splitStrings);
    }
    /*
     * In the STANDALONE mode, when a string contains multiple overlapping matching patterns, 
     * the operator will only return the longest one as the result.
     */
    @Test
    public void test3() throws TextDBException {
        String splitRegex = "b.*a";
        String splitAttrName = TestConstantsRegexSplit.DESCRIPTION;
        
        List<String> splitResult = new ArrayList<>();
        splitResult.add("banana");
        splitResult.add("mississippi");
        
        List<Tuple> results = computeRegexSplitResults(REGEX_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.STANDALONE);
        
        List<String> splitStrings = results.stream()
                .map(tuple -> tuple.getField(TestConstantsRegexSplit.DESCRIPTION).getValue().toString())
                .collect(Collectors.toList());
        Assert.assertEquals(splitResult, splitStrings);
    }
    
    /*
     * In the GROUP_LEFT mode, the operator will group the matching patterns to the left.
     */
    @Test
    public void test4() throws TextDBException {
        String splitRegex = "a.*n";
        String splitAttrName = TestConstantsRegexSplit.DESCRIPTION;
        
        List<String> splitResult = new ArrayList<>();
        splitResult.add("banan");
        splitResult.add("a");
        splitResult.add("mississippi");
        
        List<Tuple> results = computeRegexSplitResults(REGEX_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.GROUP_LEFT);
        
        List<String> splitStrings = results.stream()
                .map(tuple -> tuple.getField(TestConstantsRegexSplit.DESCRIPTION).getValue().toString())
                .collect(Collectors.toList());
        Assert.assertEquals(splitResult, splitStrings);
    }
    
    /*
     * In the GROUP_RIGHT mode, the operator will group the matching patterns to the right.
     */
    @Test
    public void test5() throws TextDBException {
        String splitRegex = "a.*n";
        String splitAttrName = TestConstantsRegexSplit.DESCRIPTION;
        
        List<String> splitResult = new ArrayList<>();
        splitResult.add("b");
        splitResult.add("anana");
        splitResult.add("mississippi");
        
        List<Tuple> results = computeRegexSplitResults(REGEX_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.GROUP_RIGHT);
        
        List<String> splitStrings = results.stream()
                .map(tuple -> tuple.getField(TestConstantsRegexSplit.DESCRIPTION).getValue().toString())
                .collect(Collectors.toList());
        Assert.assertEquals(splitResult, splitStrings);
    }
    
    /*
     * The word "banana" matches "ana" twice, but since the two matches overlap, the
     * operator will only return the first match. It's a behavior of the Pattern.matcher(). 
     */
    @Test
    public void test6() throws TextDBException {
        String splitRegex = "ana";
        String splitAttrName = TestConstantsRegexSplit.DESCRIPTION;
        
        List<String> splitResult = new ArrayList<>();
        splitResult.add("b");
        splitResult.add("ana");
        splitResult.add("na");
        splitResult.add("mississippi");
        
        List<Tuple> results = computeRegexSplitResults(REGEX_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.STANDALONE);
        
        List<String> splitStrings = results.stream()
                .map(tuple -> tuple.getField(TestConstantsRegexSplit.DESCRIPTION).getValue().toString())
                .collect(Collectors.toList());
        Assert.assertEquals(splitResult, splitStrings);
    }
    
    /*
     * Chinese test: STANDALONE, string field.
     */
    @Test
    public void testChinese1() throws TextDBException {
        String splitRegex = "克";
        String splitAttrName = TestConstantsChinese.LAST_NAME;
        
        List<String> splitResult = new ArrayList<>();
        splitResult.add("长孙");
        splitResult.add("洛");
        splitResult.add("克");
        splitResult.add("贝尔");
        splitResult.add("建筑");
        
        List<Tuple> results = computeRegexSplitResults(CHINESE_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.STANDALONE);
        
        List<String> splitStrings = results.stream()
                .map(tuple -> tuple.getField(TestConstantsChinese.LAST_NAME).getValue().toString())
                .collect(Collectors.toList());

        Assert.assertEquals(splitResult, splitStrings);
    }
    
    /*
     * Chinese test: GROUP_RIGHT, text field.
     */
    @Test
    public void testChinese2() throws TextDBException {
        String splitRegex = "学";
        String splitAttrName = TestConstantsChinese.DESCRIPTION;
        
        List<String> splitResult = new ArrayList<>();
        splitResult.add("北京大");
        splitResult.add("学电气工程");
        splitResult.add("学院");
        splitResult.add("北京大");
        splitResult.add("学计算机");
        splitResult.add("学院");
        splitResult.add("伟大的建筑是历史的坐标，具有传承的价值。");
        
        List<Tuple> results = computeRegexSplitResults(CHINESE_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.GROUP_RIGHT);
        
        List<String> splitStrings = results.stream()
                .map(tuple -> tuple.getField(TestConstantsChinese.DESCRIPTION).getValue().toString())
                .collect(Collectors.toList());

        Assert.assertEquals(splitResult, splitStrings);
    }
    
    /*
     * Chinese test: GROUP_LEFT, text field.
     */
    @Test
    public void testChinese3() throws TextDBException {
        String splitRegex = "学";
        String splitAttrName = TestConstantsChinese.DESCRIPTION;
        
        List<String> splitResult = new ArrayList<>();
        splitResult.add("北京大学");
        splitResult.add("电气工程学");
        splitResult.add("院");
        splitResult.add("北京大学");
        splitResult.add("计算机学");
        splitResult.add("院");
        splitResult.add("伟大的建筑是历史的坐标，具有传承的价值。");
        
        List<Tuple> results = computeRegexSplitResults(CHINESE_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.GROUP_LEFT);
        
        List<String> splitStrings = results.stream()
                .map(tuple -> tuple.getField(TestConstantsChinese.DESCRIPTION).getValue().toString())
                .collect(Collectors.toList());

        Assert.assertEquals(splitResult, splitStrings);
    }
    
    /*
     * Chinese test: STANDALONE, text field.
     */
    @Test
    public void testChinese4() throws TextDBException {
        String splitRegex = "学";
        String splitAttrName = TestConstantsChinese.DESCRIPTION;
        
        List<String> splitResult = new ArrayList<>();
        splitResult.add("北京大");
        splitResult.add("学");
        splitResult.add("电气工程");
        splitResult.add("学");
        splitResult.add("院");
        splitResult.add("北京大");
        splitResult.add("学");
        splitResult.add("计算机");
        splitResult.add("学");
        splitResult.add("院");
        splitResult.add("伟大的建筑是历史的坐标，具有传承的价值。");
        
        List<Tuple> results = computeRegexSplitResults(CHINESE_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.STANDALONE);
        
        List<String> splitStrings = results.stream()
                .map(tuple -> tuple.getField(TestConstantsChinese.DESCRIPTION).getValue().toString())
                .collect(Collectors.toList());

        Assert.assertEquals(splitResult, splitStrings);
    }
    
    /*
     * Chinese test: STANDALONE, text field, matching the whole field.
     */
    @Test
    public void testChinese5() throws TextDBException {
        String splitRegex = "北京大学电气工程学院";
        String splitAttrName = TestConstantsChinese.DESCRIPTION;
        
        List<String> splitResult = new ArrayList<>();
        splitResult.add("北京大学电气工程学院");
        splitResult.add("北京大学计算机学院");
        splitResult.add("伟大的建筑是历史的坐标，具有传承的价值。");
        
        List<Tuple> results = computeRegexSplitResults(CHINESE_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.STANDALONE);
        
        List<String> splitStrings = results.stream()
                .map(tuple -> tuple.getField(TestConstantsChinese.DESCRIPTION).getValue().toString())
                .collect(Collectors.toList());

        Assert.assertEquals(splitResult, splitStrings);
    }

    /*
     * Chinese test: STANDALONE, text field, no matching.
     */
    @Test
    public void testChinese6() throws TextDBException {
        String splitRegex = "美利坚合众国";
        String splitAttrName = TestConstantsChinese.DESCRIPTION;
        
        List<String> splitResult = new ArrayList<>();
        splitResult.add("北京大学电气工程学院");
        splitResult.add("北京大学计算机学院");
        splitResult.add("伟大的建筑是历史的坐标，具有传承的价值。");
        
        List<Tuple> results = computeRegexSplitResults(CHINESE_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.STANDALONE);
        
        List<String> splitStrings = results.stream()
                .map(tuple -> tuple.getField(TestConstantsChinese.DESCRIPTION).getValue().toString())
                .collect(Collectors.toList());

        Assert.assertEquals(splitResult, splitStrings);
    }
    
    /*
     * ID test: To test if each new tuple has a new ID.
     */
    @Test
    public void test7() throws TextDBException {
        String splitRegex = "ana";
        String splitAttrName = TestConstantsRegexSplit.DESCRIPTION;
        
        List<Tuple> results = computeRegexSplitResults(REGEX_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.STANDALONE);
        
        for (Tuple tuple : results) {
            tuple.getField(SchemaConstants._ID);
        }
    }
    
    /*
     * ID test: To test if each newly-split tuple's ID has conflict with the old tuple.
     */
    @Test
    public void test8() throws TextDBException {
        String splitRegex = "ana";
        String splitAttrName = TestConstantsRegexSplit.DESCRIPTION;
        
        List<Tuple> results = computeRegexSplitResults(REGEX_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.STANDALONE);
        
        ScanBasedSourceOperator scanSource = new ScanBasedSourceOperator(new ScanSourcePredicate(REGEX_TABLE));
        
        Tuple tupleTable;
        scanSource.open();
        while ((tupleTable = scanSource.getNextTuple()) != null) {
            for (Tuple tuple : results) {
                Assert.assertFalse(tuple.getField(SchemaConstants._ID).equals(tupleTable.getField(SchemaConstants._ID)));
            }
        }
        scanSource.close();
    }
}
