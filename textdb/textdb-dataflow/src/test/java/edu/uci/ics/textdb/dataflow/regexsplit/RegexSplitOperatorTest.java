package edu.uci.ics.textdb.dataflow.regexsplit;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.constants.TestConstantsChinese;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.storage.DataWriter;
import edu.uci.ics.textdb.storage.RelationManager;

public class RegexSplitOperatorTest {
    
    public static final String CHINESE_TABLE = "regex_split_test_chinese";
    
    @BeforeClass
    public static void setUp() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        // create the people table and write tuples in Chinese
        relationManager.createTable(CHINESE_TABLE, "../index/test_tables/" + CHINESE_TABLE, 
                TestConstantsChinese.SCHEMA_PEOPLE, LuceneAnalyzerConstants.chineseAnalyzerString());
        DataWriter chineseDataWriter = relationManager.getTableDataWriter(CHINESE_TABLE);
        chineseDataWriter.open();
        for (ITuple tuple : TestConstantsChinese.getSamplePeopleTuples()) {
            chineseDataWriter.insertTuple(tuple);
        }
        chineseDataWriter.close();
    }
    
    @AfterClass
    public static void cleanUp() throws TextDBException {
        RelationManager.getRelationManager().deleteTable(CHINESE_TABLE);
    }
    
    public static List<ITuple> getRegexSplitResults(
            String tableName, String splitRegex, String splitAttrName) throws TextDBException{
        ScanBasedSourceOperator scanSource = new ScanBasedSourceOperator(tableName);
        RegexSplitOperator regexSplit = new RegexSplitOperator(
                new RegexSplitPredicate(splitRegex, splitAttrName, RegexSplitPredicate.SplitType.GROUP_LEFT));
        regexSplit.setInputOperator(scanSource);
        
        List<ITuple> results = new ArrayList<>();
        ITuple tuple;
        
        regexSplit.open();
        while((tuple = regexSplit.getNextTuple()) != null) {
            results.add(tuple);
        }
        regexSplit.close();
        return results;
    }
    
    @Test
    public void test1() throws TextDBException {
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
        
        List<ITuple> results = getRegexSplitResults(CHINESE_TABLE, splitRegex, splitAttrName);
        
        List<String> splitStrings = results.stream()
                .map(tuple -> tuple.getField(TestConstantsChinese.DESCRIPTION).getValue().toString())
                .collect(Collectors.toList());

        System.out.println(splitStrings);
        Assert.assertEquals(splitResult, splitStrings);
    }

}
