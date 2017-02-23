package edu.uci.ics.textdb.dataflow.regexsplit;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.IDictionary;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.constants.TestConstantsChinese;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.DateField;
import edu.uci.ics.textdb.common.field.DoubleField;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.common.Dictionary;
import edu.uci.ics.textdb.dataflow.dictionarymatcher.DictionaryMatcherTestHelper;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
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
        RegexSplitOperator regexSplit = new RegexSplitOperator(new RegexSplitPredicate(splitRegex, splitAttrName));
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
        
        List<ITuple> results = getRegexSplitResults(CHINESE_TABLE, splitRegex, splitAttrName);
        System.out.println(Utils.getTupleListString(results));
    }

}
