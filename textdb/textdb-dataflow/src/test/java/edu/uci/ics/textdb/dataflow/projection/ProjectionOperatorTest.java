package edu.uci.ics.textdb.dataflow.projection;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.DateField;
import edu.uci.ics.textdb.common.field.DoubleField;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcher;
import edu.uci.ics.textdb.dataflow.source.IndexBasedSourceOperator;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.writer.DataWriter;

public class ProjectionOperatorTest {
    
    private DataStore dataStore;
    private DataWriter dataWriter;
    private Analyzer luceneAnalyzer;

    @Before
    public void setUp() throws Exception {
        dataStore = new DataStore(DataConstants.INDEX_DIR, TestConstants.SCHEMA_PEOPLE);
        luceneAnalyzer = new StandardAnalyzer();
        dataWriter = new DataWriter(dataStore, luceneAnalyzer);
        dataWriter.clearData();
        dataWriter.writeData(TestConstants.getSamplePeopleTuples());
    }
    
    @After
    public void cleanUp() throws Exception {
        dataWriter.clearData();
    }
    
    public List<ITuple> getProjectionResults(IOperator inputOperator, List<String> projectionFields) throws DataFlowException {
        ProjectionPredicate projectionPredicate = new ProjectionPredicate(projectionFields);
        ProjectionOperator projection = new ProjectionOperator(projectionPredicate);
        projection.setInputOperator(inputOperator);
        
        List<ITuple> results = new ArrayList<>();;
        ITuple nextTuple = null;
        while ((nextTuple = projection.getNextTuple()) != null) {
            results.add(nextTuple);
        }
     
        return results;
    }

    @Test
    public void testProjection1() throws Exception {
        List<String> projectionFields = Arrays.asList(
                TestConstants.DESCRIPTION);
        Schema projectionSchema = new Schema(TestConstants.DESCRIPTION_ATTR);
                
        IField[] fields1 = { new TextField("Tall Angry") };
        IField[] fields2 = { new TextField("Short Brown") };
        IField[] fields3 = { new TextField("White Angry") };
        IField[] fields4 = { new TextField("Lin Clooney is Short and lin clooney is Angry") };
        IField[] fields5 = { new TextField("Tall Fair") };

        ITuple tuple1 = new DataTuple(projectionSchema, fields1);
        ITuple tuple2 = new DataTuple(projectionSchema, fields2);
        ITuple tuple3 = new DataTuple(projectionSchema, fields3);
        ITuple tuple4 = new DataTuple(projectionSchema, fields4);
        ITuple tuple5 = new DataTuple(projectionSchema, fields5);
        
        List<ITuple> expectedResults = Arrays.asList(tuple1, tuple2, tuple3, tuple4, tuple5);
        
    }

}
