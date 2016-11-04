package edu.uci.ics.textdb.plangen;

import java.util.HashMap;
import java.util.HashSet;

import org.json.JSONObject;
import org.junit.Test;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISink;
import edu.uci.ics.textdb.api.plan.Plan;
import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.connector.OneToNBroadcastConnector;
import edu.uci.ics.textdb.dataflow.connector.OneToNBroadcastConnector.ConnectorOutputOperator;
import edu.uci.ics.textdb.dataflow.fuzzytokenmatcher.FuzzyTokenMatcher;
import edu.uci.ics.textdb.dataflow.join.Join;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcherSourceOperator;
import edu.uci.ics.textdb.dataflow.nlpextrator.NlpExtractor;
import edu.uci.ics.textdb.dataflow.regexmatch.RegexMatcher;
import edu.uci.ics.textdb.dataflow.sink.FileSink;
import edu.uci.ics.textdb.plangen.operatorbuilder.FileSinkBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.FuzzyTokenMatcherBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.JoinBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.KeywordMatcherBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.NlpExtractorBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.OperatorBuilderUtils;
import edu.uci.ics.textdb.plangen.operatorbuilder.RegexMatcherBuilder;
import junit.framework.Assert;

public class LogicalPlanTest {

    public static HashMap<String, String> keywordSourceProperties = new HashMap<String, String>() {
        {
            JSONObject schemaJsonJSONObject = new JSONObject();
            schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "id, city, location, content");
            schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "integer, string, string, text");

            put(KeywordMatcherBuilder.KEYWORD, "irvine");
            put(KeywordMatcherBuilder.MATCHING_TYPE, "PHRASE_INDEXBASED");
            put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
            put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "STRING, STRING, TEXT");
            put(OperatorBuilderUtils.DATA_DIRECTORY, "./index");
            put(OperatorBuilderUtils.SCHEMA, schemaJsonJSONObject.toString());
        }
    };

    public static HashMap<String, String> regexMatcherProperties = new HashMap<String, String>() {
        {
            put(RegexMatcherBuilder.REGEX, "ca(lifornia)?");
            put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "location, content");
            put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "string, text");
        }
    };
    
    public static HashMap<String, String> fuzzyTokenMatcherProperties = new HashMap<String, String>() {
        {
            put(FuzzyTokenMatcherBuilder.FUZZY_STRING, "university college school");
            put(FuzzyTokenMatcherBuilder.THRESHOLD_RATIO, "0.5");
            put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "content");
            put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "text");
        }
    };

    public static HashMap<String, String> nlpExtractorProperties = new HashMap<String, String>() {
        {
            put(NlpExtractorBuilder.NLP_TYPE, "Location");
            put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "content");
            put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "text");
        }
    };

    public static HashMap<String, String> joinProperties = new HashMap<String, String>() {
        {
            put(JoinBuilder.JOIN_PREDICATE, "CharacterDistance");
            put(JoinBuilder.JOIN_DISTANCE, "100");
            put(JoinBuilder.JOIN_ID_ATTRIBUTE_NAME, "id");
            put(JoinBuilder.JOIN_ID_ATTRIBUTE_TYPE, "integer");
            put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "content");
            put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "text");
        }
    };

    public static HashMap<String, String> fileSinkProperties = new HashMap<String, String>() {
        {
            put(FileSinkBuilder.FILE_PATH, "./result.txt");
        }
    };

    /*
     * Test a valid operator graph.
     * 
     * KeywordSource --> RegexMatcher --> FileSink
     * 
     */
    @Test
    public void testLogicalPlan1() throws Exception {
        LogicalPlan LogicalPlan = new LogicalPlan();

        LogicalPlan.addOperator("source", "KeywordSource", keywordSourceProperties);
        LogicalPlan.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        LogicalPlan.addOperator("sink", "FileSink", fileSinkProperties);
        LogicalPlan.addLink("source", "regex");
        LogicalPlan.addLink("regex", "sink");

        Plan queryPlan = LogicalPlan.buildQueryPlan();

        ISink fileSink = queryPlan.getRoot();
        Assert.assertTrue(fileSink instanceof FileSink);

        IOperator regexMatcher = ((FileSink) fileSink).getInputOperator();
        Assert.assertTrue(regexMatcher instanceof RegexMatcher);

        IOperator keywordSource = ((RegexMatcher) regexMatcher).getInputOperator();
        Assert.assertTrue(keywordSource instanceof KeywordMatcherSourceOperator);

    }

    /*
     * Test a valid operator graph.
     *                  -> RegexMatcher -->
     * KeywordSource --<                     >-- Join --> FileSink
     *                  -> NlpExtractor -->
     * 
     */
    @Test
    public void testLogicalPlan2() throws Exception {
        LogicalPlan LogicalPlan = new LogicalPlan();

        JSONObject schemaJsonJSONObject = new JSONObject();
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "id, city, location, content");
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "integer, string, string, text");

        LogicalPlan.addOperator("source", "KeywordSource", keywordSourceProperties);
        LogicalPlan.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        LogicalPlan.addOperator("nlp", "NlpExtractor", nlpExtractorProperties);
        LogicalPlan.addOperator("join", "Join", joinProperties);
        LogicalPlan.addOperator("sink", "FileSink", fileSinkProperties);

        LogicalPlan.addLink("source", "regex");
        LogicalPlan.addLink("source", "nlp");
        LogicalPlan.addLink("regex", "join");
        LogicalPlan.addLink("nlp", "join");
        LogicalPlan.addLink("join", "sink");

        Plan queryPlan = LogicalPlan.buildQueryPlan();

        ISink fileSink = queryPlan.getRoot();
        Assert.assertTrue(fileSink instanceof FileSink);

        IOperator join = ((FileSink) fileSink).getInputOperator();
        Assert.assertTrue(join instanceof Join);

        IOperator joinInput1 = ((Join) join).getInnerOperator();
        Assert.assertTrue(joinInput1 instanceof RegexMatcher);

        IOperator joinInput2 = ((Join) join).getOuterOperator();
        Assert.assertTrue(joinInput2 instanceof NlpExtractor);

        IOperator connectorOut1 = ((RegexMatcher) joinInput1).getInputOperator();
        Assert.assertTrue(connectorOut1 instanceof ConnectorOutputOperator);

        IOperator connectorOut2 = ((NlpExtractor) joinInput2).getInputOperator();
        Assert.assertTrue(connectorOut2 instanceof ConnectorOutputOperator);       
        
        HashSet<Integer> connectorIndices = new HashSet<>();
        connectorIndices.add(((ConnectorOutputOperator) connectorOut1).getOutputIndex());
        connectorIndices.add(((ConnectorOutputOperator) connectorOut2).getOutputIndex());
        Assert.assertEquals(connectorIndices.size(), 2);

        OneToNBroadcastConnector connector1 = ((ConnectorOutputOperator) connectorOut1).getOwnerConnector();
        OneToNBroadcastConnector connector2 = ((ConnectorOutputOperator) connectorOut2).getOwnerConnector();

        Assert.assertSame(connector1, connector2);

        IOperator keywordSource = connector1.getInputOperator();
        Assert.assertTrue(keywordSource instanceof KeywordMatcherSourceOperator);
    }
    
    /*
     * Test a valid operator graph.
     * 
     *                  --> RegexMatcher -->
     *                  |                    >-- Join1
     * KeywordSource --< -> NlpExtractor -->          >-- Join2 --> FileSink
     *                  |                           /
     *                  --> FuzzyTokenMatcher ----->
     * 
     */
    @Test
    public void testLogicalPlan3() throws Exception {
        LogicalPlan LogicalPlan = new LogicalPlan();

        JSONObject schemaJsonJSONObject = new JSONObject();
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "id, city, location, content");
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "integer, string, string, text");

        LogicalPlan.addOperator("source", "KeywordSource", keywordSourceProperties);
        LogicalPlan.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        LogicalPlan.addOperator("nlp", "NlpExtractor", nlpExtractorProperties);
        LogicalPlan.addOperator("fuzzytoken", "FuzzyTokenMatcher", fuzzyTokenMatcherProperties);
        LogicalPlan.addOperator("join", "Join", joinProperties);
        LogicalPlan.addOperator("join2", "Join", joinProperties);
        LogicalPlan.addOperator("sink", "FileSink", fileSinkProperties);

        LogicalPlan.addLink("source", "regex");
        LogicalPlan.addLink("source", "nlp");
        LogicalPlan.addLink("source", "fuzzytoken");
        LogicalPlan.addLink("regex", "join");
        LogicalPlan.addLink("nlp", "join");
        LogicalPlan.addLink("join", "join2");
        LogicalPlan.addLink("fuzzytoken", "join2");
        LogicalPlan.addLink("join2", "sink");

        Plan queryPlan = LogicalPlan.buildQueryPlan();

        ISink fileSink = queryPlan.getRoot();
        Assert.assertTrue(fileSink instanceof FileSink);

        IOperator join2 = ((FileSink) fileSink).getInputOperator();
        Assert.assertTrue(join2 instanceof Join);
        
        IOperator join2Input1 = ((Join) join2).getInnerOperator();
        Assert.assertTrue(join2Input1 instanceof Join);
        
        IOperator join2Input2 = ((Join) join2).getOuterOperator();
        Assert.assertTrue(join2Input2 instanceof FuzzyTokenMatcher);

        IOperator join1Input1 = ((Join) join2Input1).getInnerOperator();
        Assert.assertTrue(join1Input1 instanceof RegexMatcher);

        IOperator join1Input2 = ((Join) join2Input1).getOuterOperator();
        Assert.assertTrue(join1Input2 instanceof NlpExtractor);

        IOperator connectorOut1 = ((RegexMatcher) join1Input1).getInputOperator();
        Assert.assertTrue(connectorOut1 instanceof ConnectorOutputOperator);

        IOperator connectorOut2 = ((NlpExtractor) join1Input2).getInputOperator();
        Assert.assertTrue(connectorOut2 instanceof ConnectorOutputOperator);       
        
        IOperator connectorOut3 = ((FuzzyTokenMatcher) join2Input2).getInputOperator();
        Assert.assertTrue(connectorOut3 instanceof ConnectorOutputOperator);
        
        HashSet<Integer> connectorIndices = new HashSet<>();
        connectorIndices.add(((ConnectorOutputOperator) connectorOut1).getOutputIndex());
        connectorIndices.add(((ConnectorOutputOperator) connectorOut2).getOutputIndex());
        connectorIndices.add(((ConnectorOutputOperator) connectorOut3).getOutputIndex());
        Assert.assertEquals(connectorIndices.size(), 3);

        OneToNBroadcastConnector connector1 = ((ConnectorOutputOperator) connectorOut1).getOwnerConnector();
        OneToNBroadcastConnector connector2 = ((ConnectorOutputOperator) connectorOut2).getOwnerConnector();
        OneToNBroadcastConnector connector3 = ((ConnectorOutputOperator) connectorOut3).getOwnerConnector();

        Assert.assertSame(connector1, connector2);
        Assert.assertSame(connector1, connector3);

        IOperator keywordSource = connector1.getInputOperator();
        Assert.assertTrue(keywordSource instanceof KeywordMatcherSourceOperator);
    }


    
    /*
     * Test a operator graph without a source operator
     * 
     * RegexMatcher --> FileSink
     * 
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidLogicalPlan1() throws Exception {
        LogicalPlan LogicalPlan = new LogicalPlan();

        LogicalPlan.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        LogicalPlan.addOperator("sink", "FileSink", fileSinkProperties);
        LogicalPlan.addLink("regex", "sink");

        LogicalPlan.buildQueryPlan();
    }

    /*
     * Test a operator graph without a sink operator
     * 
     * KeywordSource --> RegexMatcher
     * 
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidLogicalPlan2() throws Exception {
        LogicalPlan LogicalPlan = new LogicalPlan();

        LogicalPlan.addOperator("source", "KeywordSource", keywordSourceProperties);
        LogicalPlan.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        LogicalPlan.addLink("source", "regex");

        LogicalPlan.buildQueryPlan();
    }

    /*
     * Test a operator graph with more than one sink operators
     *                                   -> FileSink1
     * KeywordSource --> RegexMatcher --<
     *                                   -> FileSink2
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidLogicalPlan3() throws Exception {
        LogicalPlan LogicalPlan = new LogicalPlan();

        LogicalPlan.addOperator("source", "KeywordSource", keywordSourceProperties);
        LogicalPlan.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        LogicalPlan.addOperator("sink1", "FileSink", fileSinkProperties);
        LogicalPlan.addOperator("sink2", "FileSink", fileSinkProperties);

        LogicalPlan.addLink("source", "regex");
        LogicalPlan.addLink("regex", "sink1");
        LogicalPlan.addLink("regex", "sink2");

        LogicalPlan.buildQueryPlan();
    }

    /*
     * Test a operator graph with a disconnected component
     * 
     * KeywordSource --> RegexMatcher --> FileSink
     * RegexMatcher --> NlpExtractor
     * (a disconnected graph)
     * 
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidLogicalPlan4() throws Exception {
        LogicalPlan LogicalPlan = new LogicalPlan();

        LogicalPlan.addOperator("source", "KeywordSource", keywordSourceProperties);
        LogicalPlan.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        LogicalPlan.addOperator("sink1", "FileSink", fileSinkProperties);

        LogicalPlan.addOperator("regex2", "RegexMatcher", regexMatcherProperties);
        LogicalPlan.addOperator("nlp", "NlpExtractor", nlpExtractorProperties);

        LogicalPlan.addLink("source", "regex");
        LogicalPlan.addLink("regex", "sink1");
        LogicalPlan.addLink("regex2", "nlp");

        LogicalPlan.buildQueryPlan();
    }

    /*
     * Test a operator graph with a cycle
     * 
     * KeywordSource --> RegexMatcher1 -->   
     *                                     >- Join --> FileSink                              
     *                                 -->
     * RegexMathcer2 -> NlpExtractor -< 
     *                                 --> (back to the same) RegexMatcher2
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidLogicalPlan5() throws Exception {
        LogicalPlan LogicalPlan = new LogicalPlan();

        LogicalPlan.addOperator("source", "KeywordSource", keywordSourceProperties);
        LogicalPlan.addOperator("regex1", "RegexMatcher", regexMatcherProperties);
        LogicalPlan.addOperator("sink1", "FileSink", fileSinkProperties);

        LogicalPlan.addOperator("regex2", "RegexMatcher", regexMatcherProperties);
        LogicalPlan.addOperator("nlp", "NlpExtractor", nlpExtractorProperties);
        LogicalPlan.addOperator("join", "Join", joinProperties);


        LogicalPlan.addLink("source", "regex1");
        LogicalPlan.addLink("regex1", "join");
        LogicalPlan.addLink("regex2", "nlp");
        LogicalPlan.addLink("nlp", "regex2");
        LogicalPlan.addLink("nlp", "join");
        LogicalPlan.addLink("join", "sink1");

        LogicalPlan.buildQueryPlan();
    }
    
    /*
     * Test a operator graph with an invalid input arity
     * 
     * KeywordSource1 --> RegexMatcher1 ->
     *                                    >-- FileSink
     * KeywordSource2 --> RegexMatcher2 ->
     * 
     * 
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidLogicalPlan6() throws Exception {
        LogicalPlan LogicalPlan = new LogicalPlan();

        LogicalPlan.addOperator("source", "KeywordSource", keywordSourceProperties);
        LogicalPlan.addOperator("source2", "KeywordSource", keywordSourceProperties);

        LogicalPlan.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        LogicalPlan.addOperator("regex2", "RegexMatcher", regexMatcherProperties);

        LogicalPlan.addOperator("sink1", "FileSink", fileSinkProperties);

        LogicalPlan.addLink("source", "regex");
        LogicalPlan.addLink("source2", "regex2");
        LogicalPlan.addLink("regex", "sink1");
        LogicalPlan.addLink("regex2", "sink1");

        LogicalPlan.buildQueryPlan();
    }
    
    /*
     * Test a operator graph with an invalid output arity
     *                 -> RegexMatcher1 --> FileSink
     * KeywordSource -<
     *                 -> RegexMatcher2
     *                 
     * It's okay for KeywordSource to have 2 outputs,
     * the problem is RegexMatcher2 doesn't have any outputs.
     * 
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidLogicalPlan7() throws Exception {
        LogicalPlan LogicalPlan = new LogicalPlan();

        LogicalPlan.addOperator("source", "KeywordSource", keywordSourceProperties);

        LogicalPlan.addOperator("regex", "RegexMatcher", regexMatcherProperties);
        LogicalPlan.addOperator("regex2", "RegexMatcher", regexMatcherProperties);

        LogicalPlan.addOperator("sink1", "FileSink", fileSinkProperties);

        LogicalPlan.addLink("source", "regex");
        LogicalPlan.addLink("source", "regex2");
        LogicalPlan.addLink("regex", "sink1");

        LogicalPlan.buildQueryPlan();
    }
    
    /*
     * Test a operator graph with a cycle
     * 
     * KeywordSource --> FileSik --> (back to the same) KeywordSource
     * 
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidLogicalPlan8() throws Exception {
        LogicalPlan LogicalPlan = new LogicalPlan();

        LogicalPlan.addOperator("source", "KeywordSource", keywordSourceProperties);
        LogicalPlan.addOperator("sink1", "FileSink", fileSinkProperties);

        LogicalPlan.addLink("source", "sink1");
        LogicalPlan.addLink("sink1", "source");

        LogicalPlan.buildQueryPlan();
    }

}
