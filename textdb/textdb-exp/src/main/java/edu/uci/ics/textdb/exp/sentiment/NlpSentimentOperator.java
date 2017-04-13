package edu.uci.ics.textdb.exp.sentiment;

import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import edu.uci.ics.textdb.api.constants.ErrorMessages;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.field.IntegerField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.Utils;

public class NlpSentimentOperator implements IOperator {
    
    private final NlpSentimentPredicate predicate;
    private IOperator inputOperator;
    private Schema outputSchema;
    private int cursor = CLOSED;
    
    StanfordCoreNLP sentimentPipeline;
    
    public NlpSentimentOperator(NlpSentimentPredicate predicate) {
        this.predicate = predicate;
    }
    
    public void setInputOperator(IOperator operator) {
        if (cursor != CLOSED) {  
            throw new RuntimeException("Cannot link this operator to other operator after the operator is opened");
        }
        this.inputOperator = operator;
    }
    
    /*
     * adds a new field to the schema, with name resultAttributeName and type Integer
     */
    private Schema transformSchema(Schema inputSchema) {
        if (inputSchema.containsField(predicate.getResultAttributeName())) {
            throw new RuntimeException(String.format(
                    "result attribute name %s is already in the original schema %s", 
                    predicate.getResultAttributeName(),
                    inputSchema.getAttributeNames()));
        }
        return Utils.addAttributeToSchema(inputSchema, 
                new Attribute(predicate.getResultAttributeName(), AttributeType.INTEGER));
    }

    @Override
    public void open() throws TextDBException {
        if (cursor != CLOSED) {
            return;
        }
        if (inputOperator == null) {
            throw new DataFlowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
        }
        inputOperator.open();
        Schema inputSchema = inputOperator.getOutputSchema();
        
        // check if input schema is present
        if (! inputSchema.containsField(predicate.getTargetAttributeName())) {
            throw new RuntimeException(String.format(
                    "target attribute %s is not in the input schema %s",
                    predicate.getTargetAttributeName(),
                    inputSchema.getAttributeNames()));
        }
        
        // check if attribute type is valid
        AttributeType targetAttributeType = 
                inputSchema.getAttribute(predicate.getTargetAttributeName()).getAttributeType();
        boolean isValidType = targetAttributeType.equals(AttributeType.STRING) || 
                targetAttributeType.equals(AttributeType.TEXT);
        if (! isValidType) {
            throw new RuntimeException(String.format(
                    "target attribute %s must have type String or Text, its actual type is %s",
                    predicate.getTargetAttributeName(),
                    targetAttributeType));
        }
        
        // generate output schema by transforming the input schema
        outputSchema = transformSchema(inputOperator.getOutputSchema());
        
        cursor = OPENED;
        
        // setup NLP sentiment analysis pipeline
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        sentimentPipeline = new StanfordCoreNLP(props);
    }

    @Override
    public Tuple getNextTuple() throws TextDBException {
        if (cursor == CLOSED) {
            return null;
        }
        Tuple inputTuple = inputOperator.getNextTuple();
        if (inputTuple == null) {
            return null;
        }
        
        List<IField> outputFields = inputTuple.getFields();
        outputFields.add(new IntegerField(computeSentimentScore(inputTuple)));
        
        return new Tuple(outputSchema, outputFields);
    }
    
    
    private Integer computeSentimentScore(Tuple inputTuple) {
        String targetText = inputTuple.<IField>getField(predicate.getTargetAttributeName()).getValue().toString();
        Annotation documentAnnotation = new Annotation(targetText);
        
        // mainSentiment is calculated by the sentiment class of the longest sentence
        Integer mainSentiment = 0;
        Integer longestSentenceLength = 0;
        for (CoreMap sentence : documentAnnotation.get(CoreAnnotations.SentencesAnnotation.class)) {
            Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
            int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
            String sentenceText = sentence.toString();
            if (sentenceText.length() > longestSentenceLength) {
                mainSentiment = sentiment;
            }
        }
        
        return mainSentiment;
    }

    @Override
    public void close() throws TextDBException {
        if (cursor == CLOSED) {
            return;
        }
        if (inputOperator != null) {
            inputOperator.close();
        }
        cursor = CLOSED;
    }

    @Override
    public Schema getOutputSchema() {
        return this.outputSchema;
    }
    
    

}
