package edu.uci.ics.textdb.exp.wordcount;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.IDField;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.field.IntegerField;
import edu.uci.ics.textdb.api.field.ListField;
import edu.uci.ics.textdb.api.field.StringField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.span.Span;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.Utils;
import edu.uci.ics.textdb.exp.common.AbstractSingleInputOperator;
import edu.uci.ics.textdb.exp.utils.DataflowUtils;

/**
 * @author Qinhua Huang
 */

public class WordCountOperator extends AbstractSingleInputOperator implements ISourceOperator{
    private WordCountOperatorPredicate predicate;
    
    public static final String WORD = "word";
    public static final String COUNT = "count";
    public static final Attribute WORD_ATTR = new Attribute(WORD, AttributeType.STRING);
    public static final Attribute COUNT_ATTR = new Attribute(COUNT, AttributeType.INTEGER);
    public static final Schema SCHEMA_WORD_COUNT = new Schema(SchemaConstants._ID_ATTRIBUTE, WORD_ATTR, COUNT_ATTR);
    
    private List<Entry<String, Integer>> sortedWordCountMap;
    private Iterator<Entry<String, Integer>> wordCountIterator;
    
    private Schema inputSchema;
    private Schema tmpSchema;
    
    public WordCountOperator(WordCountOperatorPredicate predicate) {
        this.predicate = predicate;
    }
    
    @Override
    protected void setUp() throws DataFlowException {
        this.outputSchema = SCHEMA_WORD_COUNT;
        inputSchema = this.inputOperator.getOutputSchema();
        tmpSchema = inputSchema;
        if (!inputSchema.containsField(SchemaConstants.PAYLOAD)) {
            tmpSchema = Utils.addAttributeToSchema(inputSchema, SchemaConstants.PAYLOAD_ATTRIBUTE);
        }
    }
    
    @Override
    protected Tuple computeNextMatchingTuple() throws TextDBException {
        if (sortedWordCountMap == null) {
            computeWordCount();
        }
        
        if (wordCountIterator.hasNext()) {
            Entry<String, Integer> entry = wordCountIterator.next();
            List<IField> tupleFieldList = new ArrayList<>();
            // Generate the new UUID.
            tupleFieldList.add(IDField.newRandomID());
            tupleFieldList.add(new StringField(entry.getKey()));
            tupleFieldList.add(new IntegerField(entry.getValue()));
            
            return new Tuple(outputSchema, tupleFieldList);
            
        }
        
        return null;
    }
    
    private void computeWordCount() throws TextDBException {
        Tuple tuple;
        HashMap<String, Integer> wordCountMap = new HashMap<>();
        while ((tuple = this.inputOperator.getNextTuple()) != null) {
            ListField<Span> payloadField = tuple.getField("payload");
            List<Span> payloadSpanList = payloadField.getValue();
            
            for (Span span : payloadSpanList) {
                if (span.getAttributeName().equals(predicate.getAttribute())) {
                    String key = span.getValue().toLowerCase();
                    wordCountMap.put(key, wordCountMap.get(key)==null ? 1 : wordCountMap.get(key) + 1);
                }
            }
        }
        sortedWordCountMap = wordCountMap.entrySet().stream()
                .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
                .collect(Collectors.toList());
        wordCountIterator = sortedWordCountMap.iterator();
    }

    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TextDBException {
        if (!inputSchema.containsField(SchemaConstants.PAYLOAD)) {
            inputTuple = DataflowUtils.getSpanTuple(inputTuple.getFields(),
                    DataflowUtils.generatePayloadFromTuple(inputTuple, predicate.getLuceneAnalyzerString()), tmpSchema);
        }
        return inputTuple;
    }

    @Override
    protected void cleanUp() throws TextDBException {
    }

}
