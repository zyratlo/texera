package edu.uci.ics.texera.exp.fuzzytokenmatcher;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.Utils;
import edu.uci.ics.texera.exp.common.AbstractSingleInputOperator;
import edu.uci.ics.texera.exp.utils.DataflowUtils;

/**
 *  @author Zuozhi Wang (zuozhiw)
 *  @author Parag Saraogi
 *  @author Varun Bharill
 *  
 *  This class provides token based fuzzy matching.
 */
public class FuzzyTokenMatcher extends AbstractSingleInputOperator {
    
    private final FuzzyTokenPredicate predicate;
    
    private Schema inputSchema;
    
    public FuzzyTokenMatcher(FuzzyTokenPredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    protected void setUp() throws TexeraException {
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = inputSchema;
        if (!outputSchema.containsField(SchemaConstants.PAYLOAD)) {
            outputSchema = Utils.addAttributeToSchema(outputSchema, SchemaConstants.PAYLOAD_ATTRIBUTE);
        }
        if (outputSchema.containsField(predicate.getSpanListName())) {
            throw new DataflowException(ErrorMessages.DUPLICATE_ATTRIBUTE(
                    predicate.getSpanListName(), outputSchema));
        }
        outputSchema = Utils.addAttributeToSchema(outputSchema, 
                new Attribute(predicate.getSpanListName(), AttributeType.LIST));
    }

    @Override
    protected Tuple computeNextMatchingTuple() throws TexeraException {
        Tuple inputTuple = null;
        Tuple resultTuple = null;
        
        while ((inputTuple = inputOperator.getNextTuple()) != null) {          
            // There's an implicit assumption that, in open() method, PAYLOAD is
            // checked before SPAN_LIST.
            // Therefore, PAYLOAD needs to be checked and added first
            if (!inputSchema.containsField(SchemaConstants.PAYLOAD)) {
                inputTuple = DataflowUtils.getSpanTuple(inputTuple.getFields(),
                        DataflowUtils.generatePayloadFromTuple(inputTuple, predicate.getLuceneAnalyzerStr()), outputSchema);
            }
            inputTuple = DataflowUtils.getSpanTuple(inputTuple.getFields(), new ArrayList<Span>(), outputSchema);
            
            resultTuple = processOneInputTuple(inputTuple);
            
            if (resultTuple != null) {
                break;
            }
        }
        return resultTuple;
    }

    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TexeraException {
        ListField<Span> payloadField = inputTuple.getField(SchemaConstants.PAYLOAD);
        List<Span> payload = payloadField.getValue();
        List<Span> relevantSpans = filterRelevantSpans(payload);
        List<Span> matchResults = new ArrayList<>();

        /*
         * The source operator returns spans even for those fields which did not
         * satisfy the threshold criterion. So if two attributes A,B have 10 and
         * 5 matching tokens, and we set threshold to 10, the number of spans
         * returned is 15. So we need to filter those 5 spans for attribute B.
         */
        for (String attributeName : this.predicate.getAttributeNames()) {
            AttributeType attributeType = this.inputSchema.getAttribute(attributeName).getAttributeType();
            
            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.TEXT && attributeType != AttributeType.STRING) {
                throw new DataflowException("FuzzyTokenMatcher: Fields other than TEXT or STRING are not supported");
            }
            
            List<Span> fieldSpans = 
                    relevantSpans.stream()
                    .filter(span -> span.getAttributeName().equals(attributeName))
                    .filter(span -> predicate.getQueryTokens().contains(span.getKey()))
                    .collect(Collectors.toList());
            
            if (fieldSpans.size() >= predicate.getThreshold()) {
                matchResults.addAll(fieldSpans);
            }          
        }

        if (matchResults.isEmpty()) {
            return null;
        }

        ListField<Span> spanListField = inputTuple.getField(predicate.getSpanListName());
        List<Span> spanList = spanListField.getValue();
        spanList.addAll(matchResults);

        return inputTuple;
    }
    
    private List<Span> filterRelevantSpans(List<Span> spanList) {
        List<Span> relevantSpans = new ArrayList<>();
        Iterator<Span> iterator = spanList.iterator();
        while (iterator.hasNext()) {
            Span span = iterator.next();
            if (predicate.getQueryTokens().contains(span.getKey())) {
                relevantSpans.add(span);
            }
        }
        return relevantSpans;
    }

    @Override
    protected void cleanUp() throws DataflowException {        
    }

    public FuzzyTokenPredicate getPredicate() {
        return this.predicate;
    }

}
