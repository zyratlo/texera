package edu.uci.ics.texera.exp.keywordmatcher;

import java.util.*;
import java.util.stream.Collectors;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.exception.DataFlowException;
import edu.uci.ics.texera.api.exception.TextDBException;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.Utils;
import edu.uci.ics.texera.exp.common.AbstractSingleInputOperator;
import edu.uci.ics.texera.exp.utils.DataflowUtils;

public class KeywordMatcher extends AbstractSingleInputOperator {

    private final KeywordPredicate predicate;

    private Schema inputSchema;
    private Set<String> queryTokenSet;
    private ArrayList<String> queryTokenList;
    private ArrayList<String> queryTokenWithStopwordsList;

    public KeywordMatcher(KeywordPredicate predicate) {
        this.predicate = predicate;
        this.limit = predicate.getLimit();
        this.offset = predicate.getOffset();

    }

    @Override
    protected void setUp() throws TextDBException {
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = inputSchema;
        if (!inputSchema.containsField(SchemaConstants.PAYLOAD)) {
            outputSchema = Utils.addAttributeToSchema(outputSchema, SchemaConstants.PAYLOAD_ATTRIBUTE);
        }
        if (inputSchema.containsField(predicate.getSpanListName())) {
            throw new DataFlowException(ErrorMessages.DUPLICATE_ATTRIBUTE(predicate.getSpanListName(), inputSchema));
        } else {
            outputSchema = Utils.addAttributeToSchema(outputSchema,
                    new Attribute(predicate.getSpanListName(), AttributeType.LIST));
        }
        if (this.predicate.getMatchingType() == KeywordMatchingType.CONJUNCTION_INDEXBASED) {
            preProcessKeywordTokens();
        } else if (this.predicate.getMatchingType() == KeywordMatchingType.PHRASE_INDEXBASED) {
            preProcessKeywordTokensWithStopwords();

        }

    }

    private void preProcessKeywordTokens() {
        queryTokenSet = new HashSet<>(DataflowUtils.tokenizeQuery(predicate.getLuceneAnalyzerString(), predicate.getQuery()));
    }

    private void preProcessKeywordTokensWithStopwords() {
        queryTokenList = DataflowUtils.tokenizeQuery(predicate.getLuceneAnalyzerString(), predicate.getQuery());
        queryTokenWithStopwordsList = DataflowUtils.tokenizeQueryWithStopwords(predicate.getQuery());
    }

    @Override
    protected Tuple computeNextMatchingTuple() throws TextDBException {
        Tuple inputTuple = null;
        Tuple resultTuple = null;

        while ((inputTuple = inputOperator.getNextTuple()) != null) {
            resultTuple = processOneInputTuple(inputTuple);

            if (resultTuple != null) {
                break;
            }
        }
        return resultTuple;
    }

    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TextDBException {
        // There's an implicit assumption that, in open() method, PAYLOAD is
        // checked before SPAN_LIST.
        // Therefore, PAYLOAD needs to be checked and added first
        if (!inputSchema.containsField(SchemaConstants.PAYLOAD)) {
            inputTuple = DataflowUtils.getSpanTuple(inputTuple.getFields(),
                    DataflowUtils.generatePayloadFromTuple(inputTuple, predicate.getLuceneAnalyzerString()), outputSchema);
        }
        if (predicate.getSpanListName() != null) {
            inputTuple = DataflowUtils.getSpanTuple(inputTuple.getFields(), new ArrayList<Span>(), outputSchema);
        }

        List<Span> matchingResults = null;
        if (this.predicate.getMatchingType() == KeywordMatchingType.CONJUNCTION_INDEXBASED) {
           matchingResults =  appendConjunctionMatchingSpans(inputTuple, predicate.getAttributeNames(), queryTokenSet, predicate.getQuery());
        }
        if (this.predicate.getMatchingType() == KeywordMatchingType.PHRASE_INDEXBASED) {
            matchingResults = appendPhraseMatchingSpans(inputTuple, predicate.getAttributeNames(), queryTokenList, queryTokenWithStopwordsList, predicate.getQuery());
        }
        if (this.predicate.getMatchingType() == KeywordMatchingType.SUBSTRING_SCANBASED) {
            matchingResults = new ArrayList<>();
            DataflowUtils.appendSubstringMatchingSpans(inputTuple, predicate.getAttributeNames(), predicate.getQuery(), matchingResults);
        }
        if (matchingResults == null) {
            throw new DataFlowException("no matching result is provided");
        }
        if (matchingResults.isEmpty()) {
            return null;
        }

        ListField<Span> spanListField = inputTuple.getField(predicate.getSpanListName());
        List<Span> spanList = spanListField.getValue();
        spanList.addAll(matchingResults);

        return inputTuple;
    }

    @Override
    protected void cleanUp() {
    }

    private List<Span> appendPhraseMatchingSpans(Tuple inputTuple, List<String> attributeNames, List<String> queryTokenList, List<String> queryTokenListWithStopwords, String queryKeyword) throws DataFlowException {
        ListField<Span> payloadField = inputTuple.getField(SchemaConstants.PAYLOAD);
        List<Span> payload = payloadField.getValue();
        List<Span> matchingResults = new ArrayList<>();
        for (String attributeName : attributeNames) {
            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }

            // for STRING type, the query should match the fieldValue completely
            if (attributeType == AttributeType.STRING) {
                if (queryKeyword.equals(fieldValue)) {
                    Span span = new Span(attributeName, 0, fieldValue.length(), fieldValue, fieldValue);
                    matchingResults.add(span);
                }
            }

            // for TEXT type, spans need to be reconstructed according to the
            // phrase query
            if (attributeType == AttributeType.TEXT) {
                Set<String> queryTokenSet = new HashSet<>(queryTokenList);
                List<Span> relevantSpans = filterRelevantSpans(payload, queryTokenSet);
                List<Span> fieldSpanList = relevantSpans.stream().filter(span -> span.getAttributeName().equals(attributeName))
                        .collect(Collectors.toList());

                if (!DataflowUtils.isAllQueryTokensPresent(fieldSpanList, queryTokenSet)) {
                    // move on to next field if not all query tokens are present
                    // in the spans
                    continue;
                }
                matchingResults.addAll(DataflowUtils.constructPhraseMatchingSpans(attributeName, fieldValue, queryKeyword, fieldSpanList, queryTokenListWithStopwords, queryTokenList));
            }
        }
        return matchingResults;
    }

    private List<Span> appendConjunctionMatchingSpans(Tuple inputTuple, List<String> attributeNames, Set<String> queryTokenSet, String queryKeyword) throws DataFlowException {
        ListField<Span> payloadField = inputTuple.getField(SchemaConstants.PAYLOAD);
        List<Span> payload = payloadField.getValue();
        List<Span> matchingResults = new ArrayList<>();
        for (String attributeName : attributeNames) {
            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }

            // for STRING type, the query should match the fieldValue completely
            if (attributeType == AttributeType.STRING) {
                if (queryKeyword.equals(fieldValue)) {
                    Span span = new Span(attributeName, 0, fieldValue.length(), fieldValue, fieldValue);
                    matchingResults.add(span);
                }
            }

            // for TEXT type, every token in the query should be present in span
            // list for this field
            if (attributeType == AttributeType.TEXT) {
                List<Span> relevantSpans = filterRelevantSpans(payload, queryTokenSet);
                List<Span> fieldSpanList = relevantSpans.stream().filter(span -> span.getAttributeName().equals(attributeName))
                        .collect(Collectors.toList());
                if (DataflowUtils.isAllQueryTokensPresent(fieldSpanList, queryTokenSet)) {
                    matchingResults.addAll(fieldSpanList);
                }

            }
        }
        return matchingResults;
    }
    
    private List<Span> filterRelevantSpans(List<Span> spanList, Set<String> queryTokenSet) {
        List<Span> relevantSpans = new ArrayList<>();
        Iterator<Span> iterator = spanList.iterator();
        while (iterator.hasNext()) {
            Span span = iterator.next();
            if (queryTokenSet.contains(span.getKey())) {
                relevantSpans.add(span);
            }
        }
        return relevantSpans;
    }

    public KeywordPredicate getPredicate() {
        return this.predicate;
    }

}
