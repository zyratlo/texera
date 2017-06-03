package edu.uci.ics.textdb.exp.regexmatcher;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import edu.uci.ics.textdb.api.constants.ErrorMessages;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.ListField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.span.Span;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.Utils;
import edu.uci.ics.textdb.exp.common.AbstractSingleInputOperator;
import edu.uci.ics.textdb.exp.utils.DataflowUtils;

/**
 * Created by chenli on 3/25/16.
 * 
 * @author Shuying Lai (laisycs)
 * @author Zuozhi Wang (zuozhiw)
 * @author Bhushan Pagariya (bhushanpagariya)
 * @author Harshini Shah
 * @author Yashaswini Amaresh

 */
public class RegexMatcher extends AbstractSingleInputOperator {
    
    private final RegexPredicate predicate;

    /**
     * Regex pattern for extracting labels.
     * Eg. Regex:- <drug> [^./</>] cure <disease>
     *     For above regex, only 'drug' and 'disease' are treated as label
     *     Skipping '/</>'
     */
    public static final String CHECK_REGEX_LABEL = "<[^<>\\\\]*>";
    public static final String CHECK_REGEX_QUALIFIER = "[^a-zA-Z0-9<> ]";
    
    private Set<String> labelList;
    private String cleanedRegex;
    private LabeledRegexFilter labeledRegexFilter;
    
    private Pattern regexPattern;
    
    private enum RegexType {
        NO_LABELS, LABELED_WITHOUT_QUALIFIER, LABELED_WITH_QUALIFIERS
    }

    private RegexType regexType;
    private Schema inputSchema;

    public RegexMatcher(RegexPredicate predicate) {
        this.predicate = predicate;
    }
    
    @Override
    protected void setUp() throws DataFlowException {
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = inputSchema;
        
        if (this.inputSchema.containsField(predicate.getSpanListName())) {
            throw new DataFlowException(ErrorMessages.DUPLICATE_ATTRIBUTE(predicate.getSpanListName(), inputSchema));
        }
        outputSchema = Utils.addAttributeToSchema(inputSchema, 
                new Attribute(predicate.getSpanListName(), AttributeType.LIST));

        preprocessRegex();
        // Check if labeled or unlabeled
        if(this.regexType == RegexType.NO_LABELS) {
            // No labels in regex, compile regex only one time
            regexPattern = Pattern.compile(predicate.getRegex());
        } else if (this.regexType == RegexType.LABELED_WITHOUT_QUALIFIER) {
            labeledRegexFilter = new LabeledRegexFilter(cleanedRegex);
        }
    }
    
    @Override
    protected Tuple computeNextMatchingTuple() throws TextDBException {
        Tuple inputTuple = null;
        Tuple resultTuple = null;
        
        while ((inputTuple = inputOperator.getNextTuple()) != null) {
            inputTuple = DataflowUtils.getSpanTuple(inputTuple.getFields(), new ArrayList<Span>(), outputSchema);         
            resultTuple = processOneInputTuple(inputTuple);
            if (resultTuple != null) {
                break;
            }
        }
        
        return resultTuple;
    }

    /**
     * This function returns a list of spans in the given tuple that match the
     * regex For example, given tuple ("george watson", "graduate student", 23,
     * "(949)888-8888") and regex "g[^\s]*", this function will return
     * [Span(name, 0, 6, "g[^\s]*", "george watson"), Span(position, 0, 8,
     * "g[^\s]*", "graduate student")]
     * 
     * @param inputTuple
     *            document in which search is performed
     * @return a list of spans describing the occurrence of a matching sequence
     *         in the document
     * @throws DataFlowException
     */
    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws DataFlowException {
        if (inputTuple == null) {
            return null;
        }
        
        if (this.regexType == RegexType.LABELED_WITHOUT_QUALIFIER) {
            return labeledRegexFilter.processTuple(inputTuple, predicate);
        } else if (this.regexType != RegexType.NO_LABELS) {
            Map<String, Set<String>> labelAttrValueList = fetchLabelValues(inputTuple);
            String regexWithVal = rewriteRegexWithLabelValues(labelAttrValueList);
            regexPattern = Pattern.compile(regexWithVal);
            return processRegex(inputTuple);
        } else {
            return processRegex(inputTuple);
        }
    }

    /**
     * Process regex pattern
     * @param inputTuple
     * @return tuple with matching entries
     */
    private Tuple processRegex(Tuple inputTuple) {
        List<Span> matchingResults = new ArrayList<>();

        for (String attributeName : predicate.getAttributeNames()) {
            AttributeType attributeType = inputSchema.getAttribute(attributeName).getAttributeType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }
            
            matchingResults.addAll(findRegexMatch(fieldValue, attributeName));
        }
        
        if (matchingResults.isEmpty()) {
            return null;
        }

        ListField<Span> spanListField = inputTuple.getField(predicate.getSpanListName());
        List<Span> spanList = spanListField.getValue();
        spanList.addAll(matchingResults);

        return inputTuple;
    }


    private List<Span> findRegexMatch(String fieldValue, String attributeName) {
        List<Span> matchingResults = new ArrayList<>();
        Matcher javaMatcher = regexPattern.matcher(fieldValue);
        while (javaMatcher.find()) {
            int start = javaMatcher.start();
            int end = javaMatcher.end();
            matchingResults.add(
                    new Span(attributeName, start, end, this.predicate.getRegex(), fieldValue.substring(start, end)));
        }
        return matchingResults;
    }


    /**
     * Pre-process the input regex
     * 1. determine the type of the regex: no_label / labeled_with_qualifier / labeled_without_qualifier
     * 2. if it's labeled, extract the label and put them to labelList
     * 3. if it's labeled, cleans the labels and put the final regex in cleanedRegex
     */
    private void preprocessRegex() {
        Matcher labelMatcher = Pattern.compile(CHECK_REGEX_LABEL).matcher(predicate.getRegex());
        cleanedRegex = predicate.getRegex();
        while (labelMatcher.find()) {
            String labelStr = predicate.getRegex().substring(
                    labelMatcher.start() + 1, labelMatcher.end() - 1);
            String substrTrimmed = labelStr.trim();
            labelList.add(substrTrimmed);
            cleanedRegex = cleanedRegex.replace("<" + labelStr + ">", "<" + substrTrimmed + ">");
        }
        
        if (labelList.size() == 0) {
            regexType = RegexType.NO_LABELS;
            return;
        }
        
        Matcher qualifierMatcher = Pattern.compile(CHECK_REGEX_QUALIFIER).matcher(cleanedRegex);
        if (qualifierMatcher.find()) {
            regexType = RegexType.LABELED_WITH_QUALIFIERS;
        } else {
            regexType = RegexType.LABELED_WITHOUT_QUALIFIER;
        }
    }


    /**
     * Create Map of label id and corresponding attribute values
     * @param inputTuple
     * @return map of label id and corresponding attribute values
     */
    private Map<String, Set<String>> fetchLabelValues(Tuple inputTuple) {
        Map<String, Set<String>> labelSpanList = new HashMap<>();
        for (String label : this.labelList) {
            Set<String> values = new HashSet<>();
            ListField<Span> spanListField = inputTuple.getField(label);
            List<Span> spanList = spanListField.getValue();
            for (Span span : spanList) {
                String attrValue = span.getValue();
                // Replace special characters in the values (which might interfere with
                // the RegexPattern) with their escape sequence
                StringBuilder modifiedAttrValue = new StringBuilder();

                for(int i = 0; i < attrValue.length(); i++) {
                    // escape a non-letter non-digit character will be itself
                    if(! Character.isLetterOrDigit(attrValue.charAt(i))) {
                        modifiedAttrValue.append("\\" + attrValue.charAt(i));
                    } else {
                        modifiedAttrValue.append(attrValue.charAt(i));
                    }
                }

                if(this.predicate.isIgnoreCase())
                    values.add(modifiedAttrValue.toString().toLowerCase());
                else
                    values.add(modifiedAttrValue.toString());
            }
            labelSpanList.put(label, values);
        }
        return labelSpanList;
    }


    /**
     * Replace labels with actual values in labeled regex
     * @param cleanedRegex
     * @param labelValueList
     * @return regex with actual span values
     */
    private String rewriteRegexWithLabelValues(Map<String, Set<String>> labelValueList) {
        String regexWithValue = cleanedRegex;
        for(Map.Entry<String, Set<String>> entry : labelValueList.entrySet()){
            String repVal = "(" + entry.getValue().stream().collect(Collectors.joining("|")) + ")";
            cleanedRegex = cleanedRegex.replaceAll("<"+entry.getKey()+">", repVal);
        }
        return regexWithValue;
    }


    @Override
    protected void cleanUp() throws DataFlowException {        
    }

    public RegexPredicate getPredicate() {
        return this.predicate;
    }
    
}
