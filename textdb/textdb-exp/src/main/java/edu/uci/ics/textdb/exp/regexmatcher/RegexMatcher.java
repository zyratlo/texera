package edu.uci.ics.textdb.exp.regexmatcher;

import java.util.*;
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

    // two available regex engines, RegexMatcher will try RE2J first
    private enum RegexEngine {
        JavaRegex, RE2J
    }

    private RegexEngine regexEngine;
    private com.google.re2j.Pattern re2jPattern;
    private java.util.regex.Pattern javaPattern;

    /**
     * Regex pattern for extracting labels.
     * Eg. Regex:- <drug> [^./</>] cure <disease>
     *     For above regex, only 'drug' and 'disease' are treated as label
     *     Skipping '/</>'
     */
    private static final String labelSyntax = "<[^<>\\\\]*>";
    private Set<String> labelsList;
    private String cleanedRegex = "";
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

        this.cleanedRegex = extractLabels(predicate.getRegex());
        // Check if labeled or unlabeled
        if(this.regexType == RegexType.NO_LABELS) {
            // No labels in regex, compile regex only one time
            compileRegexPattern(predicate.getRegex());
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

        if(this.regexType == RegexType.NO_LABELS) {
            // Unlabeled regex
            return processUnlabeledRegex(inputTuple);
        }
        // else labeled regex
        return processLabeledRegex(inputTuple);
    }

    /**
     * Process unlabeled regex pattern
     * @param inputTuple
     * @return tuple with matching entries
     */
    private Tuple processUnlabeledRegex(Tuple inputTuple) {
        List<Span> matchingResults = findRegexMatch(inputTuple);

        if (matchingResults.isEmpty()) {
            return null;
        }

        ListField<Span> spanListField = inputTuple.getField(predicate.getSpanListName());
        List<Span> spanList = spanListField.getValue();
        spanList.addAll(matchingResults);

        return inputTuple;
    }

    /**
     * Process labeled regex on input tuple
     * @param inputTuple
     * @return tuple with matching regex pattern
     */
    private Tuple processLabeledRegex(Tuple inputTuple) {
        Map<String, Set<String>> labelAttrValueList = fetchAttributeValues(inputTuple);
        String regexWithVal = getModifiedRegex(this.cleanedRegex, labelAttrValueList);
        compileRegexPattern(regexWithVal);
        List<Span> matchingResults = findRegexMatch(inputTuple);
        if (matchingResults.isEmpty()) {
            return null;
        }

        ListField<Span> spanListField = inputTuple.getField(predicate.getSpanListName());
        List<Span> spanList = spanListField.getValue();
        spanList.addAll(matchingResults);

        return inputTuple;
    }

    /**
     * Find regex matches in input tuple
     * @param inputTuple
     * @return span list for all matches
     */
    private List<Span> findRegexMatch(Tuple inputTuple) {
        List<Span> matchingResults = new ArrayList<>();

        for (String attributeName : predicate.getAttributeNames()) {
            AttributeType attributeType = inputSchema.getAttribute(attributeName).getAttributeType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }

            switch (regexEngine) {
                case JavaRegex:
                    matchingResults.addAll(javaRegexMatch(fieldValue, attributeName));
                    break;
                case RE2J:
                    matchingResults.addAll(re2jRegexMatch(fieldValue, attributeName));
                    break;
            }
        }
        return matchingResults;
    }

    private List<Span> javaRegexMatch(String fieldValue, String attributeName) {
        List<Span> matchingResults = new ArrayList<>();
        java.util.regex.Matcher javaMatcher = this.javaPattern.matcher(fieldValue);
        while (javaMatcher.find()) {
            int start = javaMatcher.start();
            int end = javaMatcher.end();
            matchingResults.add(
                    new Span(attributeName, start, end, this.predicate.getRegex(), fieldValue.substring(start, end)));
        }
        return matchingResults;
    }

    private List<Span> re2jRegexMatch(String fieldValue, String attributeName) {
        List<Span> matchingResults = new ArrayList<>();
        com.google.re2j.Matcher re2jMatcher = this.re2jPattern.matcher(fieldValue);
        while (re2jMatcher.find()) {
            int start = re2jMatcher.start();
            int end = re2jMatcher.end();
            matchingResults.add(
                    new Span(attributeName, start, end, this.predicate.getRegex(), fieldValue.substring(start, end)));
        }
        return matchingResults;
    }


    /**
     * Extract labels from regex
     * @param generalRegexPattern
     * @return cleaned regex containing ids instead of labels
     */
    private String extractLabels(String generalRegexPattern) {
        this.labelsList = new HashSet<>();
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(RegexMatcher.labelSyntax,
                java.util.regex.Pattern.CASE_INSENSITIVE);
        java.util.regex.Matcher matcher = pattern.matcher(generalRegexPattern);
        // Extracting labels from regex
        String cleanedRegex = generalRegexPattern;
        while(matcher.find()) {
            String substr = generalRegexPattern.substring(matcher.start() + 1, matcher.end() - 1);
            String substrTrimmed = substr.trim();
            this.labelsList.add(substrTrimmed);
            cleanedRegex = cleanedRegex.replace("<" + substr + ">", "<" + substrTrimmed + ">");
        }
        if(this.labelsList.size() == 0)
            regexType = RegexType.NO_LABELS;
        else {
            // Check if regex contains qualifiers
            pattern = java.util.regex.Pattern.compile("[^a-zA-Z0-9<> ]",
                    java.util.regex.Pattern.CASE_INSENSITIVE);
            matcher = pattern.matcher(generalRegexPattern);
            if(matcher.find())
                regexType = RegexType.LABELED_WITH_QUALIFIERS;
            else
                regexType = RegexType.LABELED_WITHOUT_QUALIFIER;
        }
        return cleanedRegex;
    }


    /**
     * Create Map of label id and corresponding attribute values
     * @param inputTuple
     * @return map of label id and corresponding attribute values
     */
    private Map<String, Set<String>> fetchAttributeValues(Tuple inputTuple) {
        Map<String, Set<String>> labelSpanList = new HashMap<>();
        for (String label : this.labelsList) {
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
     * Create Map of label id and corresponding span
     * @param inputTuple
     * @return map of label id and corresponding span
     */
    private Map<String, List<Span>> fetchAttributeSpanList(Tuple inputTuple) {
        Map<String, List<Span>> labelSpanList = new HashMap<>();
        for (String label : this.labelsList) {
            ListField<Span> spanListField = inputTuple.getField(label);
            labelSpanList.put(label, new ArrayList<>(spanListField.getValue()));
        }
        return labelSpanList;
    }

    /**
     * Compile regex pattern
     * @param regex
     */
    private void compileRegexPattern(String regex) {
        // try Java Regex first
        try {
            if (this.predicate.isIgnoreCase()) {
                this.javaPattern = java.util.regex.Pattern.compile(regex,
                        java.util.regex.Pattern.CASE_INSENSITIVE);
                this.regexEngine = RegexEngine.JavaRegex;
            } else {
                this.javaPattern = java.util.regex.Pattern.compile(regex);
                this.regexEngine = RegexEngine.JavaRegex;
            }

            // if Java Regex fails, try RE2J
        } catch (java.util.regex.PatternSyntaxException javaException) {
            try {
                if (this.predicate.isIgnoreCase()) {
                    this.re2jPattern = com.google.re2j.Pattern.compile(regex,
                            com.google.re2j.Pattern.CASE_INSENSITIVE);
                    this.regexEngine = RegexEngine.RE2J;
                } else {
                    this.re2jPattern = com.google.re2j.Pattern.compile(regex);
                    this.regexEngine = RegexEngine.RE2J;
                }

                // if RE2J also fails, throw exception
            } catch (com.google.re2j.PatternSyntaxException re2jException) {
                throw new DataFlowException(javaException.getMessage(), javaException);
            }
        }
    }

    /**
     * Replace labels with actual values in labeled regex
     * @param cleanedRegex
     * @param labelAttrValuesList
     * @return regex with actual span values
     */
    private String getModifiedRegex(String cleanedRegex, Map<String, Set<String>> labelAttrValuesList) {
        for(Map.Entry<String, Set<String>> entry : labelAttrValuesList.entrySet()){
            String repVal = "(" + entry.getValue().stream().collect(Collectors.joining("|")) + ")";
            cleanedRegex = cleanedRegex.replaceAll("<"+entry.getKey()+">", repVal);
        }
        return cleanedRegex;
    }



    @Override
    protected void cleanUp() throws DataFlowException {        
    }

    public RegexPredicate getPredicate() {
        return this.predicate;
    }
    
}
