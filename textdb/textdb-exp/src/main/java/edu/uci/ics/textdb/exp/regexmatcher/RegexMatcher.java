package edu.uci.ics.textdb.exp.regexmatcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import edu.uci.ics.textdb.api.constants.ErrorMessages;
import edu.uci.ics.textdb.api.constants.SchemaConstants;
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
    private static final String labelSyntax = "<[^<>\\\\]*>";
    private HashMap<Integer, HashSet<String>> idLabelMapping;
    private String cleanedRegex = "";
    
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

        this.idLabelMapping = new HashMap<>();
        this.cleanedRegex = extractLabels(predicate.getRegex());
        // Check if labelled or unlabelled
        if(this.idLabelMapping.size()==0) {
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

        if(this.idLabelMapping.size() == 0) {
            // Unlabelled regex
            return processUnlabelledRegex(inputTuple);
        }
        // else labelled regex
        return processLabelledRegex(inputTuple);
    }

    /**
     * Process unlabelled regex pattern
     * @param inputTuple
     * @return tuple with matching entries
     */
    private Tuple processUnlabelledRegex(Tuple inputTuple) {
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
     * Process labelled regex on input tuple
     * @param inputTuple
     * @return tuple with matching regex pattern
     */
    private Tuple processLabelledRegex(Tuple inputTuple) {
        HashMap<Integer, HashSet<String>> labelSpanList = fetchLabelledSpanListValues(inputTuple);
        String regexWithVal = getModifiedRegex(this.cleanedRegex, labelSpanList);
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
        this.idLabelMapping = new HashMap<>();
        java.util.regex.Pattern patt = java.util.regex.Pattern.compile(this.labelSyntax,
                java.util.regex.Pattern.CASE_INSENSITIVE);
        java.util.regex.Matcher match = patt.matcher(generalRegexPattern);

        int id = 1;
        String regexMod = generalRegexPattern;
        while(match.find()) {
            int start = match.start();
            int end = match.end();

            String substr = generalRegexPattern.substring(start+1, end-1);
            String substrWithoutSpace = substr.replaceAll("\\s+", "");

            this.idLabelMapping.put(id, new HashSet<String>());

            if(substrWithoutSpace.contains("|")) {
                // Multiple value separated by OR operator
                String[] sublabs = substrWithoutSpace.split("[|]");
                for(String lab : sublabs)
                    this.idLabelMapping.get(id).add(lab);
            } else {
                this.idLabelMapping.get(id).add(substrWithoutSpace);
            }
            regexMod = regexMod.replace("<"+substr+">", "<"+id+">");
            id++;
        }
        return regexMod;
    }


    /**
     * Create Map of label id and corresponding span values
     * @param inputTuple
     * @return map of label id and corresponding span values
     */
    private HashMap<Integer, HashSet<String>> fetchLabelledSpanListValues(Tuple inputTuple) {
        HashMap<Integer, HashSet<String>> labelSpanList = new HashMap<Integer, HashSet<String>>();
        for (int id : this.idLabelMapping.keySet()) {
            HashSet<String> labels = this.idLabelMapping.get(id);
            HashSet<String> values = new HashSet<String>();
            for (String oneField : labels) {
                ListField<Span> spanListField = inputTuple.getField(oneField);
                List<Span> spanList = spanListField.getValue();
                for (Span span : spanList) {
                    values.add(span.getValue());
                }
            }
            labelSpanList.put(id, values);
        }
        return labelSpanList;
    }

    /**
     * Create Map of label id and corresponding span
     * @param inputTuple
     * @return map of label id and corresponding span
     */
    private HashMap<Integer, List<Span>> fetchLabelledSpanList(Tuple inputTuple) {
        HashMap<Integer, List<Span>> labelSpanList = new HashMap<Integer, List<Span>>();
        for (int id : this.idLabelMapping.keySet()) {
            HashSet<String> labels = this.idLabelMapping.get(id);
            HashSet<String> values = new HashSet<String>();
            labelSpanList.put(id, new ArrayList<>());
            for (String oneField : labels) {
                ListField<Span> spanListField = inputTuple.getField(oneField);
                labelSpanList.get(id).addAll(spanListField.getValue());
            }
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
     * Replace labels with actual values in labelled regex
     * @param regexMod
     * @param labelSpanList
     * @return regex with actual span values
     */
    private String getModifiedRegex(String regexMod, HashMap<Integer, HashSet<String>> labelSpanList) {
        for(HashMap.Entry<Integer, HashSet<String>> entry : labelSpanList.entrySet()){
            int id = entry.getKey();
            Object[] values = entry.getValue().toArray();
            String repVal = (String) values[0];
            for(int i = 1; i<values.length; i++)
                repVal += "|"+ values[i];
            repVal = "("+repVal+")";
            regexMod = regexMod.replaceAll("<"+id+">", repVal);
        }
        return regexMod;
    }



    @Override
    protected void cleanUp() throws DataFlowException {        
    }

    public RegexPredicate getPredicate() {
        return this.predicate;
    }
    
}
