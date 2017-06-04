package edu.uci.ics.textdb.exp.regexmatcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import edu.uci.ics.textdb.api.field.ListField;
import edu.uci.ics.textdb.api.span.Span;
import edu.uci.ics.textdb.api.tuple.Tuple;

public class LabeledRegexFilter {

    private ArrayList<String> labelList = new ArrayList<>();
    private ArrayList<String> affixList = new ArrayList<>();
    private ArrayList<String> sortedAffixList = new ArrayList<>(); // sort the affixList by length in decreasing order to short-cut the filter tuple operation.
    
    public LabeledRegexFilter(String regex) {
        // populate labelList and affixList
        Matcher match = Pattern.compile(RegexMatcher.CHECK_REGEX_LABEL).matcher(regex);
        int pre = 0;
        while (match.find()) {
            int start = match.start();
            int end = match.end();
            String affix = regex.substring(pre, start);
            affixList.add(affix);
            String label = regex.substring(start + 1, end - 1);
            labelList.add(label);
            pre = end;
        }
        affixList.add(regex.substring(pre));

       sortedAffixList = new ArrayList<>(affixList);
       sortedAffixList.sort((String o1, String o2)->o2.length()-o1.length());
    }
    
    public boolean filterTuple(Tuple tuple, String attribute) {
            for (String affix : sortedAffixList) {
                if (! tuple.getField(attribute).getValue().toString().contains(affix)) {
                    return false;
                }
            }

        return true;
    }
    
    public Tuple processTuple(Tuple tuple, RegexPredicate predicate) {


        
        Map<String, List<Span>> labelValues = fetchLabelSpans(tuple);
        
        List<Span> allAttrsMatchSpans = new ArrayList<>();
        for (String attribute : predicate.getAttributeNames()) {
            boolean isValidTuple = filterTuple(tuple, attribute);

            if (! isValidTuple) {
                continue;
            }

            String fieldValue = tuple.getField(attribute).getValue().toString();

            List<List<Integer>> matchList = new ArrayList<>();
            
            for (int i = 0; i < labelList.size(); i++) {
                String label = labelList.get(i);
                String prefix = affixList.get(i);
                String suffix = affixList.get(i+1);
                
                List<Span> relevantSpans = labelValues.get(label).stream()
                        .filter(span -> span.getAttributeName().equals(attribute)).collect(Collectors.toList());
                
                if (i == 0) {
                    List<Span> validSpans = relevantSpans.stream()
                            .filter(span -> fieldValue.substring(span.getStart() - prefix.length(), span.getStart()).equals(prefix))
                            .collect(Collectors.toList());
                    matchList = validSpans.stream()
                            .map(span -> new ArrayList<Integer>(Arrays.asList(span.getStart() - prefix.length(), span.getStart())))
                            .collect(Collectors.toList());
                }
                
                List<List<Integer>> newMatchList = new ArrayList<>();
                
                for (List<Integer> previousMatch : matchList) {
                    for (Span span : relevantSpans) {
                        if (matchList.stream().filter(match -> match.get(1).equals(span.getStart())).findAny().isPresent()
                                && fieldValue.substring(span.getEnd(), span.getEnd() + suffix.length()).equals(suffix)) {
                            newMatchList.add(Arrays.asList(previousMatch.get(0), span.getEnd() + suffix.length()));
                        }
                    }
                }
                
                matchList = newMatchList;
                if (matchList.isEmpty()) {
                    break;
                }
            }
            
            matchList.stream().forEach(match -> allAttrsMatchSpans.add(
                    new Span(attribute, match.get(0), match.get(1), predicate.getRegex(), fieldValue.substring(match.get(0), match.get(1)))));
        }
        
        if (allAttrsMatchSpans.isEmpty()) {
            return null;
        }

        ListField<Span> spanListField = tuple.getField(predicate.getSpanListName());
        List<Span> spanList = spanListField.getValue();
        spanList.addAll(allAttrsMatchSpans);

        return tuple;
    }
    
    /**
     * Creates Map of label and corresponding s[ams
     * @param inputTuple
     * @return 
     */
    private Map<String, List<Span>> fetchLabelSpans(Tuple inputTuple) {
        Map<String, List<Span>> labelSpanMap = new HashMap<>();
        for (String label : this.labelList) {
            ListField<Span> spanListField = inputTuple.getField(label);
            labelSpanMap.put(label, spanListField.getValue());
        }
        return labelSpanMap;
    }

}
