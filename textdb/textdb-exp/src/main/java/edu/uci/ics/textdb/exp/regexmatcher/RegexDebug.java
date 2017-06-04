package edu.uci.ics.textdb.exp.regexmatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Chang on 6/3/17.
 */
public class RegexDebug {
    public List<String> labelList = new ArrayList<>();
    public  List<String>  RegexDebugtool(String regex) {

        List<String> affixList = new ArrayList<>();
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
        return affixList;
       // sortedAffixList = affixList;
       // sortedAffixList.sort((String o1, String o2)->o2.length()-o1.length());
    }
    public static void main(String[] args){
        String regex = "<lab1> <lab2>";
        RegexDebug r = new RegexDebug();
        List<String> ss = r.RegexDebugtool(regex);
        int i = 0;
        for(String s: ss){
            System.out.print(i + " " +s.length());
            i++;
        }
    }
}
