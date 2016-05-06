package edu.uci.ics.textdb.dataflow.regexmatch.re2j;

import java.util.ArrayList;

/*
 * @Author Zuozhi Wang
 * @Author Shuying Lai
 * 
 * RegexInfo for translating regex to trigrams
 * see https://swtch.com/~rsc/regexp/regexp4.html for details
 */
public class RegexInfo {
	boolean emptyable;
	// arraylist or set?
	ArrayList<String> exact;
	ArrayList<String> prefix;
	ArrayList<String> suffix;
	RegexTrigramQuery match;
	
	public RegexInfo() {
		emptyable = true;
		exact = new ArrayList<String>();
		prefix = new ArrayList<String>();
		suffix = new ArrayList<String>();
		// init to AND operator?
		match = new RegexTrigramQuery(RegexTrigramQuery.AND);
	}
	
}