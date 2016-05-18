package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author Zuozhi Wang
 * @Author Shuying Lai
 * 
 * RegexInfo for translating regex to an n-gram boolean query. <br>
 * see <a href='https://swtch.com/~rsc/regexp/regexp4.html'>https://swtch.com/~rsc/regexp/regexp4.html</a> for details. <br>
 */
class RegexInfo {
	private static final int MAX_EXACT_SIZE = 7;
	
	boolean emptyable;
	List<String> exact = null;
	List<String> prefix = null;
	List<String> suffix = null;
	GramBooleanQuery match = null;
	
	/**
	 * This initializes RegexInfo:
	 * emptyable to false
	 * exact, prefix, suffix to empty arraylist
	 * match to match ALL
	 */
	RegexInfo() {
		emptyable = false;
		exact = new ArrayList<String>();
		prefix = new ArrayList<String>();
		suffix = new ArrayList<String>();
		match = new GramBooleanQuery(GramBooleanQuery.QueryOp.ANY);
	}
	
	/**
	 * @return RegexInfo describing a regex that matches NO string
	 * This function shouldn't be called unless something goes wrong.
	 * It is used to handle error cases.
	 */
	static RegexInfo matchNone() {
		RegexInfo regexInfo = new RegexInfo();
		regexInfo.match.operator = GramBooleanQuery.QueryOp.NONE;
		return regexInfo;
	}
	
	/**
	 * 
	 * @return RegexInfo describing a regex that matches ANY string
	 */
	static RegexInfo matchAny() {
		RegexInfo regexInfo = new RegexInfo();
		regexInfo.emptyable = true;
		regexInfo.prefix.add("");
		regexInfo.suffix.add("");
		regexInfo.match.operator = GramBooleanQuery.QueryOp.ANY;
		return regexInfo;
	}
	
	/**
	 * 
	 * @return RegexInfo describing a regex that matches an EMPTY string
	 */
	static RegexInfo emptyString() {

		RegexInfo regexInfo = new RegexInfo();
		regexInfo.emptyable = true;
		regexInfo.match.operator = GramBooleanQuery.QueryOp.ANY;
		regexInfo.exact.add("");
		return regexInfo;
	}
	
	/** 
	 * @return RegexInfo describing a regex that matches ANY SINGLE character
	 * For anyChar, prefix, suffix, and exact are null (unknown), 
	 * because we don't know the exact character.
	 */
	static RegexInfo anyChar() {
		RegexInfo regexInfo = new RegexInfo();
		regexInfo.emptyable = false;
		return regexInfo;
	}
	
	/**
	 * This function simplifies the regexpInfo when the exact set gets too large.
	 * If there are now too many exact strings,
	 * loop over them, adding trigrams and moving the relevant pieces into prefix and suffix.
	 * @param force
	 */
	void simplify(boolean force) {
		RegexToGramQueryTranslator.clean(exact, false);
		
		if ( exact.size() > MAX_EXACT_SIZE ||
			( RegexToGramQueryTranslator.minLenOfString(exact) >= 3	&& force) ||
			RegexToGramQueryTranslator.minLenOfString(exact) >= 4){
			for (String str: exact) {
				if (str.length() < 3) {
					prefix.add(str);
					suffix.add(str);
				} else {
					prefix.add(str.substring(0, 3));
					suffix.add(str.substring(str.length()-4, str.length()-1));
				}
			}
			exact.clear();
		}
		
		if (exact.isEmpty()) {
			simplifySet(prefix, false);
			simplifySet(suffix, true);
		}
	}
	
	void simplifySet(List<String> strList, boolean isSuffix) {
		//TODO
	}
	
	/**
	 * This function adds to the match query the trigrams for matching info.exact.
	 */
	void addExactToMatch() {
		match.add(exact);
	}

}