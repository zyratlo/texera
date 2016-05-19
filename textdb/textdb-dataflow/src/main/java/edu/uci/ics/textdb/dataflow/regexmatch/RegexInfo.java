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
		match = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
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
		TranslatorUtils.clean(exact, false);
		
		// transfer information from exact to prefix and suffix
		if ( exact.size() > TranslatorUtils.MAX_EXACT_SIZE ||
			( TranslatorUtils.minLenOfString(exact) >= 3 && force) ||
			TranslatorUtils.minLenOfString(exact) >= 4){
			
			match.add(exact);
			for (String str: exact) {
				if (str.length() < 3) {
					prefix.add(str);
					suffix.add(str);
				} else {
					prefix.add(str.substring(0, 3));
					suffix.add(str.substring(str.length()-3, str.length()));
				}
			}
			exact.clear();
		}
		
		if (exact.isEmpty()) {
			simplifySet(prefix, false);
			simplifySet(suffix, true);
		}
		

	}
	
	/**
	 * simplifySet reduces the size of the given set (either prefix or suffix).
	 * There is no need to pass around enormous prefix or suffix sets, since 
	 * they will only be used to create trigrams.As they get too big, simplifySet
	 * moves the information they contain into the match query, which is
	 * more efficient to pass around.
	 * @param strList
	 * @param isSuffix indicates given string list is suffix list or not
	 */
	void simplifySet(List<String> strList, boolean isSuffix) {
		TranslatorUtils.clean(strList, isSuffix);
		
		// Add the OR of the current prefix/suffix set to the query.
		match.add(strList);
		
		// This loop cuts the length of prefix/suffix. It cuts all
		// strings longer than 3, and continues to cut strings
		// until the size of the list is below a threshold.
		// It cuts a prefix (suffix) string by only retaining the first (last) n characters of it
		// For example, for a prefix string "abcd", after cutting, it becomes "abc" if n = 3, "ab" if n = 2.
		// For a suffix string "abcd", after cutting, it becomes "bcd" if n = 3, "cd" if n = 2;
		for (int n = 3; n == 3 || strList.size() > TranslatorUtils.MAX_SET_SIZE; n--) {
			// replace set by strings of length n-1
			int w = 0; //TODO: better name?
			for (String str: strList) {
				if (str.length() > n) {
					if (!isSuffix) { //prefix
						str = str.substring(0, n);
					} else { //suffix
						str = str.substring(str.length()-n-1, str.length());
					}
				}
				if (w == 0 || strList.get(w-1) != str) {
					strList.set(w, str);
				}
			}
			strList = strList.subList(0, w+1);
			TranslatorUtils.clean(strList, isSuffix);
		}
		
		// Now make sure that the prefix/suffix sets aren't redundant.
		// For example, if we know "ab" is a possible prefix, then it
		// doesn't help at all to know that  "abc" is also a possible
		// prefix, so delete "abc".
		if (!isSuffix) {
			removeRedundantAffix((s, prefix) -> s.startsWith(prefix), strList);
		} else {
			removeRedundantAffix((s, suffix) -> s.endsWith(suffix), strList);
		}
		
	}
	
	private void removeRedundantAffix(TranslatorUtils.IContain iContain, List<String> strList) {
		if (strList.size() <= 1) {
			return ;
		}
		int w = 0;
		for (String str: strList) {
			if ( w == 0 || !iContain.containFunc(str, strList.get(w-1))) {
				strList.set(w, str);
				w ++;
			}
		}
		strList = strList.subList(0, w+1);
	}
	
	/**
	 * This function adds to the match query the trigrams for matching info.exact.
	 */
	void addExactToMatch() {
		match.add(exact);
	}

}