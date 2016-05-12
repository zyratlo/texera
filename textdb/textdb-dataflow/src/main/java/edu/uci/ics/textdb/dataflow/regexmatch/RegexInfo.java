package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author Zuozhi Wang
 * @Author Shuying Lai
 * 
 * RegexInfo for translating regex to trigrams
 * @see https://swtch.com/~rsc/regexp/regexp4.html for details
 */
public class RegexInfo {
	boolean emptyable;
	List<String> exact = null;
	List<String> prefix = null;
	List<String> suffix = null;
	TrigramBooleanQuery match = null;
	
	/**
	 * This initializes RegexInfo:
	 * emptyable to false
	 * exact, prefix, suffix to empty arraylist
	 * match to match ALL
	 */
	public RegexInfo() {
		emptyable = false;
		exact = new ArrayList<String>();
		prefix = new ArrayList<String>();
		suffix = new ArrayList<String>();
		match = new TrigramBooleanQuery(TrigramBooleanQuery.ALL);
	}
	
	/**
	 *
	 * @return RegexInfo describing a regex that matching NO string
	 */
	public static RegexInfo matchNone() {
		RegexInfo info = new RegexInfo();
		info.match.operator = TrigramBooleanQuery.NONE;
		return info;
	}
	
	/**
	 * 
	 * @return RegexInfo describing a regex that matching ANY string
	 */
	public static RegexInfo matchAny() {
		RegexInfo info = new RegexInfo();
		info.match.operator = TrigramBooleanQuery.ALL;
		return info;
	}
	
	/**
	 * 
	 * @return RegexInfo describing a regex that matching an EMPTY string
	 */
	public static RegexInfo emptyString() {
		RegexInfo info = new RegexInfo();
		info.emptyable = true;
		info.match.operator = TrigramBooleanQuery.ALL;
		info.exact.add("");
		return info;
	}
	
	/**
	 * 
	 * @return RegexInfo describing a regex that matching ANY SINGLE character
	 */
	public static RegexInfo anyChar() {
		RegexInfo info = new RegexInfo();
		info.emptyable = false;
		return info;
	}
	
	/**
	 * This function has not been implemented yet.
	 * @param x
	 * @param y
	 * @return RegexInfo describing a regex that matches either x or y (x|y)
	 */
	public static RegexInfo alternate(RegexInfo x, RegexInfo y) {
		RegexInfo info = new RegexInfo();
		return info;
	}
	
	/**
	 * This function has not been implemented yet.
	 * @param x
	 * @param y
	 * @return RegexInfor describing a regex that matches x followed by y (xy)
	 */
	public static RegexInfo concat(RegexInfo x, RegexInfo y) {
		RegexInfo info = new RegexInfo();
		return info;
	}

}