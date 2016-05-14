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
		match = new TrigramBooleanQuery(TrigramBooleanQuery.QueryOp.ANY);
	}
	
	/**
	 * @return RegexInfo describing a regex that doesn't match any string
	 * matchNone is used for handling error cases
	 */
	public static RegexInfo matchNone() {
		RegexInfo info = new RegexInfo();
		info.match.operator = TrigramBooleanQuery.QueryOp.NONE;
		return info;
	}
	
	/**
	 * 
	 * @return RegexInfo describing a regex that matches ANY string
	 */
	public static RegexInfo matchAny() {
		RegexInfo info = new RegexInfo();
		info.emptyable = true;
		info.prefix.add("");
		info.suffix.add("");
		info.match.operator = TrigramBooleanQuery.QueryOp.ANY;
		return info;
	}
	
	/**
	 * 
	 * @return RegexInfo describing a regex that matching an EMPTY string
	 */
	public static RegexInfo emptyString() {
		RegexInfo info = new RegexInfo();
		info.emptyable = true;
		info.match.operator = TrigramBooleanQuery.QueryOp.ANY;
		info.exact.add("");
		return info;
	}
	
	/** 
	 * @return RegexInfo describing a regex that matching ANY SINGLE character
	 * For anyChar, prefix, suffix, and exact are null (unknown), 
	 * because we don't know the exact character.
	 */
	public static RegexInfo anyChar() {
		RegexInfo info = new RegexInfo();
		info.emptyable = false;
		return info;
	}

}