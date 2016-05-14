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
	GramBooleanQuery match = null;
	
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
		match = new GramBooleanQuery(GramBooleanQuery.QueryOp.ANY);
	}
	
	/**
	 * @return RegexInfo describing a regex that matching NO string
	 * This function shouldn't be called unless something goes wrong.
	 * It is used to handle error cases.
	 */
	public static RegexInfo matchNone() {
		RegexInfo regexInfo = new RegexInfo();
		regexInfo.match.operator = GramBooleanQuery.QueryOp.NONE;
		return regexInfo;
	}
	
	/**
	 * 
	 * @return RegexInfo describing a regex that matches ANY string
	 */
	public static RegexInfo matchAny() {
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
	public static RegexInfo emptyString() {

		RegexInfo regexInfo = new RegexInfo();
		regexInfo.emptyable = true;
		regexInfo.match.operator = GramBooleanQuery.QueryOp.ANY;
		regexInfo.exact.add("");
		return regexInfo;
	}
	
	/** 
	 * @return RegexInfo describing a regex that matching ANY SINGLE character
	 * For anyChar, prefix, suffix, and exact are null (unknown), 
	 * because we don't know the exact character.
	 */
	public static RegexInfo anyChar() {
		RegexInfo regexInfo = new RegexInfo();
		regexInfo.emptyable = false;
		return regexInfo;
	}

}