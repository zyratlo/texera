package edu.uci.ics.textdb.dataflow.regexmatch;

import com.google.re2j.PatternSyntaxException;
import com.google.re2j.PublicParser;
import com.google.re2j.PublicRE2;
import com.google.re2j.PublicRegexp;
import com.google.re2j.PublicSimplify;

import java.util.regex.*;

/**
 * 
 * @Author Zuozhi Wang
 * @Author Shuying Lai
 * @since 2016-05-07
 * 
 */
public class RegexToTrigram {	

	/**
	 * Translate a regular expression to an object of TrigramBooleanQeruy
	 * @param regex 
	 * @return TrigramBooleanQuery
	 */
	//TODO how to handle exception?
	public static TrigramBooleanQuery translate(String regex) {
		try {
		    PublicRegexp re = PublicParser.parse(regex, PublicRE2.getPERL());
		    re = PublicSimplify.simplify(re);
		    RegexInfo regexInfo = analyze(re);
		    return regexInfo.match;
		} catch (com.google.re2j.PatternSyntaxException re2j_e) {
			try {
				java.util.regex.Pattern.compile(regex);
				return RegexInfo.matchAll().match;
			} catch (java.util.regex.PatternSyntaxException java_e) {
				//TODO throw which exception? re2? java? our own?
				throw java_e;
			}
		}
	}
	
	
	/**
	 * Main function to analyze a regular expression
	 * @param PublicRegexp
	 * @return RegexInfo
	 */
	private static RegexInfo analyze(PublicRegexp re) {
		RegexInfo regexInfo = new RegexInfo();
		switch (re.getOp().toString()) {
		case "ALTERNATE":
			break;
		case "ANY_CHAR":
			break;
		case "ANY_CHAR_NOT_NL":
			break;
		case "BEGIN_LINE":
			break;
		case "BEGIN_TEXT":
			break;
		case "CAPTURE":
			break;
		case "CHAR_CLASS":
			break;
		case "CONCAT":
			break;
		case "EMPTY_MATCH":
			break;
		case "END_LINE":
			break;
		case "END_TEXT":
			break;
		case "LEFT_PAREN":
			break;
		case "LITERAL":
			break;
		case "NO_MATCH":
			break;
		case "NO_WORD_BOUNDARY":
			break;
		case "PLUS":
			break;
		case "QUEST":
			break;
		case "REPEAT":
			break;
		case "STAR":
			break;
		case "VERTICAL_BAR":
			break;
		case "WORD_BOUNDARY":
			break;
		default:
			break;
		}
		
		return regexInfo;
	}
	
}
