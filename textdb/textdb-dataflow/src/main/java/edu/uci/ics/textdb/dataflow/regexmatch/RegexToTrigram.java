package edu.uci.ics.textdb.dataflow.regexmatch;

import com.google.re2j.PublicParser;
import com.google.re2j.PublicRE2;
import com.google.re2j.PublicRegexp;
import com.google.re2j.PublicSimplify;

/*
 * @Author Zuozhi Wang
 * @Author Shuying Lai
 * 
 */

public class RegexToTrigram {	

	/**
	 * Translate a regular expression to an object of TrigramBooleanQeruy
	 * @param regex 
	 * @return TrigramBooleanQuery
	 */
	public static TrigramBooleanQuery translate(String regex) {
	    PublicRegexp re = new PublicRegexp(PublicParser.parse(regex, PublicRE2.getPERL()));
	    re = new PublicRegexp(PublicSimplify.simplify(re));
	    RegexInfo regexInfo = analyze(re);
	    simplify(regexInfo);
	    return regexInfo.match;
	}
	
	
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
	
	private static void simplify(RegexInfo regexInfo) {
		
	}
}
