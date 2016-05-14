package edu.uci.ics.textdb.dataflow.regexmatch;

import com.google.re2j.PublicParser;
import com.google.re2j.PublicRE2;
import com.google.re2j.PublicRegexp;
import com.google.re2j.PublicSimplify;

/**
 * This class 
 * {@link https://swtch.com/~rsc/regexp/regexp4.html}
 * 
 * @Author Zuozhi Wang
 * @Author Shuying Lai
 * 
 */
public class RegexToTrigram {	

	/**
	 * Translate a regular expression to an object of TrigramBooleanQuery
	 * @param regex 
	 * @return TrigramBooleanQuery
	 */
	public static GramBooleanQuery translate(String regex) {
		// try to parse using RE2J
		try {
		    PublicRegexp re = PublicParser.parse(regex, PublicRE2.PERL);
		    re = PublicSimplify.simplify(re);
		    RegexInfo regexInfo = analyze(re);
		    return regexInfo.match;
		    // if RE2J parsing fails
		} catch (com.google.re2j.PatternSyntaxException re2j_e) {
			// try to parse using Java Regex
			// if succeeds, return matchAll (scan based)
			try {
				java.util.regex.Pattern.compile(regex);
				return RegexInfo.matchAny().match;
			// if Java Regex fails too, return matchNone (not a regex)
			} catch (java.util.regex.PatternSyntaxException java_e) {
				return RegexInfo.matchNone().match;
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
		switch (re.getOp()) {
		// NO_MATCH: a regex that matches "None".
		// It shouldn't happen unless something goes wrong.
		// It is used to handle error cases
		case NO_MATCH: {
			return RegexInfo.matchNone();
		}
		// treat the following cases as 
		// a regex that matches an empty string
		case EMPTY_MATCH:
		case WORD_BOUNDARY:	case NO_WORD_BOUNDARY:
		case BEGIN_LINE: 	case END_LINE:
		case BEGIN_TEXT: 	case END_TEXT: {
			return RegexInfo.emptyString();
		}
		// a regex that matches any character
		case ANY_CHAR: case ANY_CHAR_NOT_NL: {
			return RegexInfo.anyChar();
		}
		// TODO finish for every case
		case ALTERNATE:
			break;
		case CAPTURE:
			break;
		case CHAR_CLASS:
			break;
		case CONCAT:
			break;
		case LEFT_PAREN:
			break;
		case LITERAL:
			break;
		// a regex that indicates one or more occurrences of the preceding element.
		case PLUS:
			// the regex info of "(element)+" should be the same as that of a single "element"
			// except that the exact is null, because we don't know the number of repetitions.
			RegexInfo info = analyze(re.getSubs()[0]);
			info.exact = null;
			return info;
		case QUEST:
			break;
		// a regex that indicates that the preceding item is matched
		// at least min times, but not more than max times.
		case REPEAT:
			break;
		//a regex that indicates zero or more occurences of the preceding element.
		case STAR:
			return RegexInfo.matchAny();
		case VERTICAL_BAR:
			break;
		default:
			break;
		}
		
		return regexInfo;
	}
	
}
