package edu.uci.ics.textdb.dataflow.regexmatch.re2j;

import java.util.ArrayList;

public class RegexToTrigram {
	
	public class Query {
		// static final int or ENUM ?
		public static final int OR = 0;
		public static final int AND = 1;
		
		// private or public or no modifier?
		int operator;
		ArrayList<String> operands;
		ArrayList<Query> subQuerys;
		
		public Query(int operator) {
			this.operator = operator;
			operands = new ArrayList<String>();
			subQuerys = new ArrayList<Query>();
		}
		
		public void add (String operand) {
			operands.add(operand);
		}
		
		public void add (Query subQuery) {
			subQuerys.add(subQuery);
		}
	}
	
	
	public class RegexInfo {
		boolean emptyable;
		// arraylist or set?
		ArrayList<String> exact;
		ArrayList<String> prefix;
		ArrayList<String> suffix;
		Query match;
		
		public RegexInfo() {
			emptyable = true;
			exact = new ArrayList<String>();
			prefix = new ArrayList<String>();
			suffix = new ArrayList<String>();
			// init to AND operator?
			match = new Query(Query.AND);
		}
		
	}
	
	
	public Query traslate(String regex) {
	    Regexp re = Parser.parse(regex, RE2.PERL);
	    re = Simplify.simplify(re);
	    RegexInfo regexInfo = analyze(re);
	    simplify(regexInfo);
	    return regexInfo.match;
	}
	
	
	private RegexInfo analyze(Regexp re) {
		RegexInfo regexInfo = new RegexInfo();
		switch (re.op) {
		case ALTERNATE:
			break;
		case ANY_CHAR:
			break;
		case ANY_CHAR_NOT_NL:
			break;
		case BEGIN_LINE:
			break;
		case BEGIN_TEXT:
			break;
		case CAPTURE:
			break;
		case CHAR_CLASS:
			break;
		case CONCAT:
			break;
		case EMPTY_MATCH:
			break;
		case END_LINE:
			break;
		case END_TEXT:
			break;
		case LEFT_PAREN:
			break;
		case LITERAL:
			break;
		case NO_MATCH:
			break;
		case NO_WORD_BOUNDARY:
			break;
		case PLUS:
			break;
		case QUEST:
			break;
		case REPEAT:
			break;
		case STAR:
			break;
		case VERTICAL_BAR:
			break;
		case WORD_BOUNDARY:
			break;
		default:
			break;
		}
		
		return regexInfo;
	}
	
	
	private void simplify(RegexInfo regexInfo) {
		
	}
}
