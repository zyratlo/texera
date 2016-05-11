package com.google.re2j;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Public Wrapper class for re2j.Regexp
 * Regexp class represents the abstract syntax tree
 * 
 * for example,
 * regex: "abc"
 * abstract syntax tree:
 * CONCAT
 * --LITERAL a
 * --LITERAL b
 * --LITERAL c
 * 
 * regex: "a*|b"
 * abstract syntax tree:
 * ALTERNATE
 * --STAR
 * ----LITERAL a
 * --LITERAL b
 * 
 * regex: [a-f]{1-3}
 * abstract syntax tree:
 * REPEAT min:1, max:3
 * --CHAR_CLASS a-f
 * 
 * 
 * @author Zuozhi Wang
 *
 */
public class PublicRegexp extends Regexp {
/*
	// fields originally declared in Regexp:
	// for detailed explanations please see getter methods
	
	// note that for variable subs, although original comments
	// say it's never null, it could still be null
  
	Op op;                   // operator
	int flags;               // bitmap of parse flags
	Regexp[] subs;           // subexpressions, if any.  Never null.
		                           // subs[0] is used as the freelist.
	
	int[] runes;             // matched runes, for LITERAL, CHAR_CLASS
	int min, max;            // min, max for REPEAT
	int cap;                 // capturing index, for CAPTURE
	String name;             // capturing name, for CAPTURE
	
*/
	
	// publicSubs are an array of subexpressions with type PublicRegexp
	private PublicRegexp[] publicSubs;

	/**
	 * private shallow copy constructor
	 * Regexp[] subs is an array of sub-expressions of type Regexp
	 * shallow copy only copies reference to the array
	 */
	private PublicRegexp(Regexp that) {
		super(that);
	}
	  
	/**
	 * deep copy converts every Regexp Object in subexpressions to a PublicRegexp object
	 * and put them in publicSubs array
	 * only public entry point to construct a PublicRegexp object
	 * @param Regexp re, A Regexp that needs to be converted to PublicRegexp
	 * @return PublicRegexp
	 */
	public static PublicRegexp deepCopy(Regexp re) {
		PublicRegexp publicRegexp = new PublicRegexp(re);
		if (re.subs != null) {
			// initialize publicSubs array
			publicRegexp.publicSubs = new PublicRegexp[re.subs.length];
			// map every Regexp sub-expression to a PublicRegexp sub-expression
			Stream<PublicRegexp> publicSubStream = 
					Arrays.asList(re.subs).stream()
					.map(sub -> PublicRegexp.deepCopy(sub));
			// convert the result PublicRegexp subexpressions to an array
			publicSubStream.collect(Collectors.toList()).toArray(publicRegexp.publicSubs);
		} else {
			// if subs is null, set publicSubs to an empty array
			PublicRegexp[] emptySubs = {};
			publicRegexp.publicSubs = emptySubs;
		}
		return publicRegexp;
	}
	
	
	/**
	 * Enum types of Op (operator), which represents 
	 * the operator type of current node in abstract syntax tree
	 * This enum is identical to Regex.Op
	 * @author zuozhi
	 *
	 */
	public enum PublicOp {
	    NO_MATCH,           // Matches no strings.
	    EMPTY_MATCH,        // Matches empty string.
	    LITERAL,            // Matches runes[] sequence
	    CHAR_CLASS,         // Matches Runes interpreted as range pair list
	    ANY_CHAR_NOT_NL,    // Matches any character except '\n'
	    ANY_CHAR,           // Matches any character
	    BEGIN_LINE,         // Matches empty string at end of line
	    END_LINE,           // Matches empty string at end of line
	    BEGIN_TEXT,         // Matches empty string at beginning of text
	    END_TEXT,           // Matches empty string at end of text
	    WORD_BOUNDARY,      // Matches word boundary `\b`
	    NO_WORD_BOUNDARY,   // Matches word non-boundary `\B`
	    CAPTURE,            // Capturing subexpr with index cap, optional name name
	    STAR,               // Matches subs[0] zero or more times.
	    PLUS,               // Matches subs[0] one or more times.
	    QUEST,              // Matches subs[0] zero or one times.
	    REPEAT,             // Matches subs[0] [min, max] times; max=-1 => no limit.
	    CONCAT,             // Matches concatenation of subs[]
	    ALTERNATE,          // Matches union of subs[]
	    
	    // Pseudo ops, used internally by Parser for parsing stack:
	    // These shouldn't be in the final parse tree
	    LEFT_PAREN,
	    VERTICAL_BAR;
	}
	
	/**
	 * op's enum type Regexp.Op is not public
	 * this converts op to an equivalent enum type, PublicOp
	 * then returns it
	 * 
	 * @return PublicRegex.PublicOp, an enum type representing the operator
	 */
	public PublicOp getOp() {
		try {
			return PublicOp.valueOf(this.op.toString());
		} catch (IllegalArgumentException e) {
			return PublicOp.NO_MATCH;
		}
		
	}
	
	/**
	 * flags is a bitmap of parse flags
	 * @see PublicRE2 for possible flags
	 * @return a bitmap of parse flags
	 */
	public int getFlags() {
		return this.flags;
	}
	
	/**
	 * @return an array of subexpressions with type PublicRegexp
	 */
	public PublicRegexp[] getSubs() {
		return this.publicSubs;
	}
	
	/**
	 * runes: a sequence of characters
	 * it stores information related to literals and character classes
	 * it has different interpretations for different ops
	 * for example,
	 * regex: "[a-z]", runes: [a,z]
	 * 		interpretation: a character class from a to z
	 * regex: "[a-cx-z]", runes: [a,c,x,z]
	 * 		interpretation: a character class from a to c, and from x to z
	 * regex: "cat", runes [c,a,t]
	 * 		interpretation: a literal "cat"
	 * 
	 * @return an array of runes
	 */
	public int[] getRunes() {
		return this.runes;
	}
	
	/**
	 * min and max are used for repetitions
	 * for example,
	 * regex: a{3,5}, min will be 3, max will be 5
	 * @return int indicating minimum number of repetitions
	 */
	public int getMin() {
		return this.min;
	}
	
	/**
	 * min and max are used for repetitions
	 * for example,
	 * regex: a{3,5}, min will be 3, max will be 5
	 * @return int indicating maxinum number of repetitions
	 */
	public int getMax() {
		return this.max;
	}
	
	/**
	 * cap is the capturing index
	 * expressions in () makes it a capture group, 
	 * the entire regex's capturing index is 0, other capturing groups' indexes start from 1
	 * 
	 * for example,
	 * regex: (a)(b)
	 * for "(a)", cap will be 1, for "(b)", cap will be 2
	 * @return int indicating capture index
	 */
	public int getCap() {
		return this.cap;
	}
	
	
	/**
	 * name is capturing group's name (if any)
	 * for example,
	 * regex: (?<name1>a)(?<name2>b)
	 * for "(?<name1>a)", cap name will be name1, for "(?<name2>b)", cap name will be name2
	 * @return int indicating capture index
	 */
	public String getCapName() {
		return this.name;
	}
	
}
