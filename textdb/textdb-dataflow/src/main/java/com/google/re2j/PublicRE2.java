package com.google.re2j;

/**
 * PublicRE2 includes flags from RE2 class
 * comments about flags are copied directly from RE2.java
 * 
 * A commonly needed flag is PublicRE2.PERL, which tells RE2 to use Perl syntax
 * 
 * @author zuozhi
 *
 */
public class PublicRE2 {

	// Fold case during matching (case-insensitive).
	public static final int FOLD_CASE 		= RE2.FOLD_CASE;

	// Treat pattern as a literal string instead of a regexp.
	public static final int LITERAL 		= RE2.LITERAL;

	// Allow character classes like [^a-z] and [[:space:]] to match newline.
	public static final int CLASS_NL 		= RE2.CLASS_NL;

	// Allow '.' to match newline.
	public static final int DOT_NL 			= RE2.DOT_NL;

	// Treat ^ and $ as only matching at beginning and end of text, not
	// around embedded newlines. (Perl's default).
	public static final int ONE_LINE 		= RE2.ONE_LINE;

	// Make repetition operators default to non-greedy.
	public static final int NON_GREEDY 		= RE2.NON_GREEDY;

	// allow Perl extensions:
	// non-capturing parens - (?: )
	// non-greedy operators - *? +? ?? {}?
	// flag edits - (?i) (?-i) (?i: )
	// i - FoldCase
	// m - !OneLine
	// s - DotNL
	// U - NonGreedy
	// line ends: \A \z
	// \Q and \E to disable/enable metacharacters
	// (?P<name>expr) for named captures
	// \C (any byte) is not supported.
	public static final int PERL_X 			= RE2.PERL_X;

	// Allow \p{Han}, \P{Han} for Unicode group and negation.
	public static final int UNICODE_GROUPS 	= RE2.UNICODE_GROUPS;

	// Regexp END_TEXT was $, not \z. Internal use only.
	public static final int WAS_DOLLAR 		= RE2.WAS_DOLLAR;

	public static final int MATCH_NL 		= RE2.CLASS_NL | RE2.DOT_NL;

	// As close to Perl as possible.
	public static final int PERL 			= RE2.CLASS_NL | RE2.ONE_LINE | RE2.PERL_X | RE2.UNICODE_GROUPS;

	// POSIX syntax.
	public static final int POSIX 			= RE2.POSIX;

	// // Anchors
	public static final int UNANCHORED 		= RE2.UNANCHORED;
	public static final int ANCHOR_START 	= RE2.ANCHOR_START;
	public static final int ANCHOR_BOTH 	= RE2.ANCHOR_BOTH;

}
