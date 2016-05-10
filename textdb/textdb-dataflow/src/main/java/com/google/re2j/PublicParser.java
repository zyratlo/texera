package com.google.re2j;

/**
 * Wrapper class for re2j.Parser
 * @author zuozhi
 *
 */
public class PublicParser{

	/**
	 * parse will call re2j.Parser.parse
	 * it parses a string to an Regexp object, which represents the abstract syntax tree
	 * the Regexp object will be converted to a PublicRegex object before return
	 * @param pattern
	 * @param flags
	 * @return PublicRegexp
	 * @throws PatternSyntaxException
	 */
	public static PublicRegexp parse(String pattern, int flags) throws PatternSyntaxException {
		return PublicRegexp.deepCopy(Parser.parse(pattern, flags));
	}

}
