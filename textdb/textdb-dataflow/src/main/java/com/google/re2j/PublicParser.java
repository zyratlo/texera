package com.google.re2j;

/**
 * Wrapper class for re2j.Parser
 * @author zuozhi
 *
 */
public class PublicParser{

	/**
	 * parse calls re2j.Parser.parse
	 * it parses a string to a Regexp object, which represents the abstract syntax tree
	 * the Regexp object will be converted to a PublicRegex object before return
	 * @param regex, the regex string to be parsed
	 * @param flags
	 * @see PublicRE2 for possible flags
	 * @return PublicRegexp
	 * @throws PatternSyntaxException
	 */
	public static PublicRegexp parse(String regex, int flags) throws PatternSyntaxException {
		return PublicRegexp.deepCopy(Parser.parse(regex, flags));
	}

}
