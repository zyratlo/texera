package com.google.re2j;

public class PublicParser{

	//TODO return PublicRegexp or Regexp?
	public static PublicRegexp parse(String pattern, int flags) throws PatternSyntaxException {
		return PublicRegexp.deepCopy(Parser.parse(pattern, flags));
	}

}
