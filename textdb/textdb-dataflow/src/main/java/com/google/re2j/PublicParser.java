package com.google.re2j;

public class PublicParser{


	public static Regexp parse(String pattern, int flags) {
		return Parser.parse(pattern, flags);
	}

}
