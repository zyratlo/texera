package com.google.re2j;

public class PublicRE2 extends RE2 {

	PublicRE2(String expr) {
		super(expr);
	}
	
	public static int getPERL() {
		return PERL;
	}
}
