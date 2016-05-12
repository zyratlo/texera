package com.google.re2j;

import java.util.Arrays;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RE2JWrapperTest {
	
	private boolean subExprEqual(Regexp[] originalSubs, PublicRegexp[] compareSubs) {
		if (originalSubs == null && compareSubs == null) {
			return true;
		} else if (originalSubs == null || compareSubs == null) {
			return false;
		} else {
			if (originalSubs.length != compareSubs.length) {
				return false;
			} else {
				boolean equivalent = true;
				for (int i = 0; i < originalSubs.length; i++) {
					equivalent = equivalent && isEquivalent(originalSubs[i], compareSubs[i]);
				}
				return equivalent;
			}
		}
	}
	
	private boolean isEquivalent(Regexp original, PublicRegexp compare) {
		boolean equivalent = 
				(original.cap	== compare.cap) &&
				(original.flags	== compare.flags) &&
				(original.max	== compare.max) &&
				(original.min	== compare.min) &&
				(original.op	== compare.op) &&
				(original.name == null ? compare.name == null : original.name.equals(compare.name)) &&
				(Arrays.equals(original.runes, compare.runes)) &&
				(subExprEqual(original.subs, compare.publicSubs));	
		
		return equivalent;
	}
	
	private void testRE2Wrapper(String regex) {
		// test parse
		Regexp original = Parser.parse(regex, RE2.PERL);
		PublicRegexp compare = PublicParser.parse(regex, PublicRE2.PERL);
		Assert.assertTrue(isEquivalent(original, compare));
		// test simplify
		original = Simplify.simplify(original);
		compare = PublicSimplify.simplify(compare);
		Assert.assertTrue(isEquivalent(original, compare));

	}
	
	@Test
	public void testLiteral() {
		testRE2Wrapper("cat");
		testRE2Wrapper("dog");
	}
	
	@Test
	public void testAlternate() {
		testRE2Wrapper("a|b|c");
	}
	
	@Test
	public void testStar() {
		testRE2Wrapper("a*");
	}
	
	@Test
	public void testPlus() {
		testRE2Wrapper("a+");
	}
	
	@Test
	public void testCharClass() {
		testRE2Wrapper("[a-z]");
	}
	
	@Test
	public void testComplex1() {
		testRE2Wrapper("data*[bcd|pqr]");
	}


}
