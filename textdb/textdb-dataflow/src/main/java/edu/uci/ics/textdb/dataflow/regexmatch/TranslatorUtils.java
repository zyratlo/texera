package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public class TranslatorUtils {

	static final int MAX_EXACT_SIZE = 7;
	
	// Prefix and suffix sets are limited to maxSet strings.
	// If they get too big, simplify will replace groups of strings
	// sharing a common leading prefix (or trailing suffix) with
	// that common prefix (or suffix).  It is useful for maxSet
	// to be at least 2³ = 8 so that we can exactly
	// represent a case-insensitive abc by the set
	// {abc, abC, aBc, aBC, Abc, AbC, ABc, ABC}.
	static final int MAX_SET_SIZE = 20;
	
	
	@FunctionalInterface
	static interface IFold {
		RegexInfo foldFunc(RegexInfo x, RegexInfo y);
	}
	
	@FunctionalInterface
	static interface IContain {
		boolean containFunc(String str, String affix);
	}

	/**
	 * This function returns the length of the shortest string in {@code strList}.
	 * @param strList
	 * @return minLen
	 */
	static int minLenOfString(List<String> strList) {
		int minLen = Integer.MAX_VALUE;
		
		for (String str: strList) {
			minLen = Math.min(minLen, str.length());
		}
		
		return minLen;
	}

	private static int compareSuffix(String str1, String str2) {
		String str1Reverse = new StringBuilder(str1).reverse().toString();
		String str2Reverse = new StringBuilder(str2).reverse().toString();
		return str1Reverse.compareTo(str2Reverse);
	}

	/**
	 * !!! Should have name "removeDuplicate" !!!
	 * Name it clean for easier debugging
	 * @param strList
	 * @param isSuffix
	 */
	static void clean(List<String> strList, boolean isSuffix) {
		HashSet<String> strSet = new HashSet<String>(strList);
		strList.clear();
		strList.addAll(strSet);
		if (isSuffix) {
	        Collections.sort(strList, (str1, str2) -> compareSuffix(str1, str2));
		} else {
	        Collections.sort(strList, (str1, str2) -> str1.compareTo(str2));
		}
	}

	/**
	 * This function calculates the cartesian product of two string lists (treated as set),
	 * and remove the duplicates in result.
	 * For example, for xList = {"ab", "a"}, yList = {"bc", "c"},
	 * the cartesian product is {"abbc", "abc", "abc", "ac"}.
	 * After removing duplicates, {"abbc", "abc", "ac"} is returned
	 * @param xList
	 * @param yList
	 * @param isSuffix
	 * @return
	 */
	static List<String> cartesianProduct(List<String> xList, List<String> yList, boolean isSuffix) {
		List<String> product = new ArrayList<String>();
		for (String x : xList) {
			for (String y : yList) {
				product.add(x+y);
			}
		}
		clean(product, isSuffix);
		return product;
	}
	
	/**
	 * This function calculates the union of two string lists (treated as set)
	 * and remove the duplicates in result.
	 * For example, for xList = {"ab", "cd"}, yList = {"cd", "ef"},
	 * {"ab", "cd", "ef"} is returned.
	 * @param xList
	 * @param yList
	 * @param isSuffix
	 * @return
	 */
	static List<String> union(List<String> xList, List<String> yList, boolean isSuffix) {
		List<String> unionList = new ArrayList<String>(xList);
		
		unionList.addAll(yList);
		TranslatorUtils.clean(unionList, isSuffix);
		
		return unionList;
	}

}
