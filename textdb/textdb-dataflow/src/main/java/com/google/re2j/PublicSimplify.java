package com.google.re2j;

/**
 * Wrapper class for re2j.Simplify
 * @author zuozhi
 *
 */
public class PublicSimplify{
	
	/**
	 * this method simplifies a given Regexp to an equivalent one,
	 * after applying various simplifications
	 * for example, x{1,2} will be simplified to xx?
	 * @param re
	 * @return
	 */
	public static PublicRegexp simplify(Regexp re) {
		return PublicRegexp.deepCopy(Simplify.simplify(re));
	}

}
