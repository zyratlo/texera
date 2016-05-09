package com.google.re2j;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PublicRegexp extends Regexp {

	public PublicRegexp(Regexp that) {
		super(that);
	}
	
	public String getOp() {
		return this.op.toString();
	}
	
	public int getFlags() {
		return this.flags;
	}
	
	public PublicRegexp[] getSubs() {
		Stream<PublicRegexp> publicSubStream = 
				Arrays.asList(this.subs).stream()
				.map(sub -> new PublicRegexp(sub));
		PublicRegexp[] publicSubs = new PublicRegexp[this.subs.length];
		return publicSubStream.collect(Collectors.toList()).toArray(publicSubs);
	}
	
	
	public int[] getRunes() {
		return this.runes;
	}
	
	public int getMin() {
		return this.min;
	}
	
	public int getMax() {
		return this.max;
	}
	
	public int getCap() {
		return this.cap;
	}
	
	public String getCapName() {
		return this.name;
	}
	  
	
	
	



}
