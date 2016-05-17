package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

public class GramBooleanQueryTest {
	
	@Test
	public void testEmptySameOp() {
		GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
		GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
		Assert.assertTrue(query1.equals(query2));
	}
	
	@Test
	public void testEmptyDifferentOp() {
		GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
		GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		Assert.assertFalse(query1.equals(query2));
	}
	
	@Test
	public void testSameAnd() {
		GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
		GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
		query1.add(Arrays.asList(new String[]{"abc"}));
		query2.add(Arrays.asList(new String[]{"abc"}));
		Assert.assertTrue(query1.equals(query2));
	}
	
	@Test
	public void testSameOr() {
		GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		query1.add(Arrays.asList(new String[]{"abcdef"}));
		query2.add(Arrays.asList(new String[]{"abcdef"}));
		Assert.assertTrue(query1.equals(query2));
	}
	
	@Test
	public void testDifferentAnd() {
		GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
		GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
		query1.add(Arrays.asList(new String[]{"abc"}));
		query2.add(Arrays.asList(new String[]{"pqr"}));
		Assert.assertFalse(query1.equals(query2));
	}
	
	@Test
	public void testSameMultiple() {
		GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		query1.add(Arrays.asList(new String[]{"abc"}));
		query1.add(Arrays.asList(new String[]{"pqr"}));
		query2.add(Arrays.asList(new String[]{"abc"}));
		query2.add(Arrays.asList(new String[]{"pqr"}));
		Assert.assertTrue(query1.equals(query2));
	}
	
	@Test
	public void testDifferentMultiple() {
		GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		query1.add(Arrays.asList(new String[]{"qwe"}));
		query1.add(Arrays.asList(new String[]{"asd"}));
		query2.add(Arrays.asList(new String[]{"zxc"}));
		query2.add(Arrays.asList(new String[]{"vbn"}));
		Assert.assertFalse(query1.equals(query2));
	}
	
	@Test
	public void testSameDifferentOrder() {
		GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		query1.add(Arrays.asList(new String[]{"abc", "pqr"}));
		query2.add(Arrays.asList(new String[]{"pqr", "abc"}));
		Assert.assertTrue(query1.equals(query2));
	}
	
	
	@Test
	public void testSameDifferentOrder2() {
		GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		query1.add(Arrays.asList(new String[]{"asdfg", "poiuy"}));
		query2.add(Arrays.asList(new String[]{"poiuy", "asdfg"}));
		Assert.assertTrue(query1.equals(query2));
	}
		
	@Test
	public void testSameDifferentOrder3() {
		GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		query1.add(Arrays.asList(new String[]{"abcd", "qwer", "zxc"}));
		query2.add(Arrays.asList(new String[]{"zxc", "qwer", "abcd"}));
		Assert.assertTrue(query1.equals(query2));
	}
	
	@Test
	public void testDifferentDifferentOrder() {
		GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		query1.add(Arrays.asList(new String[]{"abcd", "qwer", "zxc"}));
		query2.add(Arrays.asList(new String[]{"abc", "qwe", "zxcv"}));
		Assert.assertFalse(query1.equals(query2));
	}
	
}
