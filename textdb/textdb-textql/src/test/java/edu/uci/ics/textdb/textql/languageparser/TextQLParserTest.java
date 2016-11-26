package edu.uci.ics.textdb.textql.languageparser;

import junit.framework.Assert;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;

public class TextQLParserTest {


    @Before
    public void setUp() {
        //TokenMgrError: did not match any token;
        //ParseException: got the wrong kind of token;
    }

    @Test
    public void testNumberLiteral() throws ParseException {
        Assert.assertEquals((new TextQLParser(string2InputStream(" 0 "))).numberLiteral(), "0");
        Assert.assertEquals((new TextQLParser(string2InputStream(" 12 "))).numberLiteral(), "12");
        Assert.assertEquals((new TextQLParser(string2InputStream(" 34566 "))).numberLiteral(), "34566");
        Assert.assertEquals((new TextQLParser(string2InputStream(" 78.90 "))).numberLiteral(), "78.90");
        Assert.assertEquals((new TextQLParser(string2InputStream(" 123. "))).numberLiteral(), "123.");
        Assert.assertEquals((new TextQLParser(string2InputStream(" .456 "))).numberLiteral(), ".456");
        Assert.assertEquals((new TextQLParser(string2InputStream(" -0 "))).numberLiteral(), "-0");
        Assert.assertEquals((new TextQLParser(string2InputStream(" -12 "))).numberLiteral(), "-12");
        Assert.assertEquals((new TextQLParser(string2InputStream(" -34566 "))).numberLiteral(), "-34566");
        Assert.assertEquals((new TextQLParser(string2InputStream(" -78.90 "))).numberLiteral(), "-78.90");
        Assert.assertEquals((new TextQLParser(string2InputStream(" -123. "))).numberLiteral(), "-123.");
        Assert.assertEquals((new TextQLParser(string2InputStream(" -.456 "))).numberLiteral(), "-.456");
        Assert.assertEquals((new TextQLParser(string2InputStream(" -.789.001 "))).numberLiteral(), "-.789");
        assertException(()->(new TextQLParser(string2InputStream(" -e "))).numberLiteral(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" -e 21"))).numberLiteral(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" +4 "))).numberLiteral(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" a "))).numberLiteral(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" a 22 "))).numberLiteral(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" a45 "))).numberLiteral(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" A45 "))).numberLiteral(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" FROM45 "))).numberLiteral(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" \"4\" "))).numberLiteral(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" /4/ "))).numberLiteral(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" /4 "))).numberLiteral(), TokenMgrError.class);
        
    }
    
    @Test
    public void testNumberDouble() throws ParseException {
        Assert.assertEquals((new TextQLParser(string2InputStream(" 0 "))).numberDouble(), 0.);
        Assert.assertEquals((new TextQLParser(string2InputStream(" 12 "))).numberDouble(), 12.);
        Assert.assertEquals((new TextQLParser(string2InputStream(" 34566 "))).numberDouble(), 34566.);
        Assert.assertEquals((new TextQLParser(string2InputStream(" 78.90 "))).numberDouble(), 78.90);
        Assert.assertEquals((new TextQLParser(string2InputStream(" 123. "))).numberDouble(), 123.);
        Assert.assertEquals((new TextQLParser(string2InputStream(" .456 "))).numberDouble(), .456);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -0 "))).numberDouble(), -0.);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -12 "))).numberDouble(), -12.);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -34566 "))).numberDouble(), -34566.);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -78.90 "))).numberDouble(), -78.90);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -123. "))).numberDouble(), -123.);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -.456 "))).numberDouble(), -.456);
        assertException(()->(new TextQLParser(string2InputStream(" -e "))).numberDouble(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" -e 21"))).numberDouble(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" +4 "))).numberDouble(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" a "))).numberDouble(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" a 22 "))).numberDouble(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" a45 "))).numberDouble(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" A45 "))).numberDouble(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" FROM45 "))).numberDouble(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" \"4\" "))).numberDouble(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" /4/ "))).numberDouble(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" /4 "))).numberDouble(), TokenMgrError.class);
    }
    @Test
    public void testNumberInteger() throws ParseException {
        Assert.assertEquals((new TextQLParser(string2InputStream(" 0 "))).numberInteger(), 0);
        Assert.assertEquals((new TextQLParser(string2InputStream(" 12 "))).numberInteger(), 12);
        Assert.assertEquals((new TextQLParser(string2InputStream(" 34566 "))).numberInteger(), 34566);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -0 "))).numberInteger(), 0);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -12 "))).numberInteger(), -12);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -34566 "))).numberInteger(), -34566);
        assertException(()->(new TextQLParser(string2InputStream(" 78.90 "))).numberInteger(), NumberFormatException.class);
        assertException(()->(new TextQLParser(string2InputStream(" 123. "))).numberInteger(), NumberFormatException.class);
        assertException(()->(new TextQLParser(string2InputStream(" .456 "))).numberInteger(), NumberFormatException.class);
        assertException(()->(new TextQLParser(string2InputStream(" -78.90 "))).numberInteger(), NumberFormatException.class);
        assertException(()->(new TextQLParser(string2InputStream(" -123. "))).numberInteger(), NumberFormatException.class);
        assertException(()->(new TextQLParser(string2InputStream(" -.456 "))).numberInteger(), NumberFormatException.class);
        assertException(()->(new TextQLParser(string2InputStream(" -e "))).numberInteger(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" -e 21"))).numberInteger(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" +4 "))).numberInteger(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" a "))).numberInteger(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" a 22 "))).numberInteger(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" a45 "))).numberInteger(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" A45 "))).numberInteger(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" FROM45 "))).numberInteger(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" \"4\" "))).numberInteger(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" /4/ "))).numberInteger(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" /4 "))).numberInteger(), TokenMgrError.class);

    }
    @Test
    public void testRegexLiteral() throws ParseException {
        Assert.assertEquals((new TextQLParser(string2InputStream(" // "))).regexLiteral(), "");
        Assert.assertEquals((new TextQLParser(string2InputStream(" /abc/ "))).regexLiteral(), "abc");
        Assert.assertEquals((new TextQLParser(string2InputStream(" /d\\/e/ "))).regexLiteral(), "d/e");
        Assert.assertEquals((new TextQLParser(string2InputStream(" /d\\/e\\/f/ "))).regexLiteral(), "d/e/f");
        Assert.assertEquals((new TextQLParser(string2InputStream(" /FROM/ "))).regexLiteral(), "FROM");
        Assert.assertEquals((new TextQLParser(string2InputStream(" /\"/ "))).regexLiteral(), "\"");
        Assert.assertEquals((new TextQLParser(string2InputStream(" /\\/ "))).regexLiteral(), "\\");
        assertException(()->(new TextQLParser(string2InputStream(" /21 "))).regexLiteral(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" 2/1/ "))).regexLiteral(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" \"4/ "))).regexLiteral(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" FROM// "))).regexLiteral(), ParseException.class);
    }
    @Test
    public void testStringLiteral() throws ParseException {
        Assert.assertEquals((new TextQLParser(string2InputStream(" \"\" "))).stringLiteral(), "");
        Assert.assertEquals((new TextQLParser(string2InputStream(" \"abc\" "))).stringLiteral(), "abc");
        Assert.assertEquals((new TextQLParser(string2InputStream(" \"de f\" "))).stringLiteral(), "de f");
        Assert.assertEquals((new TextQLParser(string2InputStream(" \"d\\\"e\" "))).stringLiteral(), "d\"e");
        Assert.assertEquals((new TextQLParser(string2InputStream(" \"d\\\"e\\\"f\" "))).stringLiteral(), "d\"e\"f");
        Assert.assertEquals((new TextQLParser(string2InputStream(" \"\\\"\" "))).stringLiteral(), "\"");
        Assert.assertEquals((new TextQLParser(string2InputStream(" \"\\\" "))).stringLiteral(), "\\");
        assertException(()->(new TextQLParser(string2InputStream(" \"21 "))).stringLiteral(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" 'aa' "))).stringLiteral(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" 2\"1\" "))).stringLiteral(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" 21 "))).stringLiteral(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" SELECTa "))).stringLiteral(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" abc "))).stringLiteral(), ParseException.class);
    }
    @Test
    public void testIdentifier() throws ParseException {
        Assert.assertEquals((new TextQLParser(string2InputStream(" i "))).identifier(), "i");
        Assert.assertEquals((new TextQLParser(string2InputStream(" id "))).identifier(), "id");
        Assert.assertEquals((new TextQLParser(string2InputStream(" id de "))).identifier(), "id");
        Assert.assertEquals((new TextQLParser(string2InputStream(" id0 "))).identifier(), "id0");
        Assert.assertEquals((new TextQLParser(string2InputStream(" i6i8s7s "))).identifier(), "i6i8s7s");
        Assert.assertEquals((new TextQLParser(string2InputStream(" j7i\\8s7s "))).identifier(), "j7i");
        Assert.assertEquals((new TextQLParser(string2InputStream(" k8i\"8s7s "))).identifier(), "k8i");
        Assert.assertEquals((new TextQLParser(string2InputStream(" aFROM "))).identifier(), "a");
        assertException(()->(new TextQLParser(string2InputStream(" 2df "))).identifier(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" A "))).identifier(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" FROMa "))).identifier(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" _a "))).identifier(), TokenMgrError.class);
    }
    @Test
    public void testIdentifierList() throws ParseException {
        Assert.assertEquals((new TextQLParser(string2InputStream(" i "))).identifierList(), Arrays.asList("i"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" id "))).identifierList(), Arrays.asList("id"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" id de "))).identifierList(), Arrays.asList("id"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" id,de "))).identifierList(), Arrays.asList("id","de"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" id0 "))).identifierList(), Arrays.asList("id0"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" i6i8s7s "))).identifier(), "i6i8s7s");
        Assert.assertEquals((new TextQLParser(string2InputStream(" i6,i8,s7,s "))).identifierList(), Arrays.asList("i6","i8","s7","s"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" j7i/8s7s/ "))).identifierList(), Arrays.asList("j7i"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" k8i\"8s7s\" "))).identifierList(), Arrays.asList("k8i"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" aFROM "))).identifierList(), Arrays.asList("a"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" b7FROM "))).identifierList(), Arrays.asList("b7"));
        assertException(()->(new TextQLParser(string2InputStream(" j7i,/8s7s/ "))).identifierList(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" k8i,\"8s7s\" "))).identifierList(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" k8i,,k9j "))).identifierList(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" k8i,/8s7s/ "))).identifierList(),ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" k8i, "))).identifierList(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" j7i\\8s7s "))).identifierList(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" k8i\"8s7s "))).identifierList(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" 2df "))).identifierList(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" A "))).identifierList(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" FROMa "))).identifierList(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" _a "))).identifierList(), TokenMgrError.class);
    }
    @Test
    public void testExtractCommand() throws ParseException {
        /*
         * TODO: No point of implementing the test for this rule for now since
         * the only thing it's happening is calling extractKeywordMatchCommand();
         */
    }
    
    @Test
    public void testStatement() throws ParseException {
    	String emptyStatement00 = " ; ";
    	HashMap<String, Object> emptyStatementParameters00 = buildEmptyStatementParameters();
        Assert.assertEquals((new TextQLParser(string2InputStream(emptyStatement00))).statement(), emptyStatementParameters00);
        String selectStatement00 = "SELECT * FROM a;";
    	HashMap<String, Object> selectStatementParameters00 = buildSelectParameters(Arrays.asList(), null, "a", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement00))).statement(), selectStatementParameters00);
        String selectStatement06 = "SELECT f8, fa, fc, df, ff FROM j;";
        HashMap<String, Object> selectStatementParameters06 = buildSelectParameters(Arrays.asList("f8","fa","fc","df","ff"), null, "j", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement06))).statement(), selectStatementParameters06);
    	String selectStatement13 = "SELECT h, i, j EXTRACT KEYWORDMATCH([h6,h7,k8,k9], \"key5\") FROM q;";
        HashMap<String, Object> selectStatementParameters13 = buildSelectParameters(Arrays.asList("h","i","j"), buildKeywordMatchParameters(Arrays.asList("h6","h7","k8","k9"), "key5", null), "q", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement13))).statement(), selectStatementParameters13);
        String selectStatement14 = "EXTRACT KEYWORDMATCH([i6,j7,l8,m9], \"key5\") FROM q;";
        HashMap<String, Object> selectStatementParameters14 = buildSelectParameters(null, buildKeywordMatchParameters(Arrays.asList("i6","j7","l8","m9"), "key5", null), "q", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement14))).statement(), selectStatementParameters14);
        String selectStatement21 = "EXTRACT KEYWORDMATCH([h3,i2,j1,k0], \"key\\\"/\") FROM m LIMIT 4 OFFSET 25 ;";
        HashMap<String, Object> selectStatementParameters21 = buildSelectParameters(null, buildKeywordMatchParameters(Arrays.asList("h3","i2","j1","k0"), "key\"/", null), "m", 4, 25);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement21))).statement(), selectStatementParameters21);
    	String createViewStatement00 = " CREATE VIEW v0 AS SELECT * FROM a; ";
    	HashMap<String, Object> createViewStatementParameters00 = buildViewParameters("v0", buildSelectParameters("__lid0", Arrays.asList(), null, "a", null, null));
    	Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement00))).statement(), createViewStatementParameters00);
        String createViewStatement01 = " CREATE VIEW v1 AS SELECT f8, fa, fc, df, ff FROM j LIMIT 1 OFFSET 8; ";
    	HashMap<String, Object> createViewStatementParameters01 = buildViewParameters("v1", buildSelectParameters("__lid0", Arrays.asList("f8","fa","fc","df","ff"), null, "j", 1, 8));
    	Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement01))).statement(), createViewStatementParameters01);
        String createViewStatement02 = " CREATE VIEW v2 AS SELECT e EXTRACT KEYWORDMATCH([g4,g5], \"key0\") FROM o ;";
    	HashMap<String, Object> createViewStatementParameters02 = buildViewParameters("v2", buildSelectParameters("__lid0", Arrays.asList("e"), buildKeywordMatchParameters(Arrays.asList("g4","g5"), "key0", null), "o", null, null));
    	Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement02))).statement(), createViewStatementParameters02);
        String createViewStatement03 = " CREATE VIEW v2 AS EXTRACT KEYWORDMATCH([g4,g5], \"key0\", substring) FROM o ;";
    	HashMap<String, Object> createViewStatementParameters03 = buildViewParameters("v2", buildSelectParameters("__lid0", null, buildKeywordMatchParameters(Arrays.asList("g4","g5"), "key0", "substring"), "o", null, null));
    	Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement03))).statement(), createViewStatementParameters03);
        String createViewStatement04 = " CREATE VIEW v3 AS CREATE VIEW v4 AS SELECT * FROM a LIMIT 1 OFFSET 2;";
    	HashMap<String, Object> createViewStatementParameters04 = buildViewParameters("v3", buildViewParameters("v4", buildSelectParameters("__lid0", Arrays.asList(), null, "a", 1, 2)));
    	Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement04))).statement(), createViewStatementParameters04);
    }
    @Test
    public void testStatementsMain() throws ParseException {
        /*
    	 * TODO: create some test cases with multiple statements
         */
    }
    private HashMap<String, Object> buildEmptyStatementParameters(){
    	return buildEmptyStatementParameters("__lid0");
    }
    private HashMap<String, Object> buildEmptyStatementParameters(String name){
    	return new HashMap<String, Object>() {{
            put("statementType", "empty");
            put("statementName", name);
        }};
    }  
    @Test
    public void testEmptyStatement() throws ParseException {
    	Map<String, Object> emptyStatementParameters = buildEmptyStatementParameters();
        Assert.assertEquals((new TextQLParser(string2InputStream(" "))).emptyStatement(), emptyStatementParameters);
        Assert.assertEquals((new TextQLParser(string2InputStream(" ; \nSELECT "))).emptyStatement(), emptyStatementParameters);
        Assert.assertEquals((new TextQLParser(string2InputStream(" SELECT * FROM a; "))).emptyStatement(), emptyStatementParameters);
        Assert.assertEquals((new TextQLParser(string2InputStream(" k8i, "))).emptyStatement(), emptyStatementParameters);
        Assert.assertEquals((new TextQLParser(string2InputStream(" 2df ; "))).emptyStatement(), emptyStatementParameters);
        Assert.assertEquals((new TextQLParser(string2InputStream(" FROMa; "))).emptyStatement(), emptyStatementParameters);
        Assert.assertEquals((new TextQLParser(string2InputStream(" a; "))).emptyStatement(), emptyStatementParameters);
        Assert.assertEquals((new TextQLParser(string2InputStream(" A; "))).emptyStatement(), emptyStatementParameters);
        Assert.assertEquals((new TextQLParser(string2InputStream(" _a; "))).emptyStatement(), emptyStatementParameters);
    }
    private HashMap<String, Object> buildSelectParameters(List<String> projectFields, Map<String,Object> extract, String from, Integer limit, Integer offset){
    	return buildSelectParameters("__lid0", projectFields, extract, from, limit, offset);
    }
    private HashMap<String, Object> buildSelectParameters(String name, List<String> projectFields, Map<String,Object> extract, String from, Integer limit, Integer offset){
    	return new HashMap<String, Object>() {{
            put("statementType", "select");
            put("statementName", name);
            put("projectAll", projectFields!=null&&projectFields.size()==0?true:null);
            put("projectFields", projectFields!=null&&projectFields.size()>0?projectFields:null);
            put("extractCommand", extract);
            put("from", from);
            put("limit", limit);
            put("offset", offset);
        }};
    }  
    @Test
    public void testSelectStatement() throws ParseException {
    	String selectStatement00 = "SELECT * FROM a";
    	HashMap<String, Object> selectStatementParameters00 = buildSelectParameters(Arrays.asList(), null, "a", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement00))).selectStatement(), selectStatementParameters00);
        String selectStatement01 = "SELECT * FROM b LIMIT 5";
        HashMap<String, Object> selectStatementParameters01 = buildSelectParameters(Arrays.asList(), null, "b", 5, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement01))).selectStatement(), selectStatementParameters01);
        String selectStatement02 = "SELECT * FROM c LIMIT 1 OFFSET 8";
        HashMap<String, Object> selectStatementParameters02 = buildSelectParameters(Arrays.asList(), null, "c", 1, 8);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement02))).selectStatement(), selectStatementParameters02);
        String selectStatement03 = "SELECT * FROM d OFFSET 6";
        HashMap<String, Object> selectStatementParameters03 = buildSelectParameters(Arrays.asList(), null, "d", null, 6);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement03))).selectStatement(), selectStatementParameters03);
        String selectStatement04 = "SELECT f1 FROM e";
        HashMap<String, Object> selectStatementParameters04 = buildSelectParameters(Arrays.asList("f1"), null, "e", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement04))).selectStatement(), selectStatementParameters04);
        String selectStatement05 = "SELECT f1, f5 FROM i";
        HashMap<String, Object> selectStatementParameters05 = buildSelectParameters(Arrays.asList("f1","f5"), null, "i", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement05))).selectStatement(), selectStatementParameters05);
        String selectStatement06 = "SELECT f8, fa, fc, df, ff FROM j";
        HashMap<String, Object> selectStatementParameters06 = buildSelectParameters(Arrays.asList("f8","fa","fc","df","ff"), null, "j", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement06))).selectStatement(), selectStatementParameters06);
        String selectStatement07 = "SELECT a EXTRACT KEYWORDMATCH(g0, \"key1\") FROM k";
        HashMap<String, Object> selectStatementParameters07 = buildSelectParameters(Arrays.asList("a"), buildKeywordMatchParameters(Arrays.asList("g0"), "key1", null), "k", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement07))).selectStatement(), selectStatementParameters07);
        String selectStatement08 = "SELECT b EXTRACT KEYWORDMATCH(g1, \"key2\", conjunction) FROM l";
        HashMap<String, Object> selectStatementParameters08 = buildSelectParameters(Arrays.asList("b"), buildKeywordMatchParameters(Arrays.asList("g1"), "key2", "conjunction"), "l", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement08))).selectStatement(), selectStatementParameters08);
    	String selectStatement10 = "SELECT v EXTRACT KEYWORDMATCH(u, \"keyZ\") FROM t";
        HashMap<String, Object> selectStatementParameters10 = buildSelectParameters(Arrays.asList("v"), buildKeywordMatchParameters(Arrays.asList("u"), "keyZ", null), "t", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement10))).selectStatement(), selectStatementParameters10);
    	String selectStatement11 = "SELECT e EXTRACT KEYWORDMATCH([g4], \"key0\") FROM o";
        HashMap<String, Object> selectStatementParameters11 = buildSelectParameters(Arrays.asList("e"), buildKeywordMatchParameters(Arrays.asList("g4"), "key0", null), "o", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement11))).selectStatement(), selectStatementParameters11);
        String selectStatement12 = "SELECT f EXTRACT KEYWORDMATCH([g6,g7,h8,i9], \"key\") FROM p";
        HashMap<String, Object> selectStatementParameters12 = buildSelectParameters(Arrays.asList("f"), buildKeywordMatchParameters(Arrays.asList("g6","g7","h8","i9"), "key", null), "p", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement12))).selectStatement(), selectStatementParameters12);
        String selectStatement13 = "SELECT h, i, j EXTRACT KEYWORDMATCH([h6,h7,k8,k9], \"key5\") FROM q";
        HashMap<String, Object> selectStatementParameters13 = buildSelectParameters(Arrays.asList("h","i","j"), buildKeywordMatchParameters(Arrays.asList("h6","h7","k8","k9"), "key5", null), "q", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement13))).selectStatement(), selectStatementParameters13);
        String selectStatement14 = "EXTRACT KEYWORDMATCH([i6,j7,l8,m9], \"key5\") FROM q";
        HashMap<String, Object> selectStatementParameters14 = buildSelectParameters(null, buildKeywordMatchParameters(Arrays.asList("i6","j7","l8","m9"), "key5", null), "q", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement14))).selectStatement(), selectStatementParameters14);
        String selectStatement15 = "EXTRACT KEYWORDMATCH(g0, \"key1\") FROM k";
        HashMap<String, Object> selectStatementParameters15 = buildSelectParameters(null, buildKeywordMatchParameters(Arrays.asList("g0"), "key1", null), "k", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement15))).selectStatement(), selectStatementParameters15);
        String selectStatement16 = "EXTRACT KEYWORDMATCH(g1, \"key2\", phrase) FROM l";
        HashMap<String, Object> selectStatementParameters16 = buildSelectParameters(null, buildKeywordMatchParameters(Arrays.asList("g1"), "key2", "phrase"), "l", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement16))).selectStatement(), selectStatementParameters16);
        String selectStatement19 = "EXTRACT KEYWORDMATCH([g4], \"key0\") FROM o";
        HashMap<String, Object> selectStatementParameters19 = buildSelectParameters(null, buildKeywordMatchParameters(Arrays.asList("g4"), "key0", null), "o", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement19))).selectStatement(), selectStatementParameters19);
    	String selectStatement20 = "EXTRACT KEYWORDMATCH([g6,g7,h8,i9], \"key\") FROM p";
        HashMap<String, Object> selectStatementParameters20 = buildSelectParameters(null, buildKeywordMatchParameters(Arrays.asList("g6","g7","h8","i9"), "key", null), "p", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement20))).selectStatement(), selectStatementParameters20);
    	String selectStatement21 = "EXTRACT KEYWORDMATCH([h3,i2,j1,k0], \"key\\\"/\") FROM m LIMIT 4 OFFSET 25 ";
        HashMap<String, Object> selectStatementParameters21 = buildSelectParameters(null, buildKeywordMatchParameters(Arrays.asList("h3","i2","j1","k0"), "key\"/", null), "m", 4, 25);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement21))).selectStatement(), selectStatementParameters21);
    	String selectStatement22 = "SELECT FROM a";
    	assertException(()->(new TextQLParser(string2InputStream(selectStatement22))).selectStatement(), ParseException.class);
    	String selectStatement23 = "SELECT FROM a OFFSET 5 LIMIT 6";
    	assertException(()->(new TextQLParser(string2InputStream(selectStatement23))).selectStatement(), ParseException.class);
    	String selectStatement24 = "SELECT 25 FROM a";
    	assertException(()->(new TextQLParser(string2InputStream(selectStatement24))).selectStatement(), ParseException.class);
    	String selectStatement25 = "SELECT [a,b] FROM a";
        assertException(()->(new TextQLParser(string2InputStream(selectStatement25))).selectStatement(), ParseException.class);
        String selectStatement26 = "SELECT *,a FROM a";
        assertException(()->(new TextQLParser(string2InputStream(selectStatement26))).selectStatement(), ParseException.class);
        String selectStatement27 = "SELECT * FROM [a,b]";
        assertException(()->(new TextQLParser(string2InputStream(selectStatement27))).selectStatement(), ParseException.class);
        String selectStatement28 = "SELECT KEYWORDMATCH(g0, \"key1\") FROM a";
        assertException(()->(new TextQLParser(string2InputStream(selectStatement28))).selectStatement(), ParseException.class);
        String selectStatement29 = "SELECT EXTRACT KEYWORDMATCH(g0, \"key1\") FROM a";
        assertException(()->(new TextQLParser(string2InputStream(selectStatement29))).selectStatement(), ParseException.class);
        String selectStatement30 = "EXTRACT a FROM a";
        assertException(()->(new TextQLParser(string2InputStream(selectStatement30))).selectStatement(), ParseException.class);
        String selectStatement31 = "EXTRACT * FROM a";
        assertException(()->(new TextQLParser(string2InputStream(selectStatement31))).selectStatement(), ParseException.class);
        String selectStatement32 = "EXTRACT KEYWORDMATCH(g0, \"key1\") SELECT a FROM k";
        assertException(()->(new TextQLParser(string2InputStream(selectStatement32))).selectStatement(), ParseException.class);
        String selectStatement33 = "SELECT a";
        assertException(()->(new TextQLParser(string2InputStream(selectStatement33))).selectStatement(), ParseException.class);
    }
    private HashMap<String, Object> buildKeywordMatchParameters(List<String> matchFields, String keyword, String matchType){
        return new HashMap<String, Object>() {{
            put("extractType", "keyword");
            put("matchFields", matchFields);
            put("keyword", keyword);
            put("matchType", matchType);
        }};
    }    
    @Test
    public void testExtractKeywordMatchCommand() throws ParseException {
    	String keywordMatchCommand00 = " KEYWORDMATCH(g0, \"key1\") ";
        HashMap<String, Object> keywordMatchParameters00 = buildKeywordMatchParameters(Arrays.asList("g0"), "key1", null);
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchCommand00))).extractKeywordMatchCommand(), keywordMatchParameters00);
        String keywordMatchCommand01 = " KEYWORDMATCH(g1, \"key2\", conjunction) ";
        HashMap<String, Object> keywordMatchParameters01 = buildKeywordMatchParameters(Arrays.asList("g1"), "key2", "conjunction");
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchCommand01))).extractKeywordMatchCommand(), keywordMatchParameters01);
        String keywordMatchCommand02 = " KEYWORDMATCH(g2, \"key3\", phrase) ";
        HashMap<String, Object> keywordMatchParameters02 = buildKeywordMatchParameters(Arrays.asList("g2"), "key3", "phrase");
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchCommand02))).extractKeywordMatchCommand(), keywordMatchParameters02);
        String keywordMatchCommand03 = " KEYWORDMATCH(g3, \"key4\", substring) ";
        HashMap<String, Object> keywordMatchParameters03 = buildKeywordMatchParameters(Arrays.asList("g3"), "key4", "substring");
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchCommand03))).extractKeywordMatchCommand(), keywordMatchParameters03);
        String keywordMatchCommand04 = " KEYWORDMATCH([g4], \"key0\") ";
        HashMap<String, Object> keywordMatchParameters04 = buildKeywordMatchParameters(Arrays.asList("g4"), "key0", null);
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchCommand04))).extractKeywordMatchCommand(), keywordMatchParameters04);
        String keywordMatchCommand05 = " KEYWORDMATCH([g4,g5], \"key0\") ";
        HashMap<String, Object> keywordMatchParameters05 = buildKeywordMatchParameters(Arrays.asList("g4","g5"), "key0", null);
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchCommand05))).extractKeywordMatchCommand(), keywordMatchParameters05);
        String keywordMatchCommand06 = " KEYWORDMATCH([g6,g7,h8,i9], \"key\") ";
        HashMap<String, Object> keywordMatchParameters06 = buildKeywordMatchParameters(Arrays.asList("g6","g7","h8","i9"), "key", null);
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchCommand06))).extractKeywordMatchCommand(), keywordMatchParameters06);
    	String keywordMatchCommand07 = " KEYWORDMATCH (\"key1\") ";
    	assertException(()->(new TextQLParser(string2InputStream(keywordMatchCommand07))).extractKeywordMatchCommand(), ParseException.class);
        String keywordMatchCommand08 = " KEYWORDMATCH ([i6,j7,l8,m9, \"key5\") ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchCommand08))).extractKeywordMatchCommand(), ParseException.class);
        String keywordMatchCommand09 = " KEYWORDMATCH (i6,j7,l8,m9, \"key5\") ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchCommand09))).extractKeywordMatchCommand(), ParseException.class);
        String keywordMatchCommand10 = " KEYWORDMATCH (i6,j7,l8,m9], \"key5\") ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchCommand10))).extractKeywordMatchCommand(), ParseException.class);
        String keywordMatchCommand11 = " KEYWORDMATCH ([i6,j7,l8,m9, \"key5\", conjunction) ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchCommand11))).extractKeywordMatchCommand(), ParseException.class);
        String keywordMatchCommand12 = " KEYWORDMATCH (i6,j7,l8,m9, \"key5\", substring) ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchCommand12))).extractKeywordMatchCommand(), ParseException.class);
        String keywordMatchCommand13 = " KEYWORDMATCH ([i6,j7,l8,m9, \"key5\", phrase) ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchCommand13))).extractKeywordMatchCommand(), ParseException.class);
        String keywordMatchCommand14 = " KEYWORDMATCH ([], key5) ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchCommand14))).extractKeywordMatchCommand(), ParseException.class);
        String keywordMatchCommand15 = " KEYWORDMATCH ([a], key5) ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchCommand15))).extractKeywordMatchCommand(), ParseException.class);
        String keywordMatchCommand16 = " KEYWORDMATCH ([a]) ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchCommand16))).extractKeywordMatchCommand(), ParseException.class);
        
    }
    private HashMap<String, Object> buildViewParameters(String viewName, HashMap<String, Object> substatement){
        return new HashMap<String, Object>() {{
            put("statementType", "view");
            put("statementName", viewName);
            put("substatement", substatement);
        }};
    }  
    @Test
    public void createViewStatement() throws ParseException {
    	String createViewStatement00 = " CREATE VIEW v0 AS SELECT * FROM a ";
    	HashMap<String, Object> createViewStatementParameters00 = buildViewParameters("v0", buildSelectParameters("__lid0", Arrays.asList(), null, "a", null, null));
    	Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement00))).createViewStatement(), createViewStatementParameters00);
        String createViewStatement01 = " CREATE VIEW v1 AS SELECT f8, fa, fc, df, ff FROM j LIMIT 1 OFFSET 8 ";
    	HashMap<String, Object> createViewStatementParameters01 = buildViewParameters("v1", buildSelectParameters("__lid0", Arrays.asList("f8","fa","fc","df","ff"), null, "j", 1, 8));
    	Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement01))).createViewStatement(), createViewStatementParameters01);
        String createViewStatement02 = " CREATE VIEW v2 AS SELECT e EXTRACT KEYWORDMATCH([g4,g5], \"key0\") FROM o ";
    	HashMap<String, Object> createViewStatementParameters02 = buildViewParameters("v2", buildSelectParameters("__lid0", Arrays.asList("e"), buildKeywordMatchParameters(Arrays.asList("g4","g5"), "key0", null), "o", null, null));
    	Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement02))).createViewStatement(), createViewStatementParameters02);
        String createViewStatement03 = " CREATE VIEW v2 AS EXTRACT KEYWORDMATCH([g4,g5], \"key0\", substring) FROM o ";
    	HashMap<String, Object> createViewStatementParameters03 = buildViewParameters("v2", buildSelectParameters("__lid0", null, buildKeywordMatchParameters(Arrays.asList("g4","g5"), "key0", "substring"), "o", null, null));
    	Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement03))).createViewStatement(), createViewStatementParameters03);
        String createViewStatement04 = " CREATE VIEW v3 AS CREATE VIEW v4 AS SELECT * FROM a LIMIT 1 OFFSET 2";
    	HashMap<String, Object> createViewStatementParameters04 = buildViewParameters("v3", buildViewParameters("v4", buildSelectParameters("__lid0", Arrays.asList(), null, "a", 1, 2)));
    	Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement04))).createViewStatement(), createViewStatementParameters04);
    	String createViewStatement05 = " CREATE VIEW v0 AS ";
        assertException(()->(new TextQLParser(string2InputStream(createViewStatement05))).createViewStatement(), ParseException.class);
    	String createViewStatement06 = " CREATE VIEW v0 ";
        assertException(()->(new TextQLParser(string2InputStream(createViewStatement06))).createViewStatement(), ParseException.class);
    	String createViewStatement08 = " CREATE v0 AS SELECT * FROM a ";
        assertException(()->(new TextQLParser(string2InputStream(createViewStatement08))).createViewStatement(), ParseException.class);
    	String createViewStatement09 = " VIEW v0 AS SELECT * FROM a ";
        assertException(()->(new TextQLParser(string2InputStream(createViewStatement09))).createViewStatement(), ParseException.class);
    }
    
    
    
    private void assertException(Callable<Object> test, Class<?> expectedThrowable){
    	try{
    		test.call();//run the code
    		Assert.fail("Callable did not trow a " + expectedThrowable.getName());//if the call didn't throw an exception that's an error
    	}catch(Throwable thrown){
    		//Check if got the right kind of exception
    		if(!(thrown.getClass().equals(expectedThrowable))){
    			Assert.fail("Callable has trown a " + thrown.getClass().getName() + " instead of " + expectedThrowable.getName());//not the right kind of exception
    		}
    	}
    }

    private InputStream string2InputStream(String s){
		try {
			PipedOutputStream pos = new PipedOutputStream();
			PipedInputStream pis = new PipedInputStream(pos);
			PrintStream ppos = new PrintStream(pos);
			ppos.print(s);
			ppos.close();
			return pis;
		} catch (IOException e) {
			return null;
		}
    }
    
}
