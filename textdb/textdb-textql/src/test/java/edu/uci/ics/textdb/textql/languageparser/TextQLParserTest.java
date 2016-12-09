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
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.textql.languageparser.TextQLParser.Statement;
import edu.uci.ics.textdb.textql.languageparser.TextQLParser.SelectStatement;
import edu.uci.ics.textdb.textql.languageparser.TextQLParser.CreateViewStatement;
import edu.uci.ics.textdb.textql.languageparser.TextQLParser.ExtractPredicate;
import edu.uci.ics.textdb.textql.languageparser.TextQLParser.KeywordExtractPredicate;

public class TextQLParserTest {


    @Before
    public void setUp() {
        //TokenMgrError: did not match any token;
        //ParseException: got the wrong kind of token;
    }

    @Test
    public void testNumberLiteral() throws ParseException {
        Assert.assertEquals((new TextQLParser(string2InputStream(" 0 "))).numberLiteralToString(), "0");
        Assert.assertEquals((new TextQLParser(string2InputStream(" 12 "))).numberLiteralToString(), "12");
        Assert.assertEquals((new TextQLParser(string2InputStream(" 34566 "))).numberLiteralToString(), "34566");
        Assert.assertEquals((new TextQLParser(string2InputStream(" 78.90 "))).numberLiteralToString(), "78.90");
        Assert.assertEquals((new TextQLParser(string2InputStream(" 123. "))).numberLiteralToString(), "123.");
        Assert.assertEquals((new TextQLParser(string2InputStream(" .456 "))).numberLiteralToString(), ".456");
        Assert.assertEquals((new TextQLParser(string2InputStream(" -0 "))).numberLiteralToString(), "-0");
        Assert.assertEquals((new TextQLParser(string2InputStream(" -12 "))).numberLiteralToString(), "-12");
        Assert.assertEquals((new TextQLParser(string2InputStream(" -34566 "))).numberLiteralToString(), "-34566");
        Assert.assertEquals((new TextQLParser(string2InputStream(" -78.90 "))).numberLiteralToString(), "-78.90");
        Assert.assertEquals((new TextQLParser(string2InputStream(" -123. "))).numberLiteralToString(), "-123.");
        Assert.assertEquals((new TextQLParser(string2InputStream(" -.456 "))).numberLiteralToString(), "-.456");
        Assert.assertEquals((new TextQLParser(string2InputStream(" -.789.001 "))).numberLiteralToString(), "-.789");
        assertException(()->(new TextQLParser(string2InputStream(" -e "))).numberLiteralToString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" -e 21"))).numberLiteralToString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" +4 "))).numberLiteralToString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" a "))).numberLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" a 22 "))).numberLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" a45 "))).numberLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" A45 "))).numberLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" FROM45 "))).numberLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" \"4\" "))).numberLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" /4/ "))).numberLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" /4 "))).numberLiteralToString(), TokenMgrError.class);
        
    }
    
    @Test
    public void testNumberDouble() throws ParseException {
        Assert.assertEquals((new TextQLParser(string2InputStream(" 0 "))).numberLiteralToDouble(), 0.);
        Assert.assertEquals((new TextQLParser(string2InputStream(" 12 "))).numberLiteralToDouble(), 12.);
        Assert.assertEquals((new TextQLParser(string2InputStream(" 34566 "))).numberLiteralToDouble(), 34566.);
        Assert.assertEquals((new TextQLParser(string2InputStream(" 78.90 "))).numberLiteralToDouble(), 78.90);
        Assert.assertEquals((new TextQLParser(string2InputStream(" 123. "))).numberLiteralToDouble(), 123.);
        Assert.assertEquals((new TextQLParser(string2InputStream(" .456 "))).numberLiteralToDouble(), .456);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -0 "))).numberLiteralToDouble(), -0.);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -12 "))).numberLiteralToDouble(), -12.);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -34566 "))).numberLiteralToDouble(), -34566.);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -78.90 "))).numberLiteralToDouble(), -78.90);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -123. "))).numberLiteralToDouble(), -123.);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -.456 "))).numberLiteralToDouble(), -.456);
        assertException(()->(new TextQLParser(string2InputStream(" -e "))).numberLiteralToDouble(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" -e 21"))).numberLiteralToDouble(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" +4 "))).numberLiteralToDouble(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" a "))).numberLiteralToDouble(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" a 22 "))).numberLiteralToDouble(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" a45 "))).numberLiteralToDouble(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" A45 "))).numberLiteralToDouble(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" FROM45 "))).numberLiteralToDouble(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" \"4\" "))).numberLiteralToDouble(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" /4/ "))).numberLiteralToDouble(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" /4 "))).numberLiteralToDouble(), TokenMgrError.class);
    }
    @Test
    public void testNumberInteger() throws ParseException {
        Assert.assertEquals((new TextQLParser(string2InputStream(" 0 "))).numberLiteralToInteger(), 0);
        Assert.assertEquals((new TextQLParser(string2InputStream(" 12 "))).numberLiteralToInteger(), 12);
        Assert.assertEquals((new TextQLParser(string2InputStream(" 34566 "))).numberLiteralToInteger(), 34566);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -0 "))).numberLiteralToInteger(), 0);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -12 "))).numberLiteralToInteger(), -12);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -34566 "))).numberLiteralToInteger(), -34566);
        assertException(()->(new TextQLParser(string2InputStream(" 78.90 "))).numberLiteralToInteger(), NumberFormatException.class);
        assertException(()->(new TextQLParser(string2InputStream(" 123. "))).numberLiteralToInteger(), NumberFormatException.class);
        assertException(()->(new TextQLParser(string2InputStream(" .456 "))).numberLiteralToInteger(), NumberFormatException.class);
        assertException(()->(new TextQLParser(string2InputStream(" -78.90 "))).numberLiteralToInteger(), NumberFormatException.class);
        assertException(()->(new TextQLParser(string2InputStream(" -123. "))).numberLiteralToInteger(), NumberFormatException.class);
        assertException(()->(new TextQLParser(string2InputStream(" -.456 "))).numberLiteralToInteger(), NumberFormatException.class);
        assertException(()->(new TextQLParser(string2InputStream(" -e "))).numberLiteralToInteger(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" -e 21"))).numberLiteralToInteger(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" +4 "))).numberLiteralToInteger(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" a "))).numberLiteralToInteger(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" a 22 "))).numberLiteralToInteger(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" a45 "))).numberLiteralToInteger(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" A45 "))).numberLiteralToInteger(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" FROM45 "))).numberLiteralToInteger(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" \"4\" "))).numberLiteralToInteger(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" /4/ "))).numberLiteralToInteger(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" /4 "))).numberLiteralToInteger(), TokenMgrError.class);

    }
    @Test
    public void testregexLiteralToString() throws ParseException {
        Assert.assertEquals((new TextQLParser(string2InputStream(" // "))).regexLiteralToString(), "");
        Assert.assertEquals((new TextQLParser(string2InputStream(" /abc/ "))).regexLiteralToString(), "abc");
        Assert.assertEquals((new TextQLParser(string2InputStream(" /d\\/e/ "))).regexLiteralToString(), "d/e");
        Assert.assertEquals((new TextQLParser(string2InputStream(" /d\\/e\\/f/ "))).regexLiteralToString(), "d/e/f");
        Assert.assertEquals((new TextQLParser(string2InputStream(" /FROM/ "))).regexLiteralToString(), "FROM");
        Assert.assertEquals((new TextQLParser(string2InputStream(" /\"/ "))).regexLiteralToString(), "\"");
        Assert.assertEquals((new TextQLParser(string2InputStream(" /\\/ "))).regexLiteralToString(), "\\");
        assertException(()->(new TextQLParser(string2InputStream(" /21 "))).regexLiteralToString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" 2/1/ "))).regexLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" \"4/ "))).regexLiteralToString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" FROM// "))).regexLiteralToString(), ParseException.class);
    }
    @Test
    public void teststringLiteralToString() throws ParseException {
        Assert.assertEquals((new TextQLParser(string2InputStream(" \"\" "))).stringLiteralToString(), "");
        Assert.assertEquals((new TextQLParser(string2InputStream(" \"abc\" "))).stringLiteralToString(), "abc");
        Assert.assertEquals((new TextQLParser(string2InputStream(" \"de f\" "))).stringLiteralToString(), "de f");
        Assert.assertEquals((new TextQLParser(string2InputStream(" \"d\\\"e\" "))).stringLiteralToString(), "d\"e");
        Assert.assertEquals((new TextQLParser(string2InputStream(" \"d\\\"e\\\"f\" "))).stringLiteralToString(), "d\"e\"f");
        Assert.assertEquals((new TextQLParser(string2InputStream(" \"\\\"\" "))).stringLiteralToString(), "\"");
        Assert.assertEquals((new TextQLParser(string2InputStream(" \"\\\" "))).stringLiteralToString(), "\\");
        assertException(()->(new TextQLParser(string2InputStream(" \"21 "))).stringLiteralToString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" 'aa' "))).stringLiteralToString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" 2\"1\" "))).stringLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" 21 "))).stringLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" SELECTa "))).stringLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" abc "))).stringLiteralToString(), ParseException.class);
    }
    @Test
    public void testIdentifier() throws ParseException {
        Assert.assertEquals((new TextQLParser(string2InputStream(" i "))).identifierLiteralToString(), "i");
        Assert.assertEquals((new TextQLParser(string2InputStream(" id "))).identifierLiteralToString(), "id");
        Assert.assertEquals((new TextQLParser(string2InputStream(" id de "))).identifierLiteralToString(), "id");
        Assert.assertEquals((new TextQLParser(string2InputStream(" id0 "))).identifierLiteralToString(), "id0");
        Assert.assertEquals((new TextQLParser(string2InputStream(" i6i8s7s "))).identifierLiteralToString(), "i6i8s7s");
        Assert.assertEquals((new TextQLParser(string2InputStream(" j7i\\8s7s "))).identifierLiteralToString(), "j7i");
        Assert.assertEquals((new TextQLParser(string2InputStream(" k8i\"8s7s "))).identifierLiteralToString(), "k8i");
        Assert.assertEquals((new TextQLParser(string2InputStream(" aFROM "))).identifierLiteralToString(), "aFROM");
        Assert.assertEquals((new TextQLParser(string2InputStream(" A "))).identifierLiteralToString(), "A");
        Assert.assertEquals((new TextQLParser(string2InputStream(" FROMa "))).identifierLiteralToString(), "FROMa");
        Assert.assertEquals((new TextQLParser(string2InputStream(" Ed0 "))).identifierLiteralToString(), "Ed0");
        assertException(()->(new TextQLParser(string2InputStream(" 2df "))).identifierLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" _a "))).identifierLiteralToString(), TokenMgrError.class);
    }
    @Test
    public void testidentifierListToListString() throws ParseException {
        Assert.assertEquals((new TextQLParser(string2InputStream(" i "))).identifierListToListString(), Arrays.asList("i"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" id "))).identifierListToListString(), Arrays.asList("id"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" id de "))).identifierListToListString(), Arrays.asList("id"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" id,de "))).identifierListToListString(), Arrays.asList("id","de"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" id0 "))).identifierListToListString(), Arrays.asList("id0"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" i6i8s7s "))).identifierListToListString(), Arrays.asList("i6i8s7s"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" i6,i8,s7,s "))).identifierListToListString(), Arrays.asList("i6","i8","s7","s"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" j7i/8s7s/ "))).identifierListToListString(), Arrays.asList("j7i"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" k8i\"8s7s\" "))).identifierListToListString(), Arrays.asList("k8i"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" aFROM "))).identifierListToListString(), Arrays.asList("aFROM"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" B7FROM "))).identifierListToListString(), Arrays.asList("B7FROM"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" A "))).identifierListToListString(), Arrays.asList("A"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" FROMa "))).identifierListToListString(), Arrays.asList("FROMa"));
        assertException(()->(new TextQLParser(string2InputStream(" j7i,/8s7s/ "))).identifierListToListString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" k8i,\"8s7s\" "))).identifierListToListString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" k8i,,k9j "))).identifierListToListString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" k8i,/8s7s/ "))).identifierListToListString(),ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" k8i, "))).identifierListToListString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" j7i\\8s7s "))).identifierListToListString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" k8i\"8s7s "))).identifierListToListString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" 2df "))).identifierListToListString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" _a "))).identifierListToListString(), TokenMgrError.class);
    }
    @Test
    public void testExtractPredicate() throws ParseException {
        /*
         * TODO: No point of implementing the test for this rule for now since
         * the only thing it's happening is calling extractKeywordMatchPredicate();
         */
    	//Identifier identifierListToListString
    }
    
    @Test
    public void testStatement() throws ParseException {
        String selectStatement00 = "SELECT * FROM a;";
        Statement selectStatementParameters00 = new SelectStatement("_sid0", true, null, null, "a", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement00))).statement(), selectStatementParameters00);
        String selectStatement06 = "SELECT f8, fa, fc, df, ff FROM j;";
        Statement selectStatementParameters06 = new SelectStatement("_sid0", null, Arrays.asList("f8","fa","fc","df","ff"), null, "j", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement06))).statement(), selectStatementParameters06);
    	String selectStatement13 = "SELECT h, i, j EXTRACT KEYWORDMATCH([h6,h7,k8,k9], \"key5\") FROM q;";
    	ExtractPredicate selectStatementExtract13 = new KeywordExtractPredicate(Arrays.asList("h6","h7","k8","k9"), "key5", null);
    	Statement selectStatementParameters13 = new SelectStatement("_sid0", null, Arrays.asList("h","i","j"), selectStatementExtract13, "q", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement13))).statement(), selectStatementParameters13);
    	String selectStatement14 = "EXTRACT KEYWORDMATCH([i6,j7,l8,m9], \"key5\") FROM q;";
    	ExtractPredicate selectStatementExtract14 = new KeywordExtractPredicate(Arrays.asList("i6","j7","l8","m9"), "key5", null);
    	Statement selectStatementParameters14 = new SelectStatement("_sid0", null, null, selectStatementExtract14, "q", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement14))).statement(), selectStatementParameters14);
        String selectStatement21 = "EXTRACT KEYWORDMATCH([h3,i2,j1,k0], \"key\\\"/\") FROM m LIMIT 4 OFFSET 25 ;";
        ExtractPredicate selectStatementExtract21 = new KeywordExtractPredicate(Arrays.asList("h3","i2","j1","k0"), "key\"/", null);
        Statement selectStatementParameters21 = new SelectStatement("_sid0", null, null, selectStatementExtract21, "m", 4, 25);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement21))).statement(), selectStatementParameters21);
    	String createViewStatement00 = " CREATE VIEW v0 AS SELECT * FROM a; ";
    	Statement createViewStatementParameters00 = new CreateViewStatement("v0", new SelectStatement("_sid0", true, null, null, "a", null, null));
    	Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement00))).statement(), createViewStatementParameters00);
        String createViewStatement01 = " CREATE VIEW v1 AS SELECT f8, fa, fc, df, ff FROM j LIMIT 1 OFFSET 8; ";
        Statement createViewStatementParameters01 = new CreateViewStatement("v1", new SelectStatement("_sid0", null, Arrays.asList("f8","fa","fc","df","ff"), null, "j", 1, 8));
    	Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement01))).statement(), createViewStatementParameters01);
        String createViewStatement02 = " CREATE VIEW v2 AS SELECT e EXTRACT KEYWORDMATCH([g4,g5], \"key0\") FROM o ;";
        ExtractPredicate createViewStatementExtract02 = new KeywordExtractPredicate(Arrays.asList("g4","g5"), "key0", null);
        Statement createViewStatementParameters02 = new CreateViewStatement("v2", new SelectStatement("_sid0", null, Arrays.asList("e"), createViewStatementExtract02, "o", null, null));
    	Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement02))).statement(), createViewStatementParameters02);
        String createViewStatement03 = " CREATE VIEW v2 AS EXTRACT KEYWORDMATCH([g4,g5], \"key0\", substring) FROM o ;";
        ExtractPredicate createViewStatementExtract03 = new KeywordExtractPredicate(Arrays.asList("g4","g5"), "key0", "substring");
        Statement createViewStatementParameters03 = new CreateViewStatement("v2", new SelectStatement("_sid0", null, null, createViewStatementExtract03, "o", null, null));
    	Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement03))).statement(), createViewStatementParameters03);
        String createViewStatement04 = " CREATE VIEW v3 AS CREATE VIEW v4 AS SELECT * FROM a LIMIT 1 OFFSET 2;";
        Statement createViewStatementParameters04 = new CreateViewStatement("v3", new CreateViewStatement("v4", new SelectStatement("_sid0", true, null, null, "a", 1, 2)));
    	Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement04))).statement(), createViewStatementParameters04);
    }
    @Test
    public void testStatementsMain() throws ParseException {
        /*
    	 * TODO: create some test cases with multiple statements
         */
    } 
    
    @Test
    public void testSelectStatement() throws ParseException {
    	String selectStatement00 = "SELECT * FROM a";
    	Statement selectStatementParameters00 = new SelectStatement("_sid0", true, null, null, "a", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement00))).selectStatement(), selectStatementParameters00);
        String selectStatement01 = "SELECT * FROM b LIMIT 5";
        Statement selectStatementParameters01 = new SelectStatement("_sid0", true, null, null, "b", 5, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement01))).selectStatement(), selectStatementParameters01);
        String selectStatement02 = "SELECT * FROM c LIMIT 1 OFFSET 8";
        Statement selectStatementParameters02 = new SelectStatement("_sid0", true, null, null, "c", 1, 8);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement02))).selectStatement(), selectStatementParameters02);
        String selectStatement03 = "SELECT * FROM d OFFSET 6";
        Statement selectStatementParameters03 = new SelectStatement("_sid0", true, null, null, "d", null, 6);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement03))).selectStatement(), selectStatementParameters03);
        String selectStatement04 = "SELECT f1 FROM e";
        Statement selectStatementParameters04 = new SelectStatement("_sid0", null, Arrays.asList("f1"), null, "e", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement04))).selectStatement(), selectStatementParameters04);
        String selectStatement05 = "SELECT f1, f5 FROM i";
        Statement selectStatementParameters05 = new SelectStatement("_sid0", null, Arrays.asList("f1","f5"), null, "i", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement05))).selectStatement(), selectStatementParameters05);
        String selectStatement06 = "SELECT f8, fa, fc, df, ff FROM j";
        Statement selectStatementParameters06 = new SelectStatement("_sid0", null, Arrays.asList("f8","fa","fc","df","ff"), null, "j", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement06))).selectStatement(), selectStatementParameters06);
        String selectStatement07 = "SELECT a EXTRACT KEYWORDMATCH(g0, \"key1\") FROM k";
        Statement selectStatementParameters07 = new SelectStatement("_sid0", null, Arrays.asList("a"), new KeywordExtractPredicate(Arrays.asList("g0"), "key1", null), "k", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement07))).selectStatement(), selectStatementParameters07);
        String selectStatement08 = "SELECT b EXTRACT KEYWORDMATCH(g1, \"key2\", conjunction) FROM l";
        Statement selectStatementParameters08 = new SelectStatement("_sid0", null, Arrays.asList("b"), new KeywordExtractPredicate(Arrays.asList("g1"), "key2", "conjunction"), "l", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement08))).selectStatement(), selectStatementParameters08);
    	String selectStatement10 = "SELECT v EXTRACT KEYWORDMATCH(u, \"keyZ\") FROM t";
    	Statement selectStatementParameters10 = new SelectStatement("_sid0", null, Arrays.asList("v"), new KeywordExtractPredicate(Arrays.asList("u"), "keyZ", null), "t", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement10))).selectStatement(), selectStatementParameters10);
    	String selectStatement11 = "SELECT e EXTRACT KEYWORDMATCH([g4], \"key0\") FROM o";
    	Statement selectStatementParameters11 = new SelectStatement("_sid0", null, Arrays.asList("e"), new KeywordExtractPredicate(Arrays.asList("g4"), "key0", null), "o", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement11))).selectStatement(), selectStatementParameters11);
        String selectStatement12 = "SELECT f EXTRACT KEYWORDMATCH([g6,g7,h8,i9], \"key\") FROM p";
        Statement selectStatementParameters12 = new SelectStatement("_sid0", null, Arrays.asList("f"), new KeywordExtractPredicate(Arrays.asList("g6","g7","h8","i9"), "key", null), "p", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement12))).selectStatement(), selectStatementParameters12);
        String selectStatement13 = "SELECT h, i, j EXTRACT KEYWORDMATCH([h6,h7,k8,k9], \"key5\") FROM q";
        Statement selectStatementParameters13 = new SelectStatement("_sid0", null, Arrays.asList("h","i","j"), new KeywordExtractPredicate(Arrays.asList("h6","h7","k8","k9"), "key5", null), "q", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement13))).selectStatement(), selectStatementParameters13);
        String selectStatement14 = "EXTRACT KEYWORDMATCH([i6,j7,l8,m9], \"key5\") FROM q";
        Statement selectStatementParameters14 = new SelectStatement("_sid0", null, null, new KeywordExtractPredicate(Arrays.asList("i6","j7","l8","m9"), "key5", null), "q", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement14))).selectStatement(), selectStatementParameters14);
        String selectStatement15 = "EXTRACT KEYWORDMATCH(g0, \"key1\") FROM k";
        Statement selectStatementParameters15 = new SelectStatement("_sid0", null, null, new KeywordExtractPredicate(Arrays.asList("g0"), "key1", null), "k", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement15))).selectStatement(), selectStatementParameters15);
        String selectStatement16 = "EXTRACT KEYWORDMATCH(g1, \"key2\", phrase) FROM l";
        Statement selectStatementParameters16 = new SelectStatement("_sid0", null, null, new KeywordExtractPredicate(Arrays.asList("g1"), "key2", "phrase"), "l", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement16))).selectStatement(), selectStatementParameters16);
        String selectStatement19 = "EXTRACT KEYWORDMATCH([g4], \"key0\") FROM o";
        Statement selectStatementParameters19 = new SelectStatement("_sid0", null, null, new KeywordExtractPredicate(Arrays.asList("g4"), "key0", null), "o", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement19))).selectStatement(), selectStatementParameters19);
    	String selectStatement20 = "EXTRACT KEYWORDMATCH([g6,g7,h8,i9], \"key\") FROM p";
    	Statement selectStatementParameters20 = new SelectStatement("_sid0", null, null, new KeywordExtractPredicate(Arrays.asList("g6","g7","h8","i9"), "key", null), "p", null, null);
    	Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement20))).selectStatement(), selectStatementParameters20);
    	String selectStatement21 = "EXTRACT KEYWORDMATCH([h3,i2,j1,k0], \"key\\\"/\") FROM m LIMIT 4 OFFSET 25 ";
    	Statement selectStatementParameters21 = new SelectStatement("_sid0", null, null, new KeywordExtractPredicate(Arrays.asList("h3","i2","j1","k0"), "key\"/", null), "m", 4, 25);
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

    @Test
    public void testExtractKeywordMatchPredicate() throws ParseException {
    	String keywordMatchPredicate00 = " KEYWORDMATCH(g0, \"key1\") ";
        ExtractPredicate keywordMatchParameters00 = new KeywordExtractPredicate(Arrays.asList("g0"), "key1", null);
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchPredicate00))).extractKeywordMatchPredicate(), keywordMatchParameters00);
        String keywordMatchPredicate01 = " KEYWORDMATCH(g1, \"key2\", conjunction) ";
        ExtractPredicate keywordMatchParameters01 = new KeywordExtractPredicate(Arrays.asList("g1"), "key2", "conjunction");
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchPredicate01))).extractKeywordMatchPredicate(), keywordMatchParameters01);
        String keywordMatchPredicate02 = " KEYWORDMATCH(g2, \"key3\", phrase) ";
        ExtractPredicate keywordMatchParameters02 = new KeywordExtractPredicate(Arrays.asList("g2"), "key3", "phrase");
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchPredicate02))).extractKeywordMatchPredicate(), keywordMatchParameters02);
        String keywordMatchPredicate03 = " KEYWORDMATCH(g3, \"key4\", substring) ";
        ExtractPredicate keywordMatchParameters03 = new KeywordExtractPredicate(Arrays.asList("g3"), "key4", "substring");
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchPredicate03))).extractKeywordMatchPredicate(), keywordMatchParameters03);
        String keywordMatchPredicate04 = " KEYWORDMATCH([g4], \"key0\") ";
        ExtractPredicate keywordMatchParameters04 = new KeywordExtractPredicate(Arrays.asList("g4"), "key0", null);
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchPredicate04))).extractKeywordMatchPredicate(), keywordMatchParameters04);
        String keywordMatchPredicate05 = " KEYWORDMATCH([g4,g5], \"key0\") ";
        ExtractPredicate keywordMatchParameters05 = new KeywordExtractPredicate(Arrays.asList("g4","g5"), "key0", null);
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchPredicate05))).extractKeywordMatchPredicate(), keywordMatchParameters05);
        String keywordMatchPredicate06 = " KEYWORDMATCH([g6,g7,h8,i9], \"key\") ";
        ExtractPredicate keywordMatchParameters06 = new KeywordExtractPredicate(Arrays.asList("g6","g7","h8","i9"), "key", null);
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchPredicate06))).extractKeywordMatchPredicate(), keywordMatchParameters06);
    	String keywordMatchPredicate07 = " KEYWORDMATCH (\"key1\") ";
    	assertException(()->(new TextQLParser(string2InputStream(keywordMatchPredicate07))).extractKeywordMatchPredicate(), ParseException.class);
        String keywordMatchPredicate08 = " KEYWORDMATCH ([i6,j7,l8,m9, \"key5\") ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchPredicate08))).extractKeywordMatchPredicate(), ParseException.class);
        String keywordMatchPredicate09 = " KEYWORDMATCH (i6,j7,l8,m9, \"key5\") ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchPredicate09))).extractKeywordMatchPredicate(), ParseException.class);
        String keywordMatchPredicate10 = " KEYWORDMATCH (i6,j7,l8,m9], \"key5\") ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchPredicate10))).extractKeywordMatchPredicate(), ParseException.class);
        String keywordMatchPredicate11 = " KEYWORDMATCH ([i6,j7,l8,m9, \"key5\", conjunction) ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchPredicate11))).extractKeywordMatchPredicate(), ParseException.class);
        String keywordMatchPredicate12 = " KEYWORDMATCH (i6,j7,l8,m9, \"key5\", substring) ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchPredicate12))).extractKeywordMatchPredicate(), ParseException.class);
        String keywordMatchPredicate13 = " KEYWORDMATCH ([i6,j7,l8,m9, \"key5\", phrase) ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchPredicate13))).extractKeywordMatchPredicate(), ParseException.class);
        String keywordMatchPredicate14 = " KEYWORDMATCH ([], key5) ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchPredicate14))).extractKeywordMatchPredicate(), ParseException.class);
        String keywordMatchPredicate15 = " KEYWORDMATCH ([a], key5) ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchPredicate15))).extractKeywordMatchPredicate(), ParseException.class);
        String keywordMatchPredicate16 = " KEYWORDMATCH ([a]) ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchPredicate16))).extractKeywordMatchPredicate(), ParseException.class);
        
    }
    
    @Test
    public void createViewStatement() throws ParseException {
    	String createViewStatement00 = " CREATE VIEW v0 AS SELECT * FROM a ";
    	Statement createViewStatementParameters00 = new CreateViewStatement("v0", new SelectStatement("_sid0", true, null, null, "a", null, null));
    	Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement00))).createViewStatement(), createViewStatementParameters00);
        String createViewStatement01 = " CREATE VIEW v1 AS SELECT f8, fa, fc, df, ff FROM j LIMIT 1 OFFSET 8 ";
    	Statement createViewStatementParameters01 = new CreateViewStatement("v1", new SelectStatement("_sid0", null, Arrays.asList("f8","fa","fc","df","ff"), null, "j", 1, 8));
    	Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement01))).createViewStatement(), createViewStatementParameters01);
        String createViewStatement02 = " CREATE VIEW v2 AS SELECT e EXTRACT KEYWORDMATCH([g4,g5], \"key0\") FROM o ";
    	Statement createViewStatementParameters02 = new CreateViewStatement("v2", new SelectStatement("_sid0", null, Arrays.asList("e"), new KeywordExtractPredicate(Arrays.asList("g4","g5"), "key0", null), "o", null, null));
    	Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement02))).createViewStatement(), createViewStatementParameters02);
        String createViewStatement03 = " CREATE VIEW v2 AS EXTRACT KEYWORDMATCH([g4,g5], \"key0\", substring) FROM o ";
    	Statement createViewStatementParameters03 = new CreateViewStatement("v2", new SelectStatement("_sid0", null, null, new KeywordExtractPredicate(Arrays.asList("g4","g5"), "key0", "substring"), "o", null, null));
    	Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement03))).createViewStatement(), createViewStatementParameters03);
        String createViewStatement04 = " CREATE VIEW v3 AS CREATE VIEW v4 AS SELECT * FROM a LIMIT 1 OFFSET 2";
    	Statement createViewStatementParameters04 = new CreateViewStatement("v3", new CreateViewStatement("v4", new SelectStatement("_sid0", true, null, null, "a", 1, 2)));
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
