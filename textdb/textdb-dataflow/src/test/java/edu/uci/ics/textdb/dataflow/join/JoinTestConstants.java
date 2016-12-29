package edu.uci.ics.textdb.dataflow.join;

import java.util.Arrays;
import java.util.List;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.field.TextField;

public class JoinTestConstants {
    
    public static final String ID = "id";
    public static final String AUTHOR = "author";
    public static final String TITLE = "title";
    public static final String PAGES = "numberOfPages";
    public static final String REVIEW = "reviewOfBook";
    
    public static final Attribute ID_ATTR = new Attribute(ID, FieldType.INTEGER);
    public static final Attribute AUTHOR_ATTR = new Attribute(AUTHOR, FieldType.STRING);
    public static final Attribute TITLE_ATTR = new Attribute(TITLE, FieldType.STRING);
    public static final Attribute PAGES_ATTR = new Attribute(PAGES, FieldType.INTEGER);
    public static final Attribute REVIEW_ATTR = new Attribute(REVIEW, FieldType.TEXT);
    
    public static final Schema BOOK_SCHEMA = new Schema(ID_ATTR, AUTHOR_ATTR, TITLE_ATTR, PAGES_ATTR, REVIEW_ATTR);
       
    public static final List<ITuple> bookGroup1 = Arrays.asList(     
            new DataTuple(BOOK_SCHEMA,
                    new IntegerField(52), new StringField("Mary Roach"),
                    new StringField("Grunt: The Curious Science of Humans at War"), new IntegerField(288),
                    new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                            + "gastrointestinal tract interesting (sometimes "
                            + "hilariously so), and pop science writer Mary Roach is " + "always up to the task.")),
            new DataTuple(BOOK_SCHEMA,               
                    new IntegerField(51), new StringField("author unknown"),
                    new StringField("typical"), new IntegerField(300),
                    new TextField("Review of a Book. This is a typical "
                            + "review. This is a test. A book review " + "test. A test to test queries without "
                            + "actually using actual review. From " + "here onwards, we can pretend this to "
                            + "be actually a review even if it is not " + "your typical book review.")),
            new DataTuple(BOOK_SCHEMA,
                    new IntegerField(53), new StringField("Noah Hawley"),
                    new StringField("Before the Fall"), new IntegerField(400),
                    new TextField("Review of a Book. This is a typical "
                            + "review. This is a test. A book review " + "test. A test to test queries without "
                            + "actually using actual review. From " + "here onwards, we can pretend this to "
                            + "be actually a review even if it is not " + "your typical book review.")),
            new DataTuple(BOOK_SCHEMA,
                    new IntegerField(54), new StringField("Andria Williams"),
                    new StringField("The Longest Night: A Novel"), new IntegerField(400),
                    new TextField("Review of a Book. This is a typical "
                            + "review. This is a test. A book review " + "test. A test to test queries without "
                            + "actually using actual review. From " + "here onwards, we can pretend this to "
                            + "be actually a review even if it is not " + "your typical book review.")),
            new DataTuple(BOOK_SCHEMA,
                    new IntegerField(55), new StringField("Matti Friedman"),
                    new StringField("Pumpkinflowers: A Soldier's " + "Story"), new IntegerField(256),
                    new TextField("Review of a Book. This is a typical "
                            + "review. This is a test. A book review " + "test. A test to test queries without "
                            + "actually using actual review. From " + "here onwards, we can pretend this to "
                            + "be actually a review even if it is not " + "your typical book review.")));
    
    public static final List<ITuple> bookGroup2 = Arrays.asList(
            new DataTuple(BOOK_SCHEMA,
                    new IntegerField(61), new StringField("book author"),
                    new StringField("actually typical"), new IntegerField(700),
                    new TextField("Review of a Book. This is a typical "
                            + "review. This is a test. A book review " + "test. A test to test queries without "
                            + "actually using actual review. From " + "here onwards, we can pretend this to "
                            + "be actually a review even if it is not " + "your typical book review.")),
            new DataTuple(BOOK_SCHEMA,
                    new IntegerField(62), new StringField("Siddhartha Mukherjee"),
                    new StringField("The Gene: An Intimate History"), new IntegerField(608),
                    new TextField("Review of a Book. This is a typical "
                            + "review. This is a test. A book review " + "test. A test to test queries without "
                            + "actually using actual review. From " + "here onwards, we can pretend this to "
                            + "be actually a review even if it is not " + "your typical book review.")),
            new DataTuple(BOOK_SCHEMA,
                    new IntegerField(63), new StringField("Paul Kalanithi"),
                    new StringField("When Breath Becomes Air"), new IntegerField(256),
                    new TextField("Review of a Book. This is a typical "
                            + "review. This is a test. A book review " + "test. A test to test queries without "
                            + "actually using actual review. From " + "here onwards, we can pretend this to "
                            + "be actually a review even if it is not " + "your typical book review.")),
            new DataTuple(BOOK_SCHEMA,
                    new IntegerField(64), new StringField("Matthew Desmond"),
                    new StringField("Evicted: Poverty and Profit in the " + "American City"),
                    new IntegerField(432),
                    new TextField("Review of a Book. This is a typical "
                            + "review. This is a test. A book review " + "test. A test to test queries without "
                            + "actually using actual review. From " + "here onwards, we can pretend this to "
                            + "be actually a review even if it is not " + "your typical book review.")),
            new DataTuple(BOOK_SCHEMA,
                    new IntegerField(65), new StringField("Sharon Guskin"),
                    new StringField("The Forgetting Time: A Novel"), new IntegerField(368),
                    new TextField("Review of a Book. This is a typical "
                            + "review. This is a test. A book review " + "test. A test to test queries without "
                            + "actually using actual review. From " + "here onwards, we can pretend this to "
                            + "be actually a review even if it is not " + "your typical book review.")));

    
    
}
