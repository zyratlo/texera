package edu.uci.ics.textdb.dataflow.common;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IPredicate;

/**
 * 
 * @author sripadks
 *
 */
public class JoinPredicate implements IPredicate {
	
	private Attribute outerIdAttribute;
	private Attribute outerJoinAttribute;
	private Attribute innerIdAttribute;
	private Attribute innerJoinAttribute;
	private Integer threshold;
	
	/**
	 * <p>This constructor is used to set the parameters required for the Join Operator.</p>
	 * 
	 * <p>
	 * JoinPredicate joinPre = new JoinPredicate(Attribute idAttr, Attribute descriptionAttr, Attribute idAttr, Attribute descriptionAttr, 10)
	 * <br>will create a predicate that compares the spans of type descriptionAttr of outer and inner operators
	 * (that agree on the idAttr id attributes) and outputs tuples which satisfy the criteria of being
	 * within 10 characters of each other. </p>
	 * 
	 * <p>
	 * Given below is a setting and an example using this setting to use JoinPredicate (consider the two 
	 * tuples to be from two different operators).</p>
	 * 
	 * <p>
	 * public static final Attribute idAttr = new Attribute("id", FieldType.INTEGER);
	 * <br>public static final Attribute authorAttr = new Attribute("author", FieldType.STRING);
	 * <br>public static final Attribute titleAttr = new Attribute("title", FieldType.STRING);
	 * <br>public static final Attribute pagesAttr = new Attribute("numberOfPages", FieldType.INTEGER);
	 * <br>public static final Attribute reviewAttr = new Attribute("reviewOfBook", FieldType.TEXT);
	 * </p>
	 * <p>
	 * public static final Attribute[] bookAttr = { idAttr, authorAttr, titleAttr, pagesAttr, reviewAttr, 
	   <br>SchemaConstants.SPAN_LIST_ATTRIBUTE };
	 * public static final Schema bookSchema = new Schema(bookAttr);
	 * </p>
	 * <p>
	 * List<Span> spanList = new ArrayList<>();
       <br>Span span = new Span("reviewOfBook", 11, 18, "special", "special");
       <br>spanList.add(span);
       <br>span = new Span("reviewOfBook", 19, 23, "kind", "kind");
       <br>spanList.add(span);
       <br>span = new Span("reviewOfBook", 27, 33, "writer", "writer");
       <br>spanList.add(span);
       </p>
	 * <p>
	 * IField[] book1 = { new IntegerField(1), new StringField("Mary Roach"), 
	   new StringField("Grunt: The Curious Science of Humans at War"), new IntegerField(288), 
	   new TextField("It takes a special kind of writer to make topics ranging from death to our 
	   gastrointestinal tract interesting (sometimes hilariously so), and pop science writer Mary Roach is 
	   always up to the task. In her latest book, Grunt, she explores how our soldiers combat their 
	   non-gun-wielding opponents--panic, heat exhaustion, the runs, and more. It will give you a new 
	   appreciation not only for our men and women in uniform (and by the way, one of the innumerable 
	   things you’ll learn is how and why they choose the fabric for those uniforms), but for the unsung 
	   scientist-soldiers tasked with coming up with ways to keep the “grunts” alive and well. If you are 
	   at all familiar with Roach’s oeuvre, you know her enthusiasm for her subjects is palpable and 
	   infectious. This latest offering is no exception."), new ListField<>(spanList) };
	 * </p>
	 * <p>
	 * spanList = new ArrayList<>();
       <br>Span span = new Span("reviewOfBook", 46, 54, "pulitzer", "pulitzer");
       <br>spanList.add(span);
       <br>span = new Span("reviewOfBook", 55, 60, "prize", "prize");
       <br>spanList.add(span);
       <br>span = new Span("reviewOfBook", 65, 69, "book", "book");
       <br>spanList.add(span);
	 * </p>
	 * <p>
	 * IField[] book2 = { new IntegerField(2), new StringField("Siddhartha Mukherjee"),
	   new StringField("The Gene: An Intimate History"), new IntegerField(608),
	   new TextField("In 2010, Siddhartha Mukherjee was awarded the Pulitzer Prize for his book The Emperor 
	   of All Maladies, a “biography” of cancer. Here, he follows up with a biography of the gene—and 
	   The Gene is just as informative, wise, and well-written as that first book. Mukherjee opens with a 
	   survey of how the gene first came to be conceptualized and understood, taking us through the 
	   thoughts of Aristotle, Darwin, Mendel, Thomas Morgan, and others; he finishes the section with a 
	   look at the case of Carrie Buck (to whom the book is dedicated), who eventually was sterilized in 
	   1927 in a famous American eugenics case. Carrie Buck’s sterilization comes as a warning that informs 
	   the rest of the book. This is what can happen when we start tinkering with this most personal science 
	   and misunderstand the ethical implications of those tinkerings. Through the rest of The Gene, 
	   Mukherjee clearly and skillfully illustrates how the science has grown so much more advanced and 
	   complicated since the 1920s—we are developing the capacity to directly manipulate the human 
	   genome—and how the ethical questions have also grown much more complicated. We could ask for no 
	   wiser, more fascinating and talented writer to guide us into the future of our human heredity than 
	   Siddhartha Mukherjee."), new ListField<>(spanList) }; </p>
	 * 
	 * <p>
	 * ITuple bookTuple1 = new DataTuple(bookSchema, book1);
	 * <br>ITuple bookTuple2 = new DataTuple(bookSchema, book2);
	 * </p>
	 * 
	 * JoinPredicate joinPre = new JoinPredicate(idAttr, reviewAttr, idAttr, reviewAttr, 10);
	 * <p>
	 * Example 1:
	 * Suppose we have the tuple bookTuple1 as outer tuple for join and bookTuple2 as the inner tuple for 
	 * join. In this case, when we invoke compareId(), we find that the IDs don't match. Hence, the join 
	 * doesn't take place. </p>
	 * 
	 * <p>
	 * Example 2:
	 * Suppose that both the outer and inner tuples are bookTuple1 and we want to join over reviewAttr 
	 * "special" and "kind". Since, both the words are within 10 characters from each other, join will take 
	 * place and return the tuple bookTuple1 with a new span list consisting of the combined span <11, 23>.
	 * </p>
	 * <p>
	 * Example 3:
	 * Consider the previous example but with "special" and "writer" to be joined. Since, the words are 
	 * than 10 characters, join won't produce a result and simply returns the tuple bookTuple1. </p>
	 * 
	 * @param outerIdAttribute is the ID attribute of the outer operator
	 * @param outerJoinAttribute is the attribute of the outer operator to be used for join
	 * @param innerIdAttribute is the ID attribute of the inner operator
	 * @param innerJoinAttribute is the ID of the inner operator to be used for join
	 * @param threshold is the maximum distance (in characters) between any two spans
	 */
	public JoinPredicate(Attribute outerIdAttribute, Attribute outerJoinAttribute, Attribute innerIdAttribute, Attribute innerJoinAttribute, Integer threshold) {
		this.outerIdAttribute = outerIdAttribute;
		this.outerJoinAttribute = outerJoinAttribute;
		this.innerIdAttribute = innerIdAttribute;
		this.innerJoinAttribute = innerJoinAttribute;
		this.threshold = threshold;
	}
	
	public Attribute getOuterAttrId() {
		return this.outerIdAttribute;
	}
	
	public Attribute getInnerAttrId() {
		return this.innerIdAttribute;
	}
	
	public Attribute getOuterAttr() {
		return outerJoinAttribute;
	}
	
	public Attribute getInnerAttr() {
		return this.innerJoinAttribute;
	}
	
	public Integer getThreshold() {
		return this.threshold;
	}
	
	/**
	 * Compares the IDs of the tuples. Returns true if IDs match else returns false.
	 * @return compResult
	 */
	public boolean compareId() {
		boolean compResult = false;
		return compResult;
	}
	
	/**
	 * Compares the attributes of the tuples. Returns true if attributes match else returns false.
	 * @return compResult
	 */
	public boolean compareAttr() {
		boolean compResult = false;
		return compResult;
	}
}
