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
	 * This constructor is used to set the parameters required for the Join Operator.
	 * 
	 * JoinPredicate joinPre = new JoinPredicate(ID_ATTR, DESCRIPTION_ATTR, ID_ATTR, DESCRIPTION_ATTR, 10)
	 * will create a predicate that compares the spans of type DESCRIPTION_ATTR of outer and inner operator
	 * output tuples which have the ID_ATTR id attribute to be within 10 characters of each other.
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
}
