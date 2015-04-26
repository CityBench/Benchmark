package org.insight_centre.aceis.eventmodel;

/**
 * @author feng
 * 
 *         Complex event processing operators used in event patterns
 */
public class EventOperator implements Cloneable {
	private int cardinality = 1;
	private String ID;
	private OperatorType opt;

	public EventOperator(OperatorType opt, int cardinality, String iD) {
		super();
		this.opt = opt;
		this.cardinality = cardinality;
		ID = iD;
	}

	public EventOperator clone() throws CloneNotSupportedException {
		return (EventOperator) super.clone();
	}

	public int getCardinality() {
		return cardinality;
	}

	public String getID() {
		return ID;
	}

	public OperatorType getOpt() {
		return opt;
	}

	public void print() {
		System.out.println("EO: " + this.ID + ", " + this.opt + ", " + this.cardinality);
	}

	public void setCardinality(int cardinality) {
		this.cardinality = cardinality;
	}

	public void setID(String iD) {
		ID = iD;
	}

	public void setOpt(OperatorType opt) {
		this.opt = opt;
	}

	public String toString() {
		return this.getOpt() + ":" + this.cardinality + "-" + this.ID;
	}
}
