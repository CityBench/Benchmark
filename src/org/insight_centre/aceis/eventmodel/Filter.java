package org.insight_centre.aceis.eventmodel;

import java.util.ArrayList;
import java.util.List;

public class Filter implements Cloneable {
	public static final int lt = 1, gt = 2, leq = 3, geq = 4, eq = 5;
	private int op;
	private String var;
	private Object val;

	public Filter(String var, Object val, int op) {
		super();
		this.var = var;
		this.val = val;
		this.op = op;
	}

	public int getOp() {
		return op;
	}

	public Object getVal() {
		return val;
	}

	public String getVar() {
		return var;
	}

	public void setOp(int op) {
		this.op = op;
	}

	public void setVal(Object val) {
		this.val = val;
	}

	public void setVar(String var) {
		this.var = var;
	}

	public boolean equals(Filter f) {
		if (this.op == f.getOp() && this.var.equals(f.getVar()) && this.val.equals(f.getVal()))
			return true;
		return false;
	}

	public boolean coveredBy(Filter f) {
		boolean opCovered = false;
		if (this.var.equals(f.getVar())) {// same var name

			if (this.getOp() == Filter.eq) {
				if (f.getOp() == Filter.leq) {
					if (this.getVal() instanceof Integer) {
						if (((Integer) this.getVal()) <= ((Integer) f.getVal()))
							opCovered = true;
					} else if (f.getVal() instanceof Double) {
						if (((Double) this.getVal()) <= ((Double) f.getVal()))
							opCovered = true;
					}
				} else if (f.getOp() == Filter.geq) {
					if (this.getVal() instanceof Integer) {
						if (((Integer) this.getVal()) >= ((Integer) f.getVal()))
							opCovered = true;
					} else if (f.getVal() instanceof Double) {
						if (((Double) this.getVal()) >= ((Double) f.getVal()))
							opCovered = true;
					}
				}
			} else if (this.getOp() == Filter.lt) {
				if (f.getOp() == Filter.leq) {
					if (this.getVal() instanceof Integer) {
						if (((Integer) this.getVal()) <= ((Integer) f.getVal()))
							opCovered = true;
					} else if (f.getVal() instanceof Double) {
						if (((Double) this.getVal()) <= ((Double) f.getVal()))
							opCovered = true;
					}
				} else if (f.getOp() == Filter.lt) {
					if (this.getVal() instanceof Integer) {
						if (((Integer) this.getVal()) < ((Integer) f.getVal()))
							opCovered = true;
					} else if (f.getVal() instanceof Double) {
						if (((Double) this.getVal()) < ((Double) f.getVal()))
							opCovered = true;
					}
				}
			} else if (this.getOp() == Filter.gt) {
				if (f.getOp() == Filter.geq) {
					if (this.getVal() instanceof Integer) {
						if (((Integer) this.getVal()) >= ((Integer) f.getVal()))
							opCovered = true;
					} else if (f.getVal() instanceof Double) {
						if (((Double) this.getVal()) >= ((Double) f.getVal()))
							opCovered = true;
					}
				} else if (f.getOp() == Filter.gt) {
					if (this.getVal() instanceof Integer) {
						if (((Integer) this.getVal()) > ((Integer) f.getVal()))
							opCovered = true;
					} else if (f.getVal() instanceof Double) {
						if (((Double) this.getVal()) > ((Double) f.getVal()))
							opCovered = true;
					}
				}
			} else if (this.getOp() == Filter.leq) {
				if (f.getOp() == Filter.leq || f.getOp() == Filter.lt) {
					if (this.getVal() instanceof Integer) {
						if (((Integer) this.getVal()) < ((Integer) f.getVal()))
							opCovered = true;
					} else if (f.getVal() instanceof Double) {
						if (((Double) this.getVal()) < ((Double) f.getVal()))
							opCovered = true;
					}
				}
			} else if (this.getOp() == Filter.geq) {
				if (f.getOp() == Filter.geq || f.getOp() == Filter.gt) {
					if (this.getVal() instanceof Integer) {
						if (((Integer) this.getVal()) > ((Integer) f.getVal()))
							opCovered = true;
					} else if (f.getVal() instanceof Double) {
						if (((Double) this.getVal()) > ((Double) f.getVal()))
							opCovered = true;
					}
				}
			}
		}

		return opCovered;
		// return false;
	}

	public boolean covers(Filter f) {
		return f.coveredBy(this);

	}

	public List<Filter> getCompatible(List<Filter> rootFilters1) {
		ArrayList<Filter> results = new ArrayList<Filter>();
		for (Filter f : rootFilters1) {
			if (this.getVar().equals(f.getVar())) {
				if (this.getOp() == f.getOp())
					results.add(f);
				else if (this.getOp() == Filter.lt && f.getOp() == Filter.leq)
					results.add(f);
				else if (this.getOp() == Filter.gt && f.getOp() == Filter.geq)
					results.add(f);
				else if (this.getOp() == Filter.leq && f.getOp() == Filter.lt)
					results.add(f);
				else if (this.getOp() == Filter.geq && f.getOp() == Filter.gt)
					results.add(f);
			}
		}
		return results;
	}

	public String toString() {
		return this.getVar() + " " + this.getOp() + " " + this.getVal();
	}
}
