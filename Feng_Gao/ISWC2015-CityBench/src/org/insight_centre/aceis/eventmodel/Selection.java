package org.insight_centre.aceis.eventmodel;

public class Selection implements Cloneable {
	private String propertyName, providedBy, foi, propertyType;
	private EventDeclaration originalED = new EventDeclaration(null, null, null, null, null, null);

	public EventDeclaration getOriginalED() {
		return originalED;
	}

	public void setOriginalED(EventDeclaration originalED) {
		this.originalED = originalED;
	}

	public Selection(String propertyName, String providedBy, EventDeclaration ed, String foi, String propertyType) {
		super();
		this.propertyName = propertyName;
		this.providedBy = providedBy;
		this.originalED = ed;
		this.foi = foi;
		this.propertyType = propertyType;
	}

	public String getPropertyName() {
		return propertyName;
	}

	public void setPropertyName(String propertyName) {
		this.propertyName = propertyName;
	}

	public String getProvidedBy() {
		return providedBy;
	}

	public void setProvidedBy(String providedBy) {
		this.providedBy = providedBy;
	}

	public String getFoi() {
		return foi;
	}

	public void setFoi(String foi) {
		this.foi = foi;
	}

	public String getPropertyType() {
		return propertyType;
	}

	public void setPropertyType(String propertyType) {
		this.propertyType = propertyType;
	}

	public Selection() {

	}

	public String toString() {
		return propertyName + "|" + providedBy + "|" + originalED.getServiceId() + "|" + propertyType + "|" + foi;

	}

	public Selection clone() throws CloneNotSupportedException {
		return (Selection) super.clone();
	}

	public boolean substitues(Selection sel) {
		if (this.getOriginalED().getEventType().equals(sel.getOriginalED().getEventType())
				&& this.getPropertyType().equals(sel.getPropertyType()) && this.getFoi().equals(sel.getFoi()))
			return true;
		return false;
	}

	// public boolean covers(Selection sel) {
	// return false;
	// }
}
