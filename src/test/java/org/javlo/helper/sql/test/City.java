package org.javlo.helper.sql.test;

import org.javlo.helper.sql.Column;
import org.javlo.helper.sql.Table;

@Table
public class City {	
	
	private long id;
	private String Name;
	private String postcode;
	
	@Column (primaryKey = true, notNull = true, auto = true)
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	@Column
	public String getName() {
		return Name;
	}
	public void setName(String name) {
		Name = name;
	}
	@Column
	public String getPostcode() {
		return postcode;
	}
	public void setPostcode(String postcode) {
		this.postcode = postcode;
	}
}