package org.javlo.helper.sql.test;

import org.javlo.helper.sql.Column;
import org.javlo.helper.sql.Table;

@Table
public class User {	
	private long id;
	private String firstname;
	private String lastname;
	private String email;
	private long city;
	
	@Column (auto = true, primaryKey = true, notNull = true)
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	@Column
	public String getFirstname() {
		return firstname;
	}
	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}
	@Column
	public String getLastname() {
		return lastname;
	}
	public void setLastname(String lastname) {
		this.lastname = lastname;
	}
	@Column(notNull = true)
	public String getEmail() {
		return email;
	}
	public void setEmail(String email) {
		this.email = email;
	}
	@Column(foreign = "city.id", notNull = true)
	public long getCity() {
		return city;
	}
	public void setCity(long city) {
		this.city = city;
	}
	
}