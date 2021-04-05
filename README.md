
JAVLO SQL
=========

Javlo SQL is very simple tools to create and manipulate SQL database (postgres) from java bean.

## Getting Started

### code sample

#### bean
```java
@Table
public class Hotel {

	private static Logger logger = Logger.getLogger(Hotel.class.getName());

	private long id;
	private String code;
	private String name;
	private long city;
	private Date latestUpdate = new Date();

	public Hotel() {
	}

	@Column(auto = true, primaryKey = true)
	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	@Column(unique = true)
	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	@Column
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Column (foreign = "city.id", notNull = true)	
	public long getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	@Column (name="lastest_update", type="timestamp")
	public Date getLatestUpdate() {
		return latestUpdate;
	}

	public void setLatestUpdate(Date latestUpdate) {
		this.latestUpdate = latestUpdate;
	}

}
```

#### create table
```java
SQLBuilder.createOrUpdateTable(conn, new Hotel());
```

#### update data
```java
SQLBuilder.insertOrUpdate(conn, hotel);
```

#### work with ResultSet
```java
ResultSet rs = stm.executeQuery("select * from hotel");
if (rs.next()) {
	hotel = (Hotel) SQLBuilder.rsToBean(rs, new Hotel());
}
```


### Prerequisites

java 8 / maven

## Versioning

0.0.2

## Authors

* **Patrick Vandermaesen** - *Architect main developer*

## License

This project is licensed under GNU LESSER GENERAL PUBLIC LICENSE Version 3
