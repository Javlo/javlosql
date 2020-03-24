
JAVLO SQL
=========

Javlo SQL is very simple tools to create and manipulate SQL database (postgres) from java bean.

## Getting Started

### code sample

#### bean
`
@Table
public class Hotel {

	private static Logger logger = Logger.getLogger(Hotel.class.getName());

	private long id;
	private String name;
	

	public Hotel() {
	}

	@Column(auto = true)
	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	@Column
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
`

#### create table
`
SQLBuilder.createOrUpdateTable(conn, new Hotel());
`

#### update data
`
SQLBuilder.insertOrUpdate(conn, hotel);
`

#### work with ResultSet
`
ResultSet rs = stm.executeQuery("select * from hotel");
if (rs.next()) {
	hotel = (Hotel) SQLBuilder.rsToBean(rs, new Hotel());
}
`


### Prerequisites

java 8 / maven

## Versioning

0.0.1

## Authors

* **Patrick Vandermaesen** - *Architect main developer*

## License

This project is licensed under GNU LESSER GENERAL PUBLIC LICENSE Version 3
