package com.micmiu.es.tutorial.model;

/**
 * Created
 * User: <a href="http://micmiu.com">micmiu</a>
 * Date: 6/5/2015
 * Time: 13:45
 */
public class User {

	private Long id;

	private String name;

	private Integer age;

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
