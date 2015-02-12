package com.micmiu.hive;

public class TestMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String str =  "michael|@^_^@|hadoop|@^_^@|http://www.micmiu.com/opensource/hadoop/hive-metastore-config/";
		System.out.println(str.replaceAll("\\|@\\^_\\^@\\|", "\001"));

	}

}
