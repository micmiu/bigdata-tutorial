package com.micmiu.hive.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;

/**
 * Created
 * User: <a href="http://micmiu.com">micmiu</a>
 * Date: 8/12/2015
 * Time: 13:41
 */
public class HiveConnDbcpMnagerTest {


	public static void main(String[] args) throws Exception {
		HiveConnDbcpManager manager = HiveConnDbcpManager.getInstance();
		//test1(manager.getConnection());
		for(int i=0;i<5;i++){
			test2(manager.getConnection());
		}
		manager.close();
	}

	public static void test2(Connection connection) throws Exception {
		System.out.println(">>>>> : "+connection);
		PreparedStatement st = connection.prepareStatement("select msisdn, cerno, city_id,reg_date from userinfo where msisdn = ?");
		st.setString(1, "00"+ new Random().nextInt(5));
		ResultSet res = st.executeQuery();
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2) + "\t" + res.getString(3) + "\t" + res.getInt(4));
		}
		st.close();
		connection.close();
	}

	public static void test1(Connection connection) throws Exception {
		Statement st = connection.createStatement();
		//drop if exists table
		String tableName1 = "gsm";
		String dropIfExistsTable = "drop table if exists " + tableName1;
		st.execute(dropIfExistsTable);
		//create table
		st.execute("set mapred.reduce.tasks=4");
		String createTable = "create table " + tableName1 + " tblproperties('cache'='ram','filters'='hashbucket(4):msisdn') as select * from gsm_ext distribute by msisdn";
		st.execute(createTable);
		//make queries on existing tables gsm and userinfo
		String tableName2 = "userinfo";
		String sql1 = "select count(1) from " + tableName2;
		ResultSet res = st.executeQuery(sql1);
		while (res.next()) {
			System.out.println(String.valueOf(res.getInt(1)));
		}
		String sql2 = "select msisdn, count(1) as count from " + tableName1 + " group by msisdn order by count";
		res = st.executeQuery(sql2);
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + String.valueOf(res.getInt(2)));
		}
		st.close();
		connection.close();
	}

}
