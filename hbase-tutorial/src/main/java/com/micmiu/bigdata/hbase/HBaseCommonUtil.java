package com.micmiu.bigdata.hbase;


import java.nio.charset.Charset;

/**
 * Hbase Table common handler method
 * User: <a href="http://micmiu.com">micmiu</a>
 * Date: 7/7/2015
 * Time: 23:27
 */
public class HBaseCommonUtil {

	public final static String DEF_ENCODING = "UTF-8";

	public static Charset getCharset(String encoding) {
		return ((null == encoding || "".equals(encoding.trim())) ? Charset.forName(DEF_ENCODING) : Charset.forName(encoding));
	}

}
