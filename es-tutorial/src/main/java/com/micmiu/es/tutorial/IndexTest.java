package com.micmiu.es.tutorial;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.search.SearchHit;

import java.util.Iterator;
import java.util.Map;

/**
 * Created
 * User: <a href="http://micmiu.com">micmiu</a>
 * Date: 6/5/2015
 * Time: 17:33
 */
public class IndexTest {

	private Client client = null;

	public static void startup(){

	}


	/**
	 *
	 * @throws Exception
	 */
	public void createIndex() throws Exception {
		try {

			try {
				// 预定义一个索引
				client.admin().indices().prepareCreate("app").execute().actionGet();

				// 定义索引字段属性
				XContentBuilder mapping = XContentFactory.jsonBuilder().startObject();
				mapping = mapping.startObject("title")
						// 创建索引时使用paoding解析
						.field("indexAnalyzer", "paoding")
								// 搜索时使用paoding解析
						.field("searchAnalyzer", "paoding")
						.field("store", "yes")
						.endObject();
				mapping = mapping.endObject();

				PutMappingRequest mappingRequest = Requests.putMappingRequest("app").type("article").source(mapping);
				client.admin().indices().putMapping(mappingRequest).actionGet();
			}
			catch (IndexAlreadyExistsException e) {
				System.out.println("索引库已存在");
			}

			// 生成文档
			XContentBuilder doc = XContentFactory.jsonBuilder().startObject();
			doc = doc.field("title", "java附魔大师");
			doc = doc.endObject();

			// 创建索引
			IndexResponse response = client.prepareIndex("app", "article", "1").setSource(doc).execute().actionGet();

			System.out.println(response.getId() + "====" + response.getIndex() + "====" + response.getType());
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			client.close();
		}
	}


	public void search() throws Exception {
		try {
			QueryBuilder qb = QueryBuilders.termQuery("title", "大师");
			SearchResponse scrollResp = client.prepareSearch("app").setSearchType(SearchType.SCAN).setScroll(
					new TimeValue(60000)).setQuery(qb).setSize(100).execute().actionGet();

			while (true) {
				scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
				for (SearchHit hit : scrollResp.getHits()) {
					Map<String, Object> source = hit.getSource();
					if (!source.isEmpty()) {
						for (Iterator<Map.Entry<String, Object>> it = source.entrySet().iterator(); it.hasNext();) {
							Map.Entry<String, Object> entry = it.next();
							System.out.println(entry.getKey() + "=======" + entry.getValue());

						}
					}

				}
				if (scrollResp.getHits().getTotalHits() == 0) {
					break;
				}

			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			client.close();
		}

	}
}
