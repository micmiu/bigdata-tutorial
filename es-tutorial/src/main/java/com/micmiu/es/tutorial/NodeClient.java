package com.micmiu.es.tutorial;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

/**
 * Created
 * User: <a href="http://micmiu.com">micmiu</a>
 * Date: 6/5/2015
 * Time: 14:18
 */
public class NodeClient {

	private Node node = null;
	private Client client = null;

	public void startup() {


//		NodeBuilder nodeBuilder = new NodeBuilder();
//		NodeBuilder nodeBuilder = NodeBuilder.nodeBuilder();

		//是否加载配置文件
		//NodeBuilder nodeBuilder = NodeBuilder.nodeBuilder().loadConfigSettings(true);

		//cluster.name  in elasticsearch.yml
		//NodeBuilder nodeBuilder = NodeBuilder.nodeBuilder().clusterName("yourclustername")

		//是否只作为客户端，即不存储索引数据，默认值为false
		NodeBuilder nodeBuilder = NodeBuilder.nodeBuilder().client(true);

		Node node = nodeBuilder.node();
		client = node.client();
	}

	public void shutdown() {
		if (null != node) {
			node.close();
		}
	}

	public static void main(String[] args) {
		NodeClient client = new NodeClient();
		client.startup();
		System.out.println(client);
		client.shutdown();
	}


}
