import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class SparkEsDemo {
    public static void main(String[] args) throws UnknownHostException {
        run();
    }

    public static void run() throws UnknownHostException {
        SparkSession sparkSession = SparkSession
            .builder()
            .appName("SparkEsTestDemo")
            .config("es.nodes", "xxx.xxx.xxx.xxx")
            .config("es.port", "9200")
            //重试5次（默认3次）
            .config("es.batch.write.retry.count", "5")
            //等待60秒（默认10s）
            .config("es.batch.write.retry.wait", "60")
            //es连接超时时间100s（默认1m）
            .config("es.http.timeout", "200s")
            .getOrCreate();

        // 方法一：通过将es的数据读取为Dataset<Row>
        String esResource = "esIndex/esType"; // 换成你自己的索引和索引类型
        Dataset<Row> esDf = JavaEsSparkSQL.esDF(sparkSession, esResource) // resource参数填写es的索引和索引类型
            .agg(functions.max("fieldName")) // 到了这里就相当于直接将es的数据转化为Dataset<Row>直接做计算了，很直观
            .withColumnRenamed("max(fieldName)", "maxFieldName")
            .na().fill(0);
        esDf.show(false);

        // 方法二：通过编写Query查询语句查询es数据，返回的是JavaPairRDD<String, Map<String, Object>>
        String query = "{\"query\":{\"bool\":{}},\"size\":0,\"aggs\":{\"max_price\":{\"max\":{\"field\":\"price\"}}}}";
        JavaPairRDD<String, Map<String, Object>> esRdd = JavaEsSpark.esRDD(JavaSparkContext.fromSparkContext(sparkSession.sparkContext()), esResource, query);
        esRdd.take(10).forEach(f -> {
            Map<String, Object> map = f._2();
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                System.out.println(entry.getKey() + entry.getValue());
            }
        });

        // 方法三：通过es客户端的低阶API实现聚合查询
        // https://www.cnblogs.com/benwu/articles/9230819.html
        Properties prop = new Properties();
        PreBuiltTransportClient esClient = initClient(prop);
        SearchResponse searchResponse = esClient
            .prepareSearch("recommend_smartvideo_spider_acgn")
            .setTypes("smartvideo_type_user_irrelative")
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setExplain(true)
            .setSize(0)
            .addAggregation(AggregationBuilders.max("max_cursor").field("inx_cursor"))
            .setFetchSource(false)
            .get();
        Aggregation maxCursor = searchResponse.getAggregations().get("max_cursor");
        System.out.println(maxCursor.getType());
    }

    /**
     * 初始化es客户端
     * @param prop es配置类
     * @return PreBuiltTransportClient
     */
    public static PreBuiltTransportClient initClient(Properties prop) throws UnknownHostException {
        Settings settings = Settings.builder()
            .put("cluster.name", prop.getProperty("es.cluster.name"))
            .put("client.transport.sniff", true)
            .build();
        PreBuiltTransportClient client = new PreBuiltTransportClient(settings);
        String[] hostAndPortArr = prop.getProperty("es.host&port").split(",");
        for (String hostPort : hostAndPortArr) {
            String[] hp = hostPort.split(":");
            if (hp.length == 2) {
                client.addTransportAddress(
                    new TransportAddress(InetAddress.getByName(hp[0]), Integer.parseInt(hp[1]))
                );
            } else {
                client.addTransportAddress(
                    new TransportAddress(InetAddress.getByName(hp[0]), 9300)
                );
            }
        }
        return client;
    }
}
