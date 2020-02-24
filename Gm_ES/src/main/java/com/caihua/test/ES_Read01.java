package com.caihua.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 读取ES中的查询结果：
 * 1.通过Kibana的方式读取查询结果；
 * 2.通过工具
 */
public class ES_Read01 {
    public static void main(String[] args) throws IOException {
        //1.创建ES客户端工厂对象
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.创建ES连接地址对象
        HttpClientConfig build = new HttpClientConfig.Builder("http://hadoop202:9200").build();

        //3.设置ES客户端连接地址
        jestClientFactory.setHttpClientConfig(build);

        //4.创建ES对象
        JestClient jestClient = jestClientFactory.getObject();

        //5.创建Search对象
        Search search = new Search.Builder("{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"gender\": \"male\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"must\": [\n" +
                "        {\"match\": {\n" +
                "          \"favo\": \"球\"\n" +
                "        }}\n" +
                "      ]\n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"group_class\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"class_id\",\n" +
                "        \"size\": 10\n" +
                "      }\n" +
                "    },\n" +
                "    \"max_age\":{\n" +
                "      \"max\": {\n" +
                "        \"field\": \"age\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"from\": 0,\n" +
                "  \"size\": 20\n" +
                "}")
                .build();

        //6.执行查询操作
        SearchResult result = jestClient.execute(search);

        //7.解析查询的结果
        //① 结果的概述：
        System.out.println("查询结果共：" + result.getTotal() + "条。");
        System.out.println("结果的分数：" + result.getMaxScore() + "分。");
        //② 查询到的详情(获取hits标签对应数据)
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);

        for (SearchResult.Hit<Map, Void> hit : hits) {
            //a.结果中该条记录的Index
            System.out.println("Index：" + hit.index);
            //b.编号
            System.out.println("编号：" + hit.id);
            //c.详情
            JSONObject json = new JSONObject();
            Map source = hit.source;
            for (Object o : source.keySet()) {
                json.put(o.toString(),source.get(o));
            }
            System.out.println(json);
        }
        //③ 解析聚合组
        MetricAggregation aggregations = result.getAggregations();
        System.out.println("最大年龄：" + aggregations.getMaxAggregation("max_age").getMax());
        List<TermsAggregation.Entry> group_class = aggregations.getTermsAggregation("group_class").getBuckets();
        for (TermsAggregation.Entry groupClass : group_class) {
            System.out.println(groupClass.getKey() + "->" + groupClass.getCount());
        }

        //8.关闭连接
        jestClient.shutdownClient();
    }
}
