package com.caihua.test;

import com.alibaba.fastjson.JSONObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 读取ES中的查询结果：
 *  * 1.通过Kibana的方式读取查询结果；
 *  * 2.通过工具
 */
public class ES_Read02 {
    public static void main(String[] args) throws IOException {
        //1.创建ES工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.创建ES连接地址对象
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop202:9200").build();

        //3.设置ES连接地址
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //4.创建ES对象
        JestClient jestClient = jestClientFactory.getObject();

        //5.创建查询语句对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //4.构建查询语句
        //bool
        BoolQueryBuilder bool = new BoolQueryBuilder();
        bool.filter(new TermQueryBuilder("gender","male"));
        bool.must(new MatchQueryBuilder("favo","球"));
        searchSourceBuilder.query(bool);

        //aggs
        TermsAggregationBuilder group_class = new TermsAggregationBuilder("group_class", ValueType.LONG);
        group_class.field("class_id");
        group_class.size(10);
        searchSourceBuilder.aggregation(group_class);

        //max
        MaxAggregationBuilder max_age = new MaxAggregationBuilder("max_age");
        max_age.field("age");
        searchSourceBuilder.aggregation(max_age);

        //分页
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);

        //5.创建Search对象
        Search search = new Search.Builder(searchSourceBuilder.toString()).build();

        //6.执行查询
        SearchResult result = jestClient.execute(search);

        //7.解析结果数据
        //结果概述
        System.out.println("结果总数：" + result.getTotal() + "条。");
        System.out.println("最高评分：" + result.getMaxScore());

        //结果来源
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            System.out.println("Index：" + hit.index);
            System.out.println("数据ID：" + hit.id);
            Map source = hit.source;
            JSONObject jsonObject = new JSONObject();
            for (Object o : source.keySet()) {
                jsonObject.put(o.toString(),source.get(o));
            }
            System.out.println(jsonObject);
        }

        //聚合结果
        MaxAggregation max_age1 = result.getAggregations().getMaxAggregation("max_age");
        System.out.println("最大年龄：" + max_age1.getMax());
        TermsAggregation group_class1 = result.getAggregations().getTermsAggregation("group_class");
        List<TermsAggregation.Entry> buckets = group_class1.getBuckets();
        for (TermsAggregation.Entry bucket : buckets) {
            System.out.println(bucket.getKey() + "-> "+ bucket.getCount());
        }

        //7.关闭连接
        jestClient.shutdownClient();
    }
}
