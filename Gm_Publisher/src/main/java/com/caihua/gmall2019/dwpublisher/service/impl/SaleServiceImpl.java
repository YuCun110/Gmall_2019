package com.caihua.gmall2019.dwpublisher.service.impl;

import com.caihua.constants.GmallConstants;
import com.caihua.gmall2019.dwpublisher.service.SaleService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
@Service
public class SaleServiceImpl implements SaleService {
    @Autowired
    JestClient jestClient;

    @Override
    public Map<String, Object> getSaleDetail(String date, Integer startpage, Integer size, String keyword) {
        //根据输入的参数，对ES进行查询
        //1.创建查询语句对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //2.构建查询语句
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        //① 过滤：dt = "2020-02-25"
        boolQueryBuilder.filter(new TermsQueryBuilder("dt", date));
        //② 匹配：“小米手机”
        MatchQueryBuilder sku_name = new MatchQueryBuilder("sku_name", keyword);
        sku_name.operator(Operator.AND);
        boolQueryBuilder.must(sku_name);

        searchSourceBuilder.query(boolQueryBuilder);
        //③ 按照性别分组
        TermsAggregationBuilder group_by_gender = AggregationBuilders.terms("group_by_gender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(group_by_gender);
        //④ 按照年龄分组
        TermsAggregationBuilder group_by_age = AggregationBuilders.terms("group_by_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(group_by_age);

        //⑤ 分页
        searchSourceBuilder.from((startpage - 1) * size);
        searchSourceBuilder.size(size);

        //2.创建Search对象
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstants.ES_INDEX_SALE_DETAIL).addType("_doc").build();

        //3.执行查询
        //定义返回的结果容器
        SearchResult result = null;
        //执行
        try {
            result = jestClient.execute(search);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //4.解析查询的结果集
        //① 定义返回结果的容器
        HashMap<String, Object> resultMap = new HashMap<>();
        //② 获取结果的总条数
        Long total = result.getTotal();
        resultMap.put("total", total);

        //③ 获取按照性别分组的结果
        TermsAggregation genderGreoup = result.getAggregations().getTermsAggregation("group_by_gender");

        //创建Map集合，将分组聚合后的结果存入
        HashMap<String, Long> groupByGenderMap = new HashMap<>();
        //遍历结果，存放数据
        for (TermsAggregation.Entry bucket : genderGreoup.getBuckets()) {
            groupByGenderMap.put(bucket.getKey(), bucket.getCount());
        }
        //将最终结果保存
        resultMap.put("genderMap", groupByGenderMap);

        //④ 获取按照年龄分组的结果
        TermsAggregation genderAge = result.getAggregations().getTermsAggregation("group_by_age");
        //创建Map集合，将分组聚合后的结果存入
        HashMap<String, Long> groupByAgeMap = new HashMap<>();
        //遍历结果，存放数据
        for (TermsAggregation.Entry bucket : genderAge.getBuckets()) {
            groupByAgeMap.put(bucket.getKey(), bucket.getCount());
        }
        //将最终结果保存
        resultMap.put("ageMap", groupByAgeMap);

        //⑤ 获取详细销售订单
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        //创建List集合，将详细数据保存
        ArrayList<Map> detailList = new ArrayList<>();
        //遍历元素
        for (SearchResult.Hit<Map, Void> hit : hits) {
            detailList.add(hit.source);
        }
        //将最终结果保存
        resultMap.put("detail", detailList);

        //5.返回解析后的结果集
        return resultMap;
    }
}
