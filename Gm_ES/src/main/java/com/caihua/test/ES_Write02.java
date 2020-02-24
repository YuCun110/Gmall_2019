package com.caihua.test;

import com.caihua.bean.Teacher;
import com.caihua.bean.Teacher02;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 单条数据写入：
 *  * 1.按照Kibana方式写入；
 *  * 2.通过对象方式写入
 */
public class ES_Write02 {
    public static void main(String[] args) throws IOException {
        //1.创建ES客户端工厂对象
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.创建ES客户端链接地址对象
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop202:9200").build();

        //3.设置ES客户端连接地址
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //4.创建ES客户端对象
        JestClient jestClient = jestClientFactory.getObject();

        //5.构建ES客户端插入数据
        //① 通过对象的方式插入数据
        //Teacher teacher = new Teacher("1002", "wangwu", 18234, "female", "细心，全面");
        ArrayList<String> desc = new ArrayList<>();
        desc.add("全面");
        desc.add("刻苦");
        Teacher02 teacher = new Teacher02("1002", "wangwu", 18234, "female", desc);
        //② 创建Index对象
        Index index = new Index.Builder(teacher)
                .index("teacher_index")
                .type("_doc")
                .id("1004")
                .build();

        //6.执行插入操作
        jestClient.execute(index);

        //7.关闭连接
        jestClient.shutdownClient();
    }
}
