package com.caihua.test;

import com.caihua.bean.Teacher;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.awt.*;
import java.io.IOException;
import java.util.ArrayList;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 单条数据写入：
 * 1.按照Kibana方式写入；
 * 2.通过对象方式写入
 */
public class ES_Write01 {
    public static void main(String[] args) throws IOException {
        //1.创建es客户端工厂对象
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.创建ES客户端连接地址对象
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop202:9200").build();

        //3.设置ES客户端连接的配置信息
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //4.创建ES客户端对象
        JestClient jestClient = jestClientFactory.getObject();

        //5.构建ES客户端插入数据
        //① 按照Kibana方式插入数据
        Index index = new Index.Builder("{\n" +
                "  \"t_id\":\"1002\",\n" +
                "  \"name\":\"lisi\",\n" +
                "  \"salary\":18889,\n" +
                "  \"gender\":\"female\",\n" +
                "  \"desc\":[\"刻苦\",\"严谨\",\"好学\"]\n" +
                "}")
                .index("teacher_index")
                .type("_doc")
                .id("1002")
                .build();

        //6.执行插入操作
        jestClient.execute(index);

        //7.关闭连接
        jestClient.shutdownClient();
    }
}
