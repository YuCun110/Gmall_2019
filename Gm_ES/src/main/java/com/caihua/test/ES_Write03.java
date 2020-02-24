package com.caihua.test;

import com.caihua.bean.Teacher;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 批量写入操作：Bulk
 */
public class ES_Write03 {
    public static void main(String[] args) throws IOException {
        //1.创建ES客户端工厂对象
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.创建ES客户端连接地址对象
        HttpClientConfig buid = new HttpClientConfig.Builder("http://hadoop202:9200").build();

        //3.设置ES客户端连接地址
        jestClientFactory.setHttpClientConfig(buid);

        //4.创建ES客户端对象
        JestClient jestClient = jestClientFactory.getObject();

        //5.构建Index对象
        //创建自定义对象
        Teacher teacher01 = new Teacher("1005", "zhaoliu", 17894, "male", "聪明，能干");
        Teacher teacher02 = new Teacher("1006", "qianqi", 13475, "male", "聪明，细心");
        //创建Index
        Index index01 = new Index.Builder(teacher01).index("teacher_index").type("_doc").id("1005").build();
        Index index02 = new Index.Builder(teacher02).index("teacher_index").type("_doc").id("1006").build();

        //6.创建Bulk对象
        Bulk bulk = new Bulk.Builder()
                .addAction(index01)
                .addAction(index02)
                .build();

        //7.批量导入数据
        jestClient.execute(bulk);

        //8.关闭连接
        jestClient.shutdownClient();
    }
}
