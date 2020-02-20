package com.caihua.gmall.gmlogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.caihua.constants.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;


/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 注：@RestController = @Controller + @ResponseBody (所有方法都返回字符串)
 */
//@Controller
@Slf4j
@RestController
public class LoggerController {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @PostMapping("log")
    public String receive(@RequestParam("logString") String value){
        //1.使用服务器时间，在Json字符串中补充时间戳
        //① 将获取到的JSON字符串，封装为JSON对象
        JSONObject jsonObject = JSON.parseObject(value);
        //② 添加时间戳
        jsonObject.put("dt",System.currentTimeMillis());

        //2.使用Logger对象(借助Slf4j注解)，将JSON字符串按照info级别在本地服务器落盘
        String jsonString = jsonObject.toJSONString();
        log.info(jsonString);

        //3.将数据发送至Kafka集群
        if("startup".equals(jsonObject.getString("type"))){
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP,jsonString);
        }else {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT,jsonString);
        }

        return "success";
    }
}
