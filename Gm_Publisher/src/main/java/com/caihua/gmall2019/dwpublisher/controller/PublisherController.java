package com.caihua.gmall2019.dwpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.caihua.gmall2019.dwpublisher.service.DauService;
import com.caihua.gmall2019.dwpublisher.service.GmvService;
import org.jcodings.util.Hash;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 实现Web接口的发布
 */
@RestController
public class PublisherController {
    //1.获取DauService对象
    @Autowired
    DauService dauService;

    @Autowired
    GmvService gmvService;

    /**
     * 统计当日日活用户数
     * @param date 当天的日期
     * @return 日活数
     */
    @GetMapping("realtime-total")
    public String getRealTimeTotal(@RequestParam("date") String date){

        //1.获取当日累计日活
        int total = dauService.getTotal(date);
        Double amount = gmvService.getAmount(date);

        //2.创建集合存放JSON对象
        ArrayList<Map> arrayList = new ArrayList<>();

        //3.创建Map用于存放日活用户数
        HashMap<String,Object> dauMap = new HashMap<>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",total);

        //4.创建Map用于存放新增数据
        HashMap<String,Object> newMidMap = new HashMap<>();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",233);

        //5.创建Map用于存放新增交易额
        HashMap<String,Object> gmvAmount = new HashMap<>();
        gmvAmount.put("id","order_amount");
        gmvAmount.put("name","新增交易额");
        gmvAmount.put("value",amount);

        //6.将封装结果
        arrayList.add(dauMap);
        arrayList.add(newMidMap);
        arrayList.add(gmvAmount);

        String s = JSON.toJSONString(arrayList);
        System.out.println(s);

        //7.返回JSON
        return JSON.toJSONString(arrayList);
    }

    /**
     * 分时统计每个小时的新登录用户数
     * @param id
     * @param date
     * @return
     */
    @GetMapping("realtime-hours")
    public String getPerHourTotal(@RequestParam("id") String id,@RequestParam("date") String date){
        //1.定义分时统计的输出结果容器
        Map<String, HashMap<String,Long>> resultMap = new HashMap<>();

        //2.创建Caleder对象，和日期格式化对象
        Calendar instance = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            instance.setTime(sdf.parse(date));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        //3.计算昨天的日期
        instance.add(Calendar.DAY_OF_MONTH,-1);
        String yesterday = sdf.format(new Date(instance.getTimeInMillis()));

        //4.定义容器
        Map todayHourTotal = null;
        Map yesterdayHourTotal = null;

        //5.接收分时统计结果
        if("dau".equals(id)){
            //① 分时统计日活
            todayHourTotal = dauService.getHourTotal(date);
            yesterdayHourTotal = dauService.getHourTotal(yesterday);
        }else if("order_amount".equals(id)){
            //② 分时统计交易额
            todayHourTotal = gmvService.getHourAmount(date);
            yesterdayHourTotal = gmvService.getHourAmount(yesterday);
        }

        //6.将分时统计后的结果放入结果容器中
        resultMap.put("yesterday", (HashMap<String, Long>) yesterdayHourTotal);
        resultMap.put("today", (HashMap<String, Long>) todayHourTotal);

        //7.返回JSON字符串
        return JSONObject.toJSONString(resultMap);
    }
}
