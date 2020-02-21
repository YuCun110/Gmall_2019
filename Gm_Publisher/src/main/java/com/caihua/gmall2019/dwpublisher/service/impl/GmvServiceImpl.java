package com.caihua.gmall2019.dwpublisher.service.impl;

import com.caihua.gmall2019.dwpublisher.mapper.GmvMapper;
import com.caihua.gmall2019.dwpublisher.service.GmvService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
@Service
public class GmvServiceImpl implements GmvService {
    @Autowired
    GmvMapper gmvMapper;

    @Override
    public Double getAmount(String date) {
        //1.获取Hbase中查询的日交易总额
        Double amount = gmvMapper.getAmount(date);
        //2.返回查询结果
        return amount;
    }


    @Override
    public Map getHourAmount(String date) {
        //1.定义容器，存放最终统计结果
        Map<String,Double> resultMap = new HashMap<>();

        //2.获取Hbase中查询的结果
        List<Map> hourAmount = gmvMapper.getHourAmount(date);

        //3.对统计结果进行解析，放入容器中
        for (Map map : hourAmount) {
            resultMap.put((String) map.get("LH"),(Double) map.get("TA"));
        }

        //4.返回统计结果
        return resultMap;
    }
}
