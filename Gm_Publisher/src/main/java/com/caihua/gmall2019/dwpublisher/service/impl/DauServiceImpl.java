package com.caihua.gmall2019.dwpublisher.service.impl;

import com.caihua.gmall2019.dwpublisher.mapper.DauMapper;
import com.caihua.gmall2019.dwpublisher.service.DauService;
import org.jcodings.util.Hash;
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
public class DauServiceImpl implements DauService {
    //1.获取DauMapper对象
    @Autowired
    DauMapper dauMapper;


    @Override
    public int getTotal(String date) {
        return dauMapper.getRealTimeTotal(date);
    }

    @Override
    public Map<String,Long> getHourTotal(String date) {
        //1.接收sql查询返回的数据
        List<Map> listMap = dauMapper.getHourTotal(date);

        //2.变换数据结构：list<Map> - Map
        HashMap<String,Long> hashMap = new HashMap<>();
        for (Map map : listMap) {
            hashMap.put(map.get("LH").toString(),(Long) map.get("CT"));
        }

        //3.返回结果
        return hashMap;
    }
}
