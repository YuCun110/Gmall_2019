package com.caihua.gmall2019.dwpublisher.service;

import java.util.Map;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */

public interface DauService {

    public int getTotal(String date);

    public Map<String,Long> getHourTotal(String date);
}
