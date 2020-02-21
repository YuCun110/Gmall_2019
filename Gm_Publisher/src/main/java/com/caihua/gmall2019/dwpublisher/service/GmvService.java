package com.caihua.gmall2019.dwpublisher.service;

import java.util.Map;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
public interface GmvService {
    Double getAmount(String date);

    Map getHourAmount(String date);
}
