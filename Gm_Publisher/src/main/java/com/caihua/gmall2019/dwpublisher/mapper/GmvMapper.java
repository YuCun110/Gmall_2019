package com.caihua.gmall2019.dwpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
public interface GmvMapper {
    Double getAmount(String date);

    List<Map> getHourAmount(String date);
}
