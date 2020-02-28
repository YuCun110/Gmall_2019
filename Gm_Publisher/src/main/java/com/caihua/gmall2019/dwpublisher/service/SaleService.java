package com.caihua.gmall2019.dwpublisher.service;

import org.springframework.web.bind.annotation.RequestParam;

import java.util.Map;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
public interface SaleService {
    Map getSaleDetail(String date, Integer startpage, Integer size, String keyword);
}
