package com.caihua.gmall2019.dwpublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
@Data
@AllArgsConstructor
public class Stat {
    String title;
    List<Option> options;
}

