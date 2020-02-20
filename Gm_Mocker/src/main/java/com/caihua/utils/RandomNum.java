package com.caihua.utils;

import java.util.Random;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
public class RandomNum {
    public static int getRandInt(int fromNum,int toNum){
        return   fromNum+ new Random().nextInt(toNum-fromNum+1);
    }
}
