/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package eu.socialsensor.utils;

/**
 * Created by zhangsuochao on 17/6/22.
 */
public class HugeGraphUtils {

    public static final String ID_DELEMITER = ":";

    public static String createId(String vertexLabel, String key) {
        StringBuilder sb = new StringBuilder(vertexLabel);
        sb.append(HugeGraphUtils.ID_DELEMITER);
        sb.append(key);
        return sb.toString();
    }

    public static boolean isStringEmpty(String str) {
        return str == null || "".equals(str.trim());
    }
}
