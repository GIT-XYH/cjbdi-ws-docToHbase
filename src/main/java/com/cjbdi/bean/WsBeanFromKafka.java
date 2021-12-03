package com.cjbdi.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

/**
 * @Author: XYH
 * @Date: 2021/12/3 4:37 下午
 * @Description: TODO
 */
@Data
@ToString
@AllArgsConstructor
public class WsBeanFromKafka {
    private WsBean wsBean;
    private String base64File;
}
