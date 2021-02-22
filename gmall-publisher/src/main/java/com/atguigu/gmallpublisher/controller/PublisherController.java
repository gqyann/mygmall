package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class PublisherController {
    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getDauTotal(@RequestParam("date") String date) {
        ArrayList<Map> result = new ArrayList<>();
        HashMap<String, Object> dauMap = new HashMap<>();
        HashMap<String, Object> devMap = new HashMap<>();

        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", publisherService.getDauTotal(date));

        devMap.put("id", "new_mid");
        devMap.put("name", "新增设备");
        devMap.put("value", 233);

        result.add(dauMap);
        result.add(devMap);
        return JSON.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String getDauHourTotal(@RequestParam("id")String id,
                                  @RequestParam("date")String date){
        Map todayMap = publisherService.getDauHourTotal(date);
        //拿到根据今天的日期获取昨天日期
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();
        Map yesterdayMap = publisherService.getDauHourTotal(yesterday);
        //保存结果数据
        HashMap<String, Map> result = new HashMap<>();

        result.put("today", todayMap);
        result.put("yesterday", yesterdayMap);

        return JSON.toJSONString(result);
    }
}
