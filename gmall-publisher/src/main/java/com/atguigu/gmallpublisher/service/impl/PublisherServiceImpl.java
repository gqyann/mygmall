package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    private DauMapper dauMapper;

    @Override
    public Integer getDauTotal(String date) {

        return dauMapper.selectDauTotal(date);//结果是一个数
    }

    @Override
    public Map getDauHourTotal(String date) {
        List<Map> totalHourMap = dauMapper.selectDauTotalHourMap(date);
        //结果  LH  CT
        //     23   392

        //改变数据结构
        HashMap<String, Long> result = new HashMap<>();
        for (Map map : totalHourMap) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }

        return result;
    }
}
