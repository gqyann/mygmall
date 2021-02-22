package com.atguigu.demo.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

//@Controller  + @ResponseBody 等于下面的RestController
@RestController
public class ControllerTest {
    @RequestMapping("test")
//    @ResponseBody
    public String test(){
        System.out.println("123");
        return "success";
    }

    @RequestMapping("test11")
    public String test22(@RequestParam("name")String name, @RequestParam("age")Integer age){
        System.out.println(name+":"+age);
        return "success";
    }
}
