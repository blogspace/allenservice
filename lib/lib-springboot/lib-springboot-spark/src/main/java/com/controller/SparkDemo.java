package com.controller;

import com.service.SparkDemoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SparkDemo {
    @Autowired
    SparkDemoService sparkDemoService;

    @RequestMapping("/")
    public String index() {
        return "hello hello";
    }

    @RequestMapping("/demo")
    public void demo() {
        //sparkDemoService.index();
    }

}
