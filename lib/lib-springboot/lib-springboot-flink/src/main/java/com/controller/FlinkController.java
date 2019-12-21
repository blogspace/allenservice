package com.controller;

import com.service.FlinkService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class FlinkController {
    @Autowired
    private FlinkService flinkService;

    @RequestMapping("/demo")
    public String demo() {
        return "hello world";
    }

    @RequestMapping("/indexdemo")
    public void indexDemo() {
        flinkService.index();
    }
}
