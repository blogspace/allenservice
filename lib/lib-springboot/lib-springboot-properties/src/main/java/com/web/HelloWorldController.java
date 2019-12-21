package com.web;

import com.properties.HomeProperties;
import com.properties.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Spring Boot HelloWorld 案例
 * <p>
 * Created by bysocket on 16/4/26.
 */
@Controller
public class HelloWorldController {
    @Autowired
    private HomeProperties homeProperties;

    @RequestMapping("/")
    public String sayHello() {
        return "Hello,World!";
    }

    @RequestMapping("/test")
    public String sayTest() {
        return homeProperties.toString();
    }

    //@RequestMapping("/indexDemo")
    public ModelAndView indexPage() {
        ModelAndView model = new ModelAndView();
        model.setViewName("index");
        List<User> userList = new ArrayList<>();
        User user = new User(1,"安琪拉",12);
        User user2 = new User(2,"虞姬",14);
        User user3 = new User(3,"公孙离",13);
        User user4 = new User(4,"蔡文姬",11);
        userList.add(user);
        userList.add(user2);
        userList.add(user3);
        userList.add(user4);
        //model.addObject("message", userList);
        model.addObject("userList", userList);

        return model;
    }

    @GetMapping("/demo")
    public String index1(Map<String, Object> data) {
        data.put("message","helloooooooooooooooooooooooooo");
        return "index";
    }
}
