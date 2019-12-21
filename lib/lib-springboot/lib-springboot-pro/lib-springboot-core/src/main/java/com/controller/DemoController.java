package com.controller;

import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import java.util.Date;

@RestController
public class DemoController {

    @RequestMapping("/index001")
    public static String index() {
        return "hello";
    }


    @RequestMapping("/index002")
    public static ModelAndView demo() {
        ModelAndView modelAndView = new ModelAndView("ind.html");
        return modelAndView;
    }

    @RequestMapping("/index003")
    public static ModelAndView demo2() {
        ModelAndView view = new ModelAndView("index.html");

        return view;
    }

    @RequestMapping("/index004")
    public static ModelAndView demo3() {
        ModelAndView view = new ModelAndView("login.html");

        return view;
    }

    @RequestMapping("/index005")
    public static void demo4(HttpServletRequest request, Model model) {
        String name = request.getParameter("username");
        String pwd = request.getParameter("password");
        System.out.println("name:"+name);
        System.out.println("pwd:"+pwd);
        System.out.println("demo:"+request.getRequestURI());
//        User user = userService.findByUsername(name);
//        System.out.println("********************************");
//        if (user == null) {
//            request.getSession().setAttribute("result", "-1");
//            return new ModelAndView("SystemLogin");
//        } else if (!user.getPassword().equals(pwd)) {
//            request.getSession().setAttribute("result", "-2");
//            return new ModelAndView("SystemLogin");
//        } else {
////登录成功后跳转到主页面
//            request.getSession().removeAttribute("result");
//            request.getSession().setAttribute("user", user);
//            return new ModelAndView("default");
//
//        }

    }

    @RequestMapping("/test")
    public ModelAndView getTest() {
        ModelAndView modelAndView = new ModelAndView("test.html");
        modelAndView.addObject("userName", "xiao wang");
        return modelAndView;

    }
}
