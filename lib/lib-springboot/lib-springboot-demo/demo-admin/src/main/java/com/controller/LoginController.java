package com.controller;

import com.dao.User;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.util.Enumeration;

import static org.apache.logging.log4j.message.MapMessage.MapFormat.JSON;


@RestController
public class LoginController {
//    @CrossOrigin
//    @PostMapping(path = "/login")
//    public String login(@RequestBody User user){
//        System.out.println(user.getUsername() + " + " + user.getPassword());
//        if("123".equals(user.getUsername()) && "123456".equals(user.getPassword())){
//            return "success";
//        }
//        System.out.println("name:"+user.getUsername());
//
//        return "fail";
//    }
    @RequestMapping("/api/login")
    public String demo(HttpServletRequest request) {
        System.out.println(request.getParameter("username"));
        System.out.println( request.getParameter("password"));
        return "success";
    }

    /**
     * 登录控制器，前后端分离用的不同协议和端口，所以需要加入@CrossOrigin支持跨域。
     * 给VueLoginInfoVo对象加入@Valid注解，并在参数中加入BindingResult来获取错误信息。
     * 在逻辑处理中我们判断BindingResult知否含有错误信息，如果有错误信息，则直接返回错误信息。
     */
//    @CrossOrigin
    //    @RequestMapping(value = "/api/login", method = RequestMethod.POST, produces = "application/json; charset=UTF-8")
//    @ResponseBody
//    public Result login(@Valid @RequestParam VueLoginInfoVo loginInfoVo, BindingResult bindingResult) {
//        System.out.println("------" + loginInfoVo + " " + bindingResult);
//        if (bindingResult.hasErrors()) {
//            String message = String.format("登陆失败，详细信息[%s]。", bindingResult.getFieldError().getDefaultMessage());
//            return ResultFactory.buildFailResult(message);
//        }
//        if (!Objects.equals("javalsj", loginInfoVo.getUsername()) || !Objects.equals("123456", loginInfoVo.getPassword())) {
//            String message = String.format("登陆失败，详细信息[用户名、密码信息不正确]。");
//            return ResultFactory.buildFailResult(message);
//        }
//        return ResultFactory.buildSuccessResult("登陆成功。");
//    }

}
