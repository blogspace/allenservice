package com.controller;

import com.entity.SessionInfo;
import com.service.impl.AdminUserServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.util.Enumeration;

@RestController
public class LoginController {

    @Autowired
    private AdminUserServiceImpl adminUserService;

    /**
     * 设置默认打开地址http://localhost:8020的跳转(需要在拦截器中排除)
     * 1.已登录，跳转到index.html，把adminUserInfo返回前端渲染
     * 2.未登录，跳转到登录页
     *
     * @return
     */
    @RequestMapping(value = "/")
    public ModelAndView index(HttpServletRequest request, ModelAndView view) {
        HttpSession session = request.getSession();
        SessionInfo sessionInfo = (SessionInfo) session.getAttribute("sessionInfo");
        if (null != sessionInfo) {
            return new ModelAndView("index.html");
        } else {
            return new ModelAndView("login.html");
        }
    }

    /**
     * 登录(需要在拦截器中排除)
     * 1.已登录，跳转到index.html，把adminUserInfo返回前端渲染
     * 2.未登录，验证密码，如果密码正确，跳转到index.html, 则把用户信息放入session，并把adminUserInfo返回前端渲染
     * 如果密码错误，跳转到login.html
     *
     * @return
     */
    @RequestMapping(value = "/login")
    public ModelAndView login(HttpServletRequest request) {
        //先验证session，再验证密码
        String username = request.getParameter("username");
        String password = request.getParameter("password");
        HttpSession session = request.getSession();
        SessionInfo sessionInfo = (SessionInfo) session.getAttribute("sessionInfo");
        if (null != sessionInfo) {
            return new ModelAndView("index.html");
        } else {
            //验证密码
            SessionInfo admin = adminUserService.findByUserName(username);
            System.out.println("验证登录...userName=" + username + "userPwd=" + password);
            if (admin.getPassword().equals(password)) {
                //用户信息放入session
                System.out.println("登录...成功..." + "用户名：" + username);
                request.getSession().setAttribute("sessionInfo", admin);
                return new ModelAndView("index.html");
            } else {
                System.out.println("登录...密码输入错误..." + "用户名：" + username);
                return new ModelAndView("login.html");
            }
        }
    }

    @RequestMapping("/logout")
    public ModelAndView logout(HttpServletRequest request) {
        HttpSession session = request.getSession();
        System.out.println("退出："+session);
        Enumeration eum = session.getAttributeNames();
        while (eum.hasMoreElements()) {
            String key = (String) eum.nextElement();
            session.removeAttribute(key);
        }
        System.out.println("退出2："+session);
        ModelAndView view = new ModelAndView("redirect:/");
        return view;
    }

}
