package com.service.impl;

import com.entity.SessionInfo;
import com.service.AdminUserService;
import org.springframework.stereotype.Service;

@Service
public class AdminUserServiceImpl implements AdminUserService {
    @Override
    public SessionInfo findByUserName(String username) {
        SessionInfo userInfo = new SessionInfo();
        userInfo.setId(1);
        userInfo.setUsername("admin");
        userInfo.setPassword("123456");
        return userInfo;
    }
}
