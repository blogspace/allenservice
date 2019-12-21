package com.service;

import com.entity.SessionInfo;

public interface AdminUserService {
    SessionInfo findByUserName(String userName);

}
