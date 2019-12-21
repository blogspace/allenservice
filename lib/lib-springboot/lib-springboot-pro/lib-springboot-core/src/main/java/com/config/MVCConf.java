package com.config;

import org.beetl.core.GroupTemplate;
import org.beetl.ext.spring.BeetlGroupUtilConfiguration;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class MVCConf implements WebMvcConfigurer, InitializingBean {

    public static final String DEFAULT_APP_NAME = "开发平台";
    /**
     * 系统名称,可以在application.properties中配置
     * app.name=xxx
     */
    // 开发用的模拟当前用户和机构
    Long useId;

    Long orgId;

    String mvcTestPath;

    @Autowired
    Environment env;

    @Autowired
    BeetlGroupUtilConfiguration beetlGroupUtilConfiguration;

    @Autowired
    GroupTemplate groupTemplate;

    @Override
    public void afterPropertiesSet() throws Exception {
        this.useId = env.getProperty("user.id", Long.class);
        this.orgId = env.getProperty("user.orgId", Long.class);
        this.mvcTestPath = env.getProperty("mvc.test.path");
        Map<String, Object> var = new HashMap<>(5);
        String appName =  env.getProperty("app.name");
        if(appName==null) {
        	 var.put("appName",DEFAULT_APP_NAME);
        }
        var.put("jsVer", System.currentTimeMillis());
        var.put("search", System.currentTimeMillis());

        groupTemplate.setSharedVars(var);

    }
}
