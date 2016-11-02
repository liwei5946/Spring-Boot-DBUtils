package com.hbcloudwide.didaoa;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.hbcloudwide.didaoa.dao.UserMapper;
import com.hbcloudwide.didaoa.domain.User;

import java.util.List;

/**
 * Created by lxq on 15-11-18.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = CloudWideApplication.class)
public class TestDruid {

    Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private UserMapper userMapper;

    @Test
    public void Test1(){
        List<User> user = userMapper.getAllUser();
        logger.info(user.get(0).toString());
    }
}
