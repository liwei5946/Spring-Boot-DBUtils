package com.hbcloudwide.didaoa.controller.restApi;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.hbcloudwide.didaoa.domain.User;
import com.hbcloudwide.didaoa.service.UserService;


/**
 * Created by lxq on 15-11-19.
 */
@RestController
@RequestMapping("/api")
public class UserApi {

    private final String baseUrl = "/api";

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private UserService testService;



    /**
     * 新增
     * @param test
     * @return
     */
//    @RequestMapping(value = "/test", method = RequestMethod.POST)
//    public void addProduct(@RequestBody User test) {
//        testService.saveUser(test);
//    }



    /**
     * 获取单个商品详情
     * @param id
     * @return
     */
//    @RequestMapping(value = "/test/{id}", method = RequestMethod.GET)
//    public User product(@PathVariable("id") String id) {
//        long l_id = Long.parseLong(id);
//        return testService.getUserById(l_id);
//    }

    /**
     * 编辑
     * @param id
     * @param test
     * @return
     */
//    @RequestMapping(value = "/test/{id}", method = RequestMethod.PUT)
//    public void modifyProduct(@PathVariable("id") String id,
//                                            @RequestBody User test) {
//        testService.updateUser(test);
//    }

    /**
     * 删除商品
     * @param id
     * @return
     */
//    @RequestMapping(value = "/test/{id}", method = RequestMethod.DELETE)
//    public void removeProduct(@PathVariable("id") String id) {
//        long l_id = Long.parseLong(id);
//        testService.deleteUserById(l_id);
//    }

    /**
     * 获取所有商品
     * @return
     */
    @RequestMapping(value = "/tests", method = RequestMethod.GET)
    public List<User> products() {
        return testService.getAllUser();
    }
}
