package com.hbcloudwide.didaoa.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.hbcloudwide.didaoa.domain.User;
import com.hbcloudwide.didaoa.service.UserService;

@Controller
@RequestMapping("/hello")
public class HelloController {
	private static Logger log = LoggerFactory.getLogger(HelloController.class);
	
	@Autowired
	private UserService userService;

    @RequestMapping
    public String hello() {
        return "Hello Spring-Boot";
    }

    @RequestMapping("/info")
    public Map<String, String> getInfo(@RequestParam String name) {
        Map<String, String> map = new HashMap<>();
        map.put("name", name);
        return map;
    }


    @RequestMapping("/list")
    public String list(Model model) {
        List<User> user = userService.getAllUser();
        model.addAttribute("user",user);
        return "/user/list";
    }
    
    
    
}