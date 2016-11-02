package com.hbcloudwide.didaoa.service;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.hbcloudwide.didaoa.dao.DbUtilsTemplate;
import com.hbcloudwide.didaoa.dao.UserMapper;
import com.hbcloudwide.didaoa.domain.User;
import com.hbcloudwide.didaoa.utils.ObjToolsUtil;
import com.hbcloudwide.didaoa.utils.RedisUtil;

/**
 * Created by lxq on 15-11-19.
 */
//public interface UserService {
//
//    User getUserById(long id);
//
//    void saveUser(User user);
//
//    void updateUser(User user);
//
//    void deleteUserById(long id);
//
//    List<User> getAllUser();
//
//
//}
@Service
public class UserService{
//	private static Logger log = LoggerFactory.getLogger(UserService.class);
	 private final Logger log = LoggerFactory.getLogger(this.getClass());
	 
	 @Autowired
	 private DbUtilsTemplate dbu; 

	
	/**
	 * 获取所有用户信息
	 * @return
	 */
	public List<User> getAllUser() {
		List<User> userList = null;
		String sql = "SELECT * FROM user";
		userList = dbu.find(User.class, sql);
		return userList;
	}
	
	public  void updateUser() {
		
	}
}
