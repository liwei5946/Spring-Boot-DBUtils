package com.hbcloudwide.didaoa.dao;


import java.util.List;

import com.hbcloudwide.didaoa.domain.User;

/**
 * Created by zl on 2015/8/27.
 */
public interface UserMapper {

    public User getUserById(long id);

    public void saveUser(User user);

    public void updateUserById(User user);

    public void deleteUserById(long id);

    public List<User> getAllUser();
}
