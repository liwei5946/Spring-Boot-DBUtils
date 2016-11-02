package com.hbcloudwide.didaoa.domain;

/**
 * Created by zl on 2015/8/27.
 */
public class User {

    private long id;
    private String user_name;
    private String pass_word;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getUser_name() {
        return user_name;
    }

    public void setUser_name(String user_name) {
        this.user_name = user_name;
    }

    public String getPass_word() {
        return pass_word;
    }

    public void setPass_word(String pass_word) {
        this.pass_word = pass_word;
    }

    @Override
    public String toString(){
        return "[id : "+id+", user_name : "+user_name+", pass_word : "+pass_word+"]";
    }
}
