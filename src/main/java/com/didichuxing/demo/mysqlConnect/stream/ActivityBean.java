package com.didichuxing.demo.mysqlConnect.stream;

public class ActivityBean {
    public String userId;
    public String actName;
    public String actTime;
    public ActivityBean(){};
    public ActivityBean(String userId, String actName ,String actTime){
        this.userId = userId;
        this.actName = actName;
        this.actTime = actTime;
    }
    @Override
    public String toString(){
        return "ActivityBean( " + "userId = " + userId + ", actName = " + actName + ", actTime = " + actTime + " )";
    }

    public static ActivityBean of(String userId, String actName ,String actTime){
       return new ActivityBean(userId, actName, actTime);
    }
}
