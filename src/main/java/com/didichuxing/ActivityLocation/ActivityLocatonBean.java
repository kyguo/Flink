package com.didichuxing.ActivityLocation;

import com.didichuxing.demo.mysqlConnect.stream.ActivityBean;

public class ActivityLocatonBean {
    public String userId;
    public Double lat;
    public Double lng;
    public String location;

    public ActivityLocatonBean(){};
    public ActivityLocatonBean(String userId,Double lat,Double lng,String location){
        this.userId = userId;
        this.lat = lat;
        this.lng = lng;
        this.location = location;
    }
    @Override
    public String toString(){
        return "ActivityBean( " + "userId = " + userId + ", location = " + location + ")";
    }

    public static ActivityLocatonBean of(String userId,Double lat,Double lng,String location){
        return new ActivityLocatonBean(userId, lat,lng,location);
    }
}
