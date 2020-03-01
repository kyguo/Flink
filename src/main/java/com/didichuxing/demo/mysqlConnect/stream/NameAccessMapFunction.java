package com.didichuxing.demo.mysqlConnect.stream;

import com.didichuxing.demo.mysqlConnect.stream.ActivityBean;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.sql.*;

public class NameAccessMapFunction extends RichMapFunction<String, ActivityBean> {
    private transient Connection connection = null;//如果这一步在open里面声明的话，那在close里面就拿不到它了
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //需要引入mysql-connector-java包
         connection = DriverManager.getConnection("jdbc:mysql://localhost/sunxiaoxiao?characterEncoding=UTF-8", "root", "123456");
    }


    @Override
    public ActivityBean map(String value) throws Exception {
        String[] fields = value.split(",");
        String userId = fields[0];
        String actId = fields[1];
        String actTime = fields[2];

        PreparedStatement preparedStatement = connection.prepareStatement("SELECT name FROM activity_mapping WHERE id = ?");
        preparedStatement.setString(1,actId);
        ResultSet resultSet = preparedStatement.executeQuery();

        String name = null;
        while (resultSet.next()){
            //获取指定列的值，如下表示获取第一列的值（也就是name，name的类型是string的）
            name = resultSet.getString(1);
        }
        preparedStatement.close();

        return ActivityBean.of(userId,name,actTime);
    }



    @Override
    public void close() throws Exception {
        super.close();
        connection.close();

    }

}

