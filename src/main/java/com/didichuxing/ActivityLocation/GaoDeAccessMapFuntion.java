package com.didichuxing.ActivityLocation;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class GaoDeAccessMapFuntion extends RichMapFunction<String,ActivityLocatonBean> {
    private transient CloseableHttpClient httpclient = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        httpclient = HttpClients.createDefault();
    }

    @Override
    public ActivityLocatonBean map(String value) throws Exception {
        String[] fields = value.split(",");
        String userId = fields[0];
        Double lat = Double.parseDouble(fields[1]);
        Double lng = Double.parseDouble(fields[2]);
        String province = null;
        try {
            String url = "https://restapi.amap.com/v3/geocode/regeo?output=json&location="+lng + lat + "&key=b9c3dc795effd8bc01a19c12e95154aa&radius=1000&extensions=base";
            HttpGet httpGet = new HttpGet(url);
            CloseableHttpResponse response = httpclient.execute(httpGet);
            //获取返回码
            int status = response.getStatusLine().getStatusCode();
            if (status == 200) {
                //获取reponse字符串
                String jsonStr = EntityUtils.toString(response.getEntity());
                //将字符串解析成JsonObject
                JSONObject jsonObject = JSON.parseObject(jsonStr);
                //获取key为regeocode的body，并以JsonObject形式返回
                JSONObject regeocode = jsonObject.getJSONObject("regeocode");
                //若返回不为空，则解析出城市
                if (regeocode != null && !regeocode.isEmpty()) {
                    JSONObject addressComponent = regeocode.getJSONObject("addressComponent");
                    province = addressComponent.getString("province");
                }
            }
            return ActivityLocatonBean.of(userId,lat,lng,province);
        } finally {
            httpclient.close();
            return ActivityLocatonBean.of(userId,lat,lng,province);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }


}
