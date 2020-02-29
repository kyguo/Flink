package com.didichuxing.AsyncActivityLocation;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.didichuxing.ActivityLocation.ActivityLocatonBean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

public class AsyncAccessMapFunction extends RichAsyncFunction<String, ActivityLocatonBean> {
    private transient CloseableHttpAsyncClient httpclient = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(3000)
                .setConnectTimeout(3000)
                .build();
        httpclient = HttpAsyncClients.custom()
                .setMaxConnTotal(20)
                .setDefaultRequestConfig(requestConfig).build();
        httpclient.start();
    }

    @Override
    public void asyncInvoke(String value, ResultFuture<ActivityLocatonBean> resultFuture) throws Exception {
        String[] fields = value.split(",");
        String userId = fields[0];
        Double lat = Double.parseDouble(fields[1]);
        Double lng = Double.parseDouble(fields[2]);
        String url = "https://restapi.amap.com/v3/geocode/regeo?output=json&location="+lng +"," + lat + "&key=b9c3dc795effd8bc01a19c12e95154aa&radius=1000&extensions=base";
        final HttpGet request = new HttpGet(url);
        Future<HttpResponse> future = httpclient.execute(request, null);

        CompletableFuture.supplyAsync(new Supplier<String>() {

            @Override
            public String get() {
                try {
                    String province = null;
                    HttpResponse response = future.get();
                    if (response.getStatusLine().getStatusCode() == 200) {
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

                    return province;
                } catch (Exception e) {
                    // Normally handled explicitly.
                    return null;
                }
            }
        }).thenAccept( (String province) -> {
            resultFuture.complete(Collections.singleton(ActivityLocatonBean.of(userId,lat,lng,province)));
        });
    }


    @Override
    public void close() throws Exception {
        super.close();
        httpclient.close();
    }

}
