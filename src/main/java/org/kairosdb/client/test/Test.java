package org.kairosdb.client.test;

import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.AggregatorFactory;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.builder.TimeUnit;
import org.kairosdb.client.response.QueryResponse;

import java.io.IOException;
import java.net.MalformedURLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class Test {

    public void queryDataPoints() throws IOException {
        try(HttpClient client = new HttpClient("http://localhost:8080"))
        {
            QueryBuilder builder = QueryBuilder.getInstance();
            builder.setStart(2, TimeUnit.MONTHS)
//                    .setEnd(1, TimeUnit.MONTHS)
                    .addMetric("met_4@interface_9@device_9925")
//                    .addTag("hour","2018082322")
                    .addAggregator(AggregatorFactory
                            .createAverageAggregator(5, TimeUnit.MINUTES));
            QueryResponse response = client.query(builder);
            System.out.println(response);
        } catch (Exception e) {
           throw  e;
        }
    }
    public void push() {
        try(HttpClient client = new HttpClient("http://localhost:8080"))
        {
            long count = 0;
            while (true) {
                long t1 = System.currentTimeMillis();
                MetricBuilder builder = MetricBuilder.getInstance();
                try {
                    for (int i = 0; i < 10000l ; i ++) {
                        String device = "device_"+i;
                        for (int j = 0; j < 10; j++) {
                            String ifc = "interface_"+j;
                            for (int k = 0; k < 5; k++) {
                                String metric = "met_"+k+"@"+ifc+"@"+device;
                                double value = 10 * new Random().nextDouble();
                                double value2 = 10 * new Random().nextDouble();

                                builder.addMetric(metric)
                                        .addTag("device", device)
                                        .addTag("ifc", ifc)
                                        .addTag("day",new SimpleDateFormat("yyyyMMdd").format(new Date()))
                                        .addTag("hour",new SimpleDateFormat("yyyyMMddHH").format(new Date()))
                                        .addTag("min",new SimpleDateFormat("yyyyMMddHHmm").format(new Date()))
//                                    .addTag("customer2", "Acme2")
                                        .addDataPoint(System.currentTimeMillis(), value)
                                        .addDataPoint(System.currentTimeMillis(), value2);


                                if (count ++ % 10000 == 0) {
                                    System.out.println(count+" created");
                                }
                            }
                        }
                    }
                    client.pushMetrics(builder);
                    System.out.println(new Date()+" "+count+" pushed");
                    long t2 = System.currentTimeMillis() - t1;
                    System.out.println("spend : "+t2+"ms");
                    Thread.sleep(1000l);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws IOException {
//        new Test().push();
        new Test().queryDataPoints();
    }
}
