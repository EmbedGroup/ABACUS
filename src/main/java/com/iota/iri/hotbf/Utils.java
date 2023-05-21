package com.iota.iri.hotbf;


import com.iota.iri.metrics.ConsoleReporter;
import com.iota.iri.metrics.MetricAttribute;
import com.iota.iri.metrics.MetricRegistry;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class Utils {

    public static String IntToTrytes(int x,int length){
        if(x > Math.pow(27, length)){
            System.out.println("index outbound");
            return null;
        }
        String Trytes="";
        for(int i=0;i<length;i++){
            int k=x%27;
            x=x/27;
            if(k==0){
                Trytes='9'+Trytes;
            }
            else{
                Trytes=(char)('A'+k-1)+Trytes;
            }
        }
        return Trytes;
    }

    public static int TrytesToInt(String addr,int length){
        int result=0;
        for(int i=0;i<length;i++){
            char k=addr.charAt(i);
            if(k=='9'){
                result+=0;
            }else{
                result+=(int)(k-'A')+1;
            }
            if(i!=length-1)   result*=27;
        }
        return result;
    }
    public static ConsoleReporter getreport(MetricRegistry registry){
        Set<MetricAttribute> dis=new HashSet<>();
        dis.add(MetricAttribute.MAX);
        dis.add(MetricAttribute.MIN);
        dis.add(MetricAttribute.STDDEV);
        dis.add(MetricAttribute.P50);
        dis.add(MetricAttribute.P75);
        dis.add(MetricAttribute.P95);
        dis.add(MetricAttribute.P98);
        dis.add(MetricAttribute.P99);
        dis.add(MetricAttribute.P999);
        dis.add(MetricAttribute.M1_RATE);
        dis.add(MetricAttribute.M5_RATE);
        dis.add(MetricAttribute.M15_RATE);
        dis.add(MetricAttribute.MEAN_RATE);

        ConsoleReporter reporter = ConsoleReporter.forRegistry(registry).convertRatesTo(TimeUnit.MICROSECONDS)
                .convertDurationsTo(TimeUnit.MICROSECONDS).disabledMetricAttributes(dis).build();
        return reporter;

    }

}