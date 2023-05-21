package com.iota.iri.levelbf;

import java.util.Random;
public class RecycledDemo {
    static String STR="ABCDEFGHIJKLMNOPQRSTUVWXYZ9";
    public static String randomAddress(int l){
        Random random=new Random();
        StringBuffer sb=new StringBuffer();
        for(int i=0;i<l;i++){
            int number=random.nextInt(27);
            sb.append(STR.charAt(number));
        }
        return sb.toString();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("recycle demo.....");
        int size=4*1024*8;  
        int k=7;
        BloomFilter<String> sbf = new FilterBuilder(size, k).buildBloomFilter();

        FilterBuilder fb2=new FilterBuilder(size, k);
        fb2.hashFunction(HashProvider.HashMethod.MD5);
        BloomFilter<String> sbf2 = fb2.buildBloomFilter();

        FilterBuilder fb3 = new FilterBuilder(size, k);
        fb3.hashFunction(HashProvider.HashMethod.SHA256);
        BloomFilter<String> sbf3 = fb3.buildBloomFilter();

        FilterBuilder compared_fb=new FilterBuilder(size, k);
        compared_fb.hashFunction(HashProvider.HashMethod.MD5);
        BloomFilter<String> compared_bf = compared_fb.buildBloomFilter();

        int fp=0;
        int recy_1=0;
        int recy_2=0;
        int fpr_compared_sbf=0;

        for(int i=0;;i++){
            if(i%100==0){
                // Math.pow(1-Math.pow(Math.E, -1*k*i/size), k)
                System.out.printf("%d %f %f %f %f\n", i,Double.valueOf(fp)/i, Double.valueOf(recy_1)/i, Double.valueOf(recy_2)/i, Double.valueOf(fpr_compared_sbf)/i);
                
            }
            if(Double.valueOf(fp)/i > 0.5){
                break;
            }
            // String addr=randomAddress(81);
            String addr=String.valueOf(i);
            
            if(sbf.contains(addr)){
                fp++;
                if(!sbf2.contains(addr)){
                    recy_1++;
                }else{
                    if(!sbf3.contains(addr)){
                        recy_2++;
                    }
                }
            }
            if(compared_bf.contains(addr)) fpr_compared_sbf++;

            sbf.add(addr);
            sbf2.add(addr);
            sbf3.add(addr);
            compared_bf.add(addr);
        }

 /*
        LeveledBloomFilter lbf=new LeveledBloomFilter("LBF");
        lbf.Initialize();

        LeveledBloomFilter back=new LeveledBloomFilter("BACK", HashProvider.HashMethod.SHA256);
        back.Initialize();

        String addr_1=randomAddress(81);

        lbf.insert(addr_1);
        if(lbf.check(addr_1)){
            System.out.println("contain");
        }

        back.insert(addr_1);
        if(back.check(addr_1)){
            System.out.println("back check");
        }

        lbf.Shutdown();
        back.Shutdown();*/
    }
}
