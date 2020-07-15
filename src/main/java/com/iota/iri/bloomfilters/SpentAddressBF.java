package com.iota.iri.bloomfilters;

import java.io.File;
import java.util.ArrayList;
import java.util.List;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SpentAddressBF{
    private static final Logger log = LoggerFactory.getLogger(SpentAddressBF.class);
    private int bytesPerBF;
    private int BFNum; 
    private int expectedElements;
    private double[] falseProbabilityPerBF=new double[49];
    private final List<BloomFilter<String>> BFs=new ArrayList<>();
    String storePath;
    /**
     * 初始化布隆过滤器个数，并指定每个过滤器的预期最大元素数和falsePositiveProbability，之后hash function将自动产生
     * 注意若是使用的bytePerBF不能整除49，那么最后一个过滤器的长度会比其他的小，但是元素数量和其他是一样的，所以碰撞的几率会更大，也许需要考虑更小的falseProbability
     * @param _bytesPerBF 每个过滤器需要负载的一个address中的一部分，指定每部分大小。一个address转换为byte形式后具有49Byte
     * @param expectedElements 每个过滤器的预期最大元素数
     * @param falseProb 每个过滤器的falsePositiveProbability
     */
    public SpentAddressBF(int bytes,int expectedelements,double[] falseProb,String path){
        bytesPerBF=bytes;
        expectedElements=expectedelements;
        BFNum=bytesPerBF==1 ? 49 : (49/bytesPerBF)+1;
        for(int i=0;i<BFNum;i++){
            falseProbabilityPerBF[i]=falseProb[i];        
        }

        storePath=path;

    }
    /**
     * 默认的定义，expectedElement=2^(bytePerBF*8),falsePositiveProbability=0.01,path="./BFS"
     * @param _bytesPerBF
     */
    public SpentAddressBF(int bytes){
        bytesPerBF=bytes;
        expectedElements=1<<bytesPerBF*8;
        BFNum=bytesPerBF==1 ? 49 : (49/bytesPerBF)+1;
        for(int i=0;i<BFNum;i++){
            falseProbabilityPerBF[i]=0.01;        
        }
        falseProbabilityPerBF[BFNum-1]=0.005;
        storePath="./BFS";
        
    }
    /**
     * 使用参数创建过滤器.如果目录下不存在BFS文件夹就创建，否则尝试从中恢复过滤器的状态
     */
    public void buildBF(){
        for(int i=0;i<BFNum;i++){
            BloomFilter<String> bf=new FilterBuilder(expectedElements,falseProbabilityPerBF[i]).buildBloomFilter();
            BFs.add(bf);
        }
        File f=new File(storePath);
        if(!f.exists()){
            f.mkdirs();
        }else{
            //目录下原先存在文件
            loadBFs();
        }
    }
    /**
     * 每次关机会将过滤器状态保存在硬盘中，开机后需要从文件重新读取过滤器快照到内存中
     */
    public void loadBFs(){
        for(int i=0;i<BFNum;i++){
            String paths=storePath+"/"+i;
            BFs.get(i).load(paths);
        }
    }
    /**
     * 关机时需要将过滤器中的快照状态保存在本地
     */
    public void saveBFs(){
        for(int i=0;i<BFNum;i++){
            String paths=storePath+"/"+i;
            BFs.get(i).save(paths);
        }
    }
    /**
     * 将地址分段存入各自的布隆过滤器
     * @param object
     */
    public void add(String address){
        log.info("add address:"+address);
        byte[] addr=address.getBytes();
        int i;
        for(i=0;i<BFNum-1;i++){
            BFs.get(i).add(new String(addr, i*bytesPerBF, bytesPerBF));
        }
        BFs.get(BFNum-1).add(new String(addr, i*bytesPerBF,49-i*bytesPerBF));
    }
    /**
     * 判断address是否可能存在
     * @param address
     * @return 返回false表示肯定不存在，true表示可能存在
     */
    public boolean mayExist(String address){
        byte[] addr=address.getBytes();
        int i;
        for(i=0;i<BFNum-1;i++){
            System.out.println("check bf: "+i);
            if(!BFs.get(i).contains(new String(addr, i*bytesPerBF, bytesPerBF))){
                return false;
            }
        }
        if(!BFs.get(BFNum-1).contains(new String(addr, i*bytesPerBF,49-i*bytesPerBF))){
            return false;
        }
        return true;
    }

}