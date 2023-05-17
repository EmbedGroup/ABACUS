package com.iota.iri.bloomfilters;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;

/**
 * one Item keep metadata of a GBF,27*128KB
 * filePath: the path of this entire 27*128KB file.
 * valid:whether this part of BFs is valid.valid if once insert
 * active[27]:int,current active 4KB sub_BF number in this item
 * byte[27][4*1024]:real data of active 4KB sub_BF
 * elements:number of elements in active SBF,this must be save in metadata
 */
public class MTableItem {
    String path;
    HashProvider.HashMethod HashFunction;
    String fileName;
    boolean valid;
    int[] active=new int[27];
    boolean[] inmemory=new boolean[27];//used to make sure specific SBF has been imported into memory
    ArrayList<BloomFilter<String>> subBFs=new ArrayList<>(27);
    boolean[] dirty=new boolean[27];//sbf is dirty,if once insert after last flush .used to save time in  shotdown
    int[] elements=new int[27];
    MTableItem(String p, HashProvider.HashMethod hf){
        path=p;
        HashFunction=hf;
        //when MtableItem firt constructed all data still in Disk,only then readinData,inmemory[] are set true
        for(int i=0;i<27;i++){
            inmemory[i]=false;
            active[i]=0;
            dirty[i]=false;
            elements[i]=0;
            
        }
        fileName="";
        valid=false;
        
    }
    /**
     * validate this Group Bloom Filter
     * pre-allocate 27*128KB space 
     * build 27 SBF
     * inmemory=true
     * valid=true
     */
    public void validate(int groupID){
        fileName=path+"/GBF"+groupID;
        File f=new File(fileName);
        try {
            if(!f.exists()){
                f.createNewFile();
                RandomAccessFile rf=new RandomAccessFile(f, "rw");
                rf.setLength(27*128*1024);//27*128KB
                rf.close(); 
                for(int i=0;i<27;i++){
                    BloomFilter<String> bf=new FilterBuilder(4*1024*8, 7).hashFunction(HashFunction).buildBloomFilter();
                    subBFs.add(bf);
                    inmemory[i]=true;
                }
                
                //System.out.println("------validate GBF: "+groupID);
            }
           
        } catch (Exception e) {
            //TODO: handle exception
            e.printStackTrace();
        }
        valid=true;
    }

    //accroding to faileName,
    public void readinData(){

    }

    //insert one address to this part of BFs
    public void insert(String address){
        if(!valid){
            //first address in this 27*128KBs
            
        }
    }
}