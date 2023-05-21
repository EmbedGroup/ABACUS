package com.iota.iri.levelbf;

import java.io.*;
import java.util.LinkedList;
import java.util.Queue;

// import org.omg.CORBA.FREE_MEM;

/**
 * 2 prefix table which contains 27*27 items
 */
public class LeveledBloomFilter {
    private String path;
    HashProvider.HashMethod HashFunction;
    public MTableItem[] table;
    // private String metadataPath = "metadata";
    int SBFsize = 4 * 1024 * 8;
    int SBFhashs = 7;// p=0.01,k=log2(1/p)
    int[] SBFcapicity=new int[32];  //capicity decrese with 0.9 rate,so to as to maiantain 0.1 FPR for 32S BFs
    SBFCache cache;
    boolean lock;

    Queue<String> ConflictAddresses;
    Queue<String> RecycledAddresses;
    boolean isRecycle;
    
    /**
     * LBF start: 1.constructed Mtable 2.loadMetadata 3.run background
     * "readinActiveSBF"
     */
    public LeveledBloomFilter(String p) {
        path=p;
        HashFunction= HashProvider.HashMethod.Murmur3KirschMitzenmacher;
        cache=new SBFCache(path, HashFunction);
        table = new MTableItem[27 * 27];
        for (int i = 0; i < 27 * 27; i++) {
            table[i] = new MTableItem(path, HashFunction);
        }
        lock = true;
        SBFcapicity[0]=3418;
        for(int i=1;i<32;i++){
            SBFcapicity[i]=(int)(SBFcapicity[i-1]*0.9);
        }
    }
    public LeveledBloomFilter(String p, HashProvider.HashMethod hf, boolean recycle) {
        path=p;
        HashFunction= hf;
        isRecycle=recycle;
        cache=new SBFCache(path, HashFunction);
        table = new MTableItem[27 * 27];
        for (int i = 0; i < 27 * 27; i++) {
            table[i] = new MTableItem(path, HashFunction);
        }
        lock = true;
        SBFcapicity[0]=3418;
        for(int i=1;i<32;i++){
            SBFcapicity[i]=(int)(SBFcapicity[i-1]*0.9);
        }
        if(isRecycle){
            ConflictAddresses=new LinkedList<>();
            RecycledAddresses=new LinkedList<>();
        }
    }


    public void Initialize() {
        File f=new File(path);
        if(!f.exists()){
            f.mkdirs();
        }
        loadMetadata();
        Thread t = new readinActiveSBF();
        t.start();
    }

    // load metadata from fixed position in disk,lisk:"LBF/metadata"
    /**
     * metadata file : fileName valid ActiveElements[0] ...... active[0] active[1]
     * ......
     */
    public int loadMetadata() {
        int valids=0;
        try {
            String meta=path+"/metadata";
            File f = new File(meta);
            if (f.exists()) {
                BufferedReader bf = new BufferedReader(new FileReader(meta));

                String line = null;

                for (int i = 0; i < 27 * 27; i++) {
                    // System.out.println(i);
                    if ((line = bf.readLine()) != null) {
                        table[i].fileName = line;
                    }
                    if ((line = bf.readLine()) != null) {
                        table[i].valid = new Integer(line).intValue() == 0 ? false : true;
                        if(table[i].valid){
                            valids++;
                        }
                    }
                    if ((line = bf.readLine()) != null) {
                        String[] ss = line.split(" ");
                        int j = 0;
                        for (String it : ss) {
                            table[i].elements[j++] = new Integer(it).intValue();
                        }
                    }
                    if ((line = bf.readLine()) != null) {
                        String[] ss = line.split(" ");
                        int j = 0;
                        for (String it : ss) {
                            table[i].active[j++] = new Integer(it).intValue();
                        }
                    }

                }

                bf.close();

                System.out.println("------finish load metadata");
                
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        return valids;
    }

    // cause import all SBF will cost many time,i.e.:69.740379s,so allow this
    // procedure run in background
    class readinActiveSBF extends Thread {

        @Override
        public void run() {
            // TODO Auto-generated method stub
            int valids = 0;
            for (int i = 0; i < 27 * 27; i++) {
                if (table[i].valid) {
                    // only read in valid GBF's active 27 SBF in
                    for (int j = 0; j < 27; j++) {
                        // SBF size=4*1024*8,hashs=log2(1/0.0.1)=7
                        BloomFilter<String> sbf = new FilterBuilder(SBFsize, SBFhashs).hashFunction(HashFunction).buildBloomFilter();
                        int offset = (table[i].active[j]) * (4 * 1024) + j * 128 * 1024;// offset=j*128KB+active[j]*4KB
                        sbf.load(table[i].fileName, offset, 4 * 1024);
                        // table[i].subBFs[j] = sbf;
                        table[i].subBFs.add(sbf);
                        table[i].inmemory[j] = true;
                    }
                    valids++;
                }
            }
            lock = false;
            
            System.out.println("Finish Initialize in Memory SBF,valids: " + valids);
            if(valids==0){
                // fackAddress();
            }
            // fackcache();
        }

    }

    class Recycler extends Thread{
        public void run(){
            while(true){
                if(isRecycle && ConflictAddresses.size()!=0){

                }
            }
        }
    }
    /**
     * shutdown: storeMetadata storedata
     */
    public void Shutdown() {
        System.out.println("Shuting down...");
        storeMetadata();
        System.out.println("    Stored metadata");
        storedata();

        System.out.printf("    cache called total:%d,miss:%d\n",cache.total,cache.miss);
    }

    public void storeMetadata() {
        try {
            String meta=path+"/metadata";
            File f = new File(meta);
            f.delete();
            f.createNewFile();
            FileOutputStream os = new FileOutputStream(meta);
            PrintWriter pw = new PrintWriter(os);
            for (int i = 0; i < 27 * 27; i++) {
                pw.println(table[i].fileName);
                if (table[i].valid) {
                    pw.println(1);
                } else {
                    pw.println(0);
                }
                String line = "";
                for (int j = 0; j < 27; j++) {
                    line += table[i].elements[j] + " ";
                }
                pw.println(line);
                line = "";
                for (int j = 0; j < 27; j++) {
                    line += table[i].active[j] + " ";
                }
                pw.println(line);
            }
            pw.close();
            os.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * only store those "valid" GBF only store those "dirty" SBF
     */
    public void storedata() {
        int dirtys = 0;
        for (int i = 0; i < 27 * 27; i++) {
            if (table[i].valid) {
                for (int j = 0; j < 27; j++) {
                    if (table[i].dirty[j]) {
                        dirtys++;
                        int offset = j * 128 * 1024 + table[i].active[j] * 4 * 1024;
                        // table[i].subBFs[j].save(table[i].fileName, offset, 4 * 1024);
                        table[i].subBFs.get(j).save(table[i].fileName, offset, 4 * 1024);
                    }
                }
            }
        }
       
        System.out.println("    stored data dirtys: " + dirtys);
    }

    /**
     * inset addr to LBF: not valid: validate GBF not in memory: load SBF update SBF
     * elements++ set dirty SBF full: flushSBF.start
     * 
     * @param addr
     */
    public void insert(String addr) {
        while (lock) {
            // System.out.printf(".");
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        int gbf = getGBFnumber(addr);
        int bf = getBFnumber(addr);
        if (!table[gbf].valid) {
            table[gbf].validate(gbf);// allocate space
        }

        if (!table[gbf].inmemory[bf]) {
            //System.out.println("bring into memory");
            int offset = 128 * 1024 * bf + table[gbf].active[bf] * 4 * 1024;
            // table[gbf].subBFs[bf].load(table[gbf].fileName, offset, 4 * 1024);
            table[gbf].subBFs.get(bf).load(table[gbf].fileName, offset, 4 * 1024);
            table[gbf].inmemory[bf] = true;
        }

        // table[gbf].subBFs[bf].add(addr);// insert
        table[gbf].subBFs.get(bf).add(addr);

        table[gbf].elements[bf]++;
        table[gbf].dirty[bf] = true;

        if (table[gbf].elements[bf] >= SBFcapicity[table[gbf].active[bf]]) {
            flushSBF t = new flushSBF(gbf, bf);
            t.run();
            // t.start();
        }
    }

    public int getGBFnumber(String addr) {
        char addr0 = addr.charAt(0);
        char addr1 = addr.charAt(1);
        int result;
        if (addr0 == '9') {
            result = 0;
        } else {
            result = addr0 - 'A' + 1;
        }
        result *= 27;
        if (addr1 == '9') {
            result += 0;
        } else {
            result += addr1 - 'A' + 1;
        }
        return result;
    }

    /**
     * 
     * @param number  corresponding prefix in Int
     * @param prefixs address prefixs length
     * @return
     */
    public String getprefix(int number, int prefixs) {
        String result = "";
        int[] t = new int[prefixs];
        for (int i = 0; i < prefixs; i++) {
            t[i] = number % 27;
            number = number / 27;
        }
        for (int i = prefixs - 1; i >= 0; i--) {
            char r;
            if (t[i] == 0) {
                r = '9';
            } else {
                r = (char) ((int) 'A' + (t[i] - 1));
            }
            result += r;
        }
        return result;
    }

   
    public int getBFnumber(String addr) {
        char addr2 = addr.charAt(2);
        if (addr2 == '9') {
            return 0;
        } else {
            return addr2 - 'A' + 1;
        }
    }

    /**
     * save the active SBF in BF[bf],GBF[gbf] to Disk bring next SBF in memory as
     * active flush current active SBF to DISK update active clear BloomFilter
     * object
     */
    class flushSBF extends Thread {
        private int gbf, bf;

        public flushSBF(int g, int b) {
            gbf = g;
            bf = b;
        }

        public void run() {
            if (table[gbf].active[bf] < 32) {
                table[gbf].inmemory[bf] = false;
                // save now active SBF
                int offset = 128 * 1024 * bf + 4 * 1024 * table[gbf].active[bf];
                // table[gbf].subBFs[bf].save(table[gbf].fileName, offset, 4 * 1024);
                table[gbf].subBFs.get(bf).save(table[gbf].fileName, offset, 4 * 1024);

                // clear old SBF,so as to be new active one
                // active++,dirty=false,elements=0
                // table[gbf].subBFs[bf].clear();
                table[gbf].subBFs.get(bf).clear();
                table[gbf].active[bf]++;
                table[gbf].elements[bf] = 0;
                /*
                 * offset = (table[gbf].active[bf]) * (4 * 1024) + bf * 128 * 1024;//
                 * offset=j*128KB+active[j]*4KB sbf.load(table[gbf].fileName, offset, 4 * 1024);
                 */
                table[gbf].dirty[bf] = false;
                table[gbf].inmemory[bf] = true;

                //System.out.printf("----For gbf:%d bf:%d,flush SBF,now active=%d\n", gbf, bf, table[gbf].active[bf]);
            }else{
                System.out.println("WARNING:one of BF is full");
            }
        }
    }

    /**
     * check if addr may exists in LBF.
     * 
     * @param addr
     * @return
     */
    public boolean check(String addr) {
        int gbf = getGBFnumber(addr);
        int bf = getBFnumber(addr);

        // System.out.printf("active:%d\n",table[gbf].active[bf]);
        if (table[gbf].subBFs.get(bf) != null) {
            if (table[gbf].subBFs.get(bf).contains(addr)) {
                return true;
            } else {
                if (table[gbf].active[bf] > 0) {
                    return cache.get(gbf, bf, table[gbf].active[bf], addr);
                } else {
                    return false;
                }
            }
        }
        return false;
    }
    /**
     * Generate 27*27*27*32*3418 address,every 32*3418 addresses belong to same BF which means they share same first 3 trytes
     * In one BF,address with their prefix,and inc from "999999.....99999"
     * insert those addresses to LBF
     */
    public void fackAddress() {
        System.out.println("---------------FACK ADDRESS");
        for (int i = 0; i < 27; i++) {
            for (int j = 0; j < 27; j++) {
                for (int l = 0; l < 27; l++) {
                    String prefix = getprefix(i * 27*27 + j*27+l, 3);
                    String addr = "99999999999999999999999999999999999999999999999999999999999999999999999999";
                    
                    int index=0;
                    for(int k=0;k<32;k++){
                        for(int m=0;m<SBFcapicity[k];m++){
                            String tail=getprefix(index+m, 4);
                            insert(prefix+addr+tail);
                        }
                        index+=SBFcapicity[k];
                    }
                }

            }
            System.out.printf("Finish %d of 27\n",i);
        }
    }
    /**
     * Get the first 32 SBFs of each GBF,so as to fill cache
     * By check every first address in 32 SBFs in first BF of each GBF
     */
    public void fackcache(){
        System.out.println("-------------FACK CACHE");
        for(int i=0;i<27;i++){
            for(int j=0;j<27;j++){
                String prefix1=getprefix(i*27+j, 2);
                String prefix2="9";
                String add="99999999999999999999999999999999999999999999999999999999999999999999999999";
                for(int k=0;k<32*3418;k+=3418){
                    String tail=getprefix(k, 4);
                    check(prefix1+prefix2+add+tail);
                }
                int index=0;
                for(int k=0;k<32;k++){
                    String tail=getprefix(index, 4);
                    check(prefix1+prefix2+add+tail);
                    index+=SBFcapicity[k];
                }
                
            }
        }
        System.out.println("Finish fack cache");
        for(int i=0;i<cache.cacheNumber;i++){
            if(cache.lists.get(i).size()!=cache.scacheSize){
                System.out.printf("sub cache %d is not full . %d\n",i,cache.lists.get(i).size());
            }
        }
    }
}
