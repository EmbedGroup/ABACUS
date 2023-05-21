package com.iota.iri.hotbf;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.iota.iri.hotbf.GroupBloomFilter.mayExistsResponce;
import com.iota.iri.metrics.ConsoleReporter;
import com.iota.iri.metrics.MetricRegistry;
import com.iota.iri.metrics.Timer;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;


public class HotBF {
    private int prefixLength;
    private int BFUsize;
    private int hashFunctions;
    private int BFunits;
    double P;
    private int limitedSize;
    private int BlockNumber;
    public int[] Scales;// scale times for Group Bloom Filters
    private int Scaled = 0;// Scaled=max(Scales)
    //ConcurrentLinkedQueue<Integer> BlockLRU = new ConcurrentLinkedQueue<>();
    ConcurrentLinkedHashMap<Integer,Integer> BlockLRU;
    HashMap<Integer, GroupBloomFilter> BlockMap = new HashMap<>();

    public MetricRegistry metrics = new MetricRegistry();
    public ConsoleReporter reporter = Utils.getreport(metrics);

    public int remainder;// left space in memory
    Timer eliminateT = metrics.timer("HotBF eliminate");
    Timer iniT = metrics.timer("HotBF ini");
    Timer insertT = metrics.timer("HotBF insert");
    Timer mayexistsT = metrics.timer("HotBF mayExists");
    Timer savadataT = metrics.timer("HotBF savadata");
    Timer scaleT = metrics.timer("HotBF scale");
    Timer shutT = metrics.timer("HotBF shutdown");
    Timer tryEliminateT = metrics.timer("HotBF tryEliminate");
    Timer totalActiveT = metrics.timer("HotBF totalActive");
    Timer BlockLRUT=metrics.timer("HotBF BlockLRU");
    /**
     * 
     * @param prefixlength
     * @param bfusize
     * @param hashfunctions
     * @param p
     * @param limitedsize   Bloom filters size in memory threhold(bits)
     */
    public void ini(int prefixlength, int bfusize, int hashfunctions, double p, int limitedsize) {
        Timer.Context c = iniT.time();

        prefixLength = prefixlength;
        BFUsize = bfusize;
        hashFunctions = hashfunctions;
        P = p;
        limitedSize = limitedsize;
        remainder = limitedsize / BFUsize;

        BlockNumber = (int) Math.pow(27, prefixLength);
        GroupBloomFilter gb = new GroupBloomFilter(BFUsize, hashFunctions, P);
        BFunits = gb.BFUnits;
        Scales = new int[BlockNumber];
        // build block map
        // At First,no Active BFU

        // load metadata
        File f = new File("HBF-Meta");
        if (!f.exists()) {
            // Bootstrap,Initialize HBF file
            try {
                f.createNewFile();
                File data = new File("HBF0");
                if (!data.exists()) {
                    data.createNewFile();
                    RandomAccessFile rf = new RandomAccessFile(data, "rw");
                    rf.setLength((long) gb.Size() * BlockNumber / 8);
                    rf.close();
                } else {
                    System.out.println("ERROR:HBF but without HBF-Meta");
                }

            } catch (Exception e) {
                // TODO: handle exception
                e.printStackTrace();
            }
            // initialize GroupBloomFIlters
            for (int i = 0; i < BlockNumber; i++) {
                GroupBloomFilter g = new GroupBloomFilter(BFUsize, hashFunctions, P, this, i);
                g.setPath("HBF" + 0);
                g.iniBFULRU();
                BlockMap.put(i, g);
            }
            BlockLRU=new ConcurrentLinkedHashMap.Builder<Integer,Integer>().maximumWeightedCapacity(BlockNumber).build();
            // initialize BlockLRU,BFULRU
            for (int i = 0; i < BlockNumber; i++) {
                BlockLRU.put(i, i);
            }

        } else {
            try {
                // Not bootstrap,Load metadata
                BufferedReader br = new BufferedReader(new FileReader(f));
                // Scales
                String[] scas = br.readLine().split(" ");
                for (int i = 0; i < BlockNumber; i++) {
                    Scales[i] = Integer.valueOf(scas[i]);
                    if (Scales[i] > Scaled)
                        Scaled = Scales[i];
                }
                // BLockLRU
                BlockLRU=new ConcurrentLinkedHashMap.Builder<Integer,Integer>().maximumWeightedCapacity(BlockNumber*(Scaled+1)).build();
                String queue = br.readLine();
                String[] q = queue.split(" ");
                for (String s : q) {
                    BlockLRU.put(Integer.valueOf(s),Integer.valueOf(s));
                }
                
                // add Scaled Blocks
                // entitis | active BFUs
                // BFULRU
                // load in active BFU
                for (int i = 0; i <= Scaled; i++) {
                    File data = new File("HBF" + i);
                    if (!data.exists()) {
                        System.out.println("ERROR:NO DATA");
                        return;
                    }
                    RandomAccessFile ra = new RandomAccessFile(data, "r");
                    byte[] tmp = new byte[BFUsize];

                    for (int j = 0; j < BlockNumber; j++) {
                        // metadata
                        GroupBloomFilter g = new GroupBloomFilter(BFUsize, hashFunctions, 5 + i, this, j);
                        g.setPath("HBF" + i);
                        String meta1 = br.readLine();
                        String meta2 = br.readLine();
                        g.loadmeta(meta1, meta2);

                        remainder -= g.Actives();

                        // load in active BFU
                        for (int k = 0; k < g.BFUnits; k++) {
                            if (g.isActive(k)) {
                                // BlockMap.get(i * BlockNumber + k).LoadInBFU(j); massive cost way
                                BloomFilter<String> bf = new FilterBuilder(BFUsize, hashfunctions).buildBloomFilter();

                                ra.seek( (((long)j * g.Size()) + k * BFUsize) / 8);
                                ra.read(tmp);
                                bf.setBloom(tmp);
                                g.Group.put(k, bf);
                            }
                        }
                        // add block
                        BlockMap.put(i * BlockNumber + j, g);
                    }
                    ra.close();
                }
                //System.out.println("BlockMap size "+BlockMap.size());
                br.close();
            } catch (Exception e) {
                // TODO: handle exception
                e.printStackTrace();
            }

        }
        // reporter.start(5, TimeUnit.SECONDS);
        c.close();
    }

    // save all Active BFU
    // save metaData
    public void ShutDown() {
        Timer.Context c = shutT.time();
        /*
         * for (int i = 0; i < BlockMap.size(); i++) { BlockMap.get(i).ShutDown(); }
         */
        savaData();

        File meta = new File("HBF-Meta");
        try {
            if (meta.exists())
                meta.delete();

            BufferedWriter bw = new BufferedWriter(new FileWriter(meta, true));
            // Scales
            for (int i = 0; i < BlockNumber; i++) {
                bw.write(Scales[i] + " ");
            }
            bw.write("\n");

            // BlockLRU
            Set<Integer> lru=BlockLRU.ascendingKeySet();//Cold->Hot
            Iterator<Integer> it = lru.iterator();
            while (it.hasNext()) {
                bw.write(it.next() + " ");
            }
            bw.write("\n");

            for (int i = 0; i < BlockMap.size(); i++) {
                BlockMap.get(i).savemeta(bw);
            }
            bw.close();
        } catch (Exception e) {
            // TODO: handle exception
        }
        c.close();
    }

    public void WarmUp() {
        // bring all of GBF's first BFU into memory
        // RAW bootstrap
        int threshold = limitedSize / BFUsize;
        if (BlockNumber < threshold)
            threshold = BlockNumber;

        for (int i = 0; i < threshold; i++) {
            String addr = Utils.IntToTrytes(i, prefixLength) + "AAAAAAA";
            mayExists(addr);
        }
        int activeB=0;
        for(int i=0;i<BlockNumber;i++){
            if(BlockMap.get(i).Size()>=1){
                activeB++;
            }
        }
        System.out.println("warmup:"+activeB);
        reporter.report();
        GroupBloomFilter.reporter.report();
    }

    public void savaData() {
        Timer.Context c = savadataT.time();
        for (int i = 0; i <= Scaled; i++) {
            String path = "HBF" + i;
            File f = new File(path);
            try {
                if (!f.exists()) {
                    System.out.println("DATA File Not Exists");
                }

                RandomAccessFile rs = new RandomAccessFile(f, "rw");
                for (int j = 0; j < BlockNumber; j++) {
                    GroupBloomFilter g = BlockMap.get(i * BlockNumber + j);
                    for (int k = 0; k < g.BFUnits; k++) {
                        if (g.isActive(k)) {
                            rs.seek( ((long)j * g.Size() + k * BFUsize) / 8);
                            rs.write(g.getBFU(k).getBloomAsByteArray());
                        }
                    }
                }
                rs.close();
            } catch (Exception e) {
                // TODO: handle exception
            } // 47ms
        }
        c.close();
    }

    public int prefixLength() {
        return prefixLength;
    }

    public ConcurrentLinkedHashMap<Integer, Integer> BlockLRU() {
        return BlockLRU;
    }

    public void Eliminated(int numbers) {
        Timer.Context c = eliminateT.time();
        // choose from numbers oldest Block(with BFUs>0 in memory),everyone choose
        // oldest BFU to remove
        Set<Integer> lru=BlockLRU.ascendingKeySet();
        Iterator<Integer> it = lru.iterator();
        int target = numbers;

        // gather enough activeBFUs
        int activeGBF = 0;
        int activeBFU = 0;
        while (it.hasNext()) {
            int t = it.next();
            int BFUs;
            if(BlockMap.get(t)==null){
                System.out.println("NULL GOT "+t);
            }
            if ((BFUs = BlockMap.get(t).Actives()) > 0) {
                activeGBF++;
                activeBFU += BFUs;
                if (activeGBF >= numbers)
                    break;
            }
        }
        it = lru.iterator();
        if (activeGBF >= numbers) {
            // everyone eliminate one BFU
            while (target > 0) {
                int t = 0;
                if (it.hasNext()) {
                    t = it.next();
                } else {
                    System.out.println("Eliminated No Target");
                }

                GroupBloomFilter gb = BlockMap.get(t);
                if (gb.isActive()) {
                    target -= gb.EliminateBFU(1);
                    remainder++;
                }
            }
        } else if (activeBFU >= numbers) {
            // eliminate all left BFUs
            while (target > 0) {
                int t = 0;
                t = it.next();
                GroupBloomFilter gb = BlockMap.get(t);
                int left = gb.Actives();
                if (left > 0) {
                    if (left > target) {
                        left = target;
                    }
                    int removed = gb.EliminateBFU(left);
                    target -= removed;
                    remainder += removed;
                }
            }
        } else {
            System.out.println("NO ENOUGH BFU TO ELIMINATE");
        }
        c.close();
    }

    // Total Active BFUs in memory
    public int TotalActives() {
        Timer.Context c = totalActiveT.time();
        int result = 0;
        for (int i = 0; i < BlockMap.size(); i++) {
            result += BlockMap.get(i).Actives();
        }
        c.close();
        return result;
    }

    // Timer t3=metrics.timer("Elininate");
    public void tryEliminated(int BFUs) {
        Timer.Context c = tryEliminateT.time();
        int T = TotalActives();
        int limitedBFU = limitedSize / BFUsize;
        if (T * BFUsize > limitedSize) {
            // Timer.Context ctx=t3.time();
            Eliminated(T - limitedBFU);
            // ctx.close();
        }
        c.close();
    }

    /**
     * Obtain the block number according to the prefix of the address, load all BFUs
     * related to this block into the memory and insert the address to update. If
     * the size reaches the limit, select BFU to move out of the memory according to
     * the principle of LRU
     */
    public void Insert(String address) {
        Timer.Context c = insertT.time();
        int prefix = Utils.TrytesToInt(address, prefixLength);
        int currentBlock = Scales[prefix] * BlockNumber + prefix;
        // System.out.println(currentBlock);
        GroupBloomFilter G = BlockMap.get(currentBlock);
        remainder -= G.Insert(address);// load all BFUs of block into memory

        // move block to queue tail
        //BlockLRU.remove(currentBlock);
        //BlockLRU.add(currentBlock);
        BlockLRU.get(currentBlock);

        // if hit the threshold,try remove BFUs following LRU
        if (remainder < 0) {
            Eliminated(0 - remainder);
        }
        // check capacity
        synchronized (this) {
            if (G.isFull()) {
                Scales[prefix]++;
                // if this is the first block get full
                if (Scales[prefix] > Scaled)
                    Scale();

            }
        }
        c.close();
    }

    // Timer t2=metrics.timer("mayExists");
    /**
     * First, check the BFU of the memory one by one, and return false directly if
     * it returns negative. Otherwise, load the BFU from the disk one by one, and
     * check, until the end, if all return positive, return true
     */
    public boolean mayExists(String address) {
        Timer.Context c = mayexistsT.time();
        int prefix = Utils.TrytesToInt(address, prefixLength);
        boolean Result = false;
        int TryEliminate = 0;
        // check Block from newly generated to old
        for (int i = Scales[prefix]; i >= 0; i--) {
            int currentBlock = i * BlockNumber + prefix;
            // update BlockLRU
            Timer.Context ctx=BlockLRUT.time();
            //BlockLRU.remove(currentBlock);
            //BlockLRU.add(currentBlock);
            BlockLRU.get(currentBlock);
            ctx.close();
            // Timer.Context ctx=t2.time();
            mayExistsResponce result = BlockMap.get(currentBlock).mayExists(address);
            // ctx.close();
            // any block load in new BFU

            TryEliminate += result.loadin;
            if (result.exists == true) {
                Result = true;
                break;
            }
        }

        remainder -= TryEliminate;
        if (remainder < 0) {
            Eliminated(0 - remainder);
        }
        c.close();
        return Result;
    }

    // one of current GroupBloomFilter is full,since the mean distribution,it will
    // be soon all Group get full
    // create and setlength of new "HBF"file
    public void Scale() {
        Timer.Context c = scaleT.time();
        Scaled++;
        System.out.println("=============================================Scale "+Scaled);
        File f = new File("HBF" + (Scaled));
        if (!f.exists()) {
            try {
                f.createNewFile();
                RandomAccessFile rf = new RandomAccessFile(f, "rw");
                rf.setLength((long) (Scaled + 5) * BFUsize * BlockNumber / 8);// every scale add one new BFU
                rf.close();
            } catch (Exception e) {
                // TODO: handle exception
            }
        }

        // add new group of Blocks with same prefix
        // compared with old block,new block hava one more BFU
        // double newP = P / Math.pow(4,Scaled); // ,Scaled P<=P0/(1-r)
        for (int i = 0; i < BlockNumber; i++) {
            // GroupBloomFilter newG = new GroupBloomFilter(BFUsize, hashFunctions, newP);
            GroupBloomFilter newG = new GroupBloomFilter(BFUsize, hashFunctions, 5 + Scaled, this, i);
            newG.setPath("HBF" + Scaled);
            newG.iniBFULRU();
            BlockMap.put(Scaled * BlockNumber + i, newG);
        }
        // reschedule BlockLRU
        ConcurrentLinkedHashMap<Integer,Integer> newBlockLRU=new ConcurrentLinkedHashMap.Builder<Integer,Integer>().maximumWeightedCapacity(BlockNumber*(Scaled+1)).build();
        int index=Scaled*BlockNumber;
        for (int j = 0; j < BlockNumber; j++){
            newBlockLRU.put(index+j, index+j);
        }

        Set<Integer> lru=BlockLRU.ascendingKeySet();        
        Iterator<Integer> it = lru.iterator();
        while (it.hasNext()){
            int oldindex=it.next();
            newBlockLRU.put(oldindex, oldindex);
        }

        BlockLRU = newBlockLRU;
        c.close();
    }

    public void print() {
        Set<Integer> lru=BlockLRU.ascendingKeySet();
        Iterator<Integer> it = lru.iterator();
        System.out.printf("BlockLRU:");
        while (it.hasNext()) {
            System.out.printf("%d ", it.next().intValue());
        }
        System.out.printf("\n");

        for (int i = 0; i < BlockMap.size(); i++) {
            BlockMap.get(i).print();
        }
    }
}
