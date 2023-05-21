package com.iota.iri.hotbf;

import com.iota.iri.metrics.ConsoleReporter;
import com.iota.iri.metrics.MetricRegistry;
import com.iota.iri.metrics.Timer;

import java.io.BufferedWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * a GroupBloomFilter is consists of a set of independent BlommFilterUnit.
 * BFUsize:The size of a BFU,so entrie size of GBF N=BFUsize*BFUnits. P:the
 * false positive of GBF.P=p^BFUnits,p=false positive of BFU;
 * hashFunction:number of hash functions for one BFU,total hash
 * functions=hashFunction*BFUnits=K=log2(1/P)
 */
public class GroupBloomFilter {
    private int index;
    // public CopyOnWriteArrayList<BloomFilter<String>> Group =new
    // CopyOnWriteArrayList<>();//Thread safety for write and delete operations
    public ConcurrentHashMap<Integer, BloomFilter<String>> Group = new ConcurrentHashMap<>();
    private int BFUsize;
    private int HashFunctions;
    private double P;
    private String path;
    public int BFUnits;// BFunits=log2(1/P) / hashFUnctions

    private int Size;// M=BFUsize*BFUnits
    private int Capacity;// M*(ln2)^2 / |lnP|
    private int Entities;// current number of enttities inseted to group bloom filter
    HotBF hot;

    private boolean[] Active;// true if BFU in memory
    private int Actives;// BFU numbers in memory

    public static MetricRegistry metrics = new MetricRegistry();
    public static ConsoleReporter reporter = Utils.getreport(metrics);

    public static Timer addT = metrics.timer("Group add");
    public static Timer checkBFUWithHashdT = metrics.timer("Group checkBFUWithHashValues");
    public static Timer eliminateT = metrics.timer("Group eliminateBFU");
    public static Timer getBFUT = metrics.timer("Group getBFU");
    public static Timer getHashValuesT = metrics.timer("Group getHashValues");
    public static Timer insertT = metrics.timer("Group insert");
    public static Timer loadinBFUT = metrics.timer("Group LoadinBFU");
    public static Timer mayexistsT = metrics.timer("Group mayExists");
    public static Timer mayexixsts1T = metrics.timer("Group mayExixsts 1");
    public static Timer mayexixsts2T = metrics.timer("Group mayExixsts 2");

    // Deque
    ConcurrentLinkedDeque<Integer> BFULRU = new ConcurrentLinkedDeque<>();// BFULRU was born empty

    GroupBloomFilter(int bfusize, int hashFunctions, double p, HotBF hotbf, int Index) {
        BFUsize = bfusize;
        HashFunctions = hashFunctions;
        P = p;
        hot = hotbf;
        index = Index;

        int K = (int) (Math.ceil(Math.log(1 / P) / Math.log(2)));

        BFUnits = K / hashFunctions;
        Size = BFUsize * BFUnits;
        double e = 2.7182818285;
        Capacity = (int) (Size * Math.pow((Math.log(2) / Math.log(e)), 2) / Math.abs((Math.log(P) / Math.log(e))));

        Active = new boolean[BFUnits];
        for (int i = 0; i < BFUnits; i++) {
            Active[i] = false;
            // BFULRU.add(i);
        }
        Actives = 0;

    }

    GroupBloomFilter(int bfusize, int hashFunctions, int bfunits, HotBF hotbf, int Index) {
        BFUsize = bfusize;
        HashFunctions = hashFunctions;
        BFUnits = bfunits;
        hot = hotbf;
        index = Index;

        P = (double) 1 / Math.pow(2, BFUnits * hashFunctions);
        Size = BFUsize * BFUnits;
        double e = 2.7182818285;
        Capacity = (int) (Size * Math.pow((Math.log(2) / Math.log(e)), 2) / Math.abs((Math.log(P) / Math.log(e))));

        Active = new boolean[BFUnits];
        for (int i = 0; i < BFUnits; i++) {
            Active[i] = false;
            // BFULRU.add(i);
        }
        Actives = 0;

    }

    GroupBloomFilter(int bfusize, int hashFunctions, double p) {
        BFUsize = bfusize;
        HashFunctions = hashFunctions;
        P = p;

        int K = (int) (Math.ceil(Math.log(1 / P) / Math.log(2)));

        BFUnits = K / hashFunctions;
        Size = BFUsize * BFUnits;
        double e = 2.7182818285;
        Capacity = (int) (Size * Math.pow((Math.log(2) / Math.log(e)), 2) / Math.abs((Math.log(P) / Math.log(e))));

        Active = new boolean[BFUnits];
        for (int i = 0; i < BFUnits; i++)
            Active[i] = false;

    }

    public int Size() {
        return Size;
    }

    public void iniBFULRU() {
        for (int i = 0; i < BFUnits; i++) {
            BFULRU.add(i);
        }
    }

    public void setPath(String p) {
        path = p;
    }

    public void loadmeta(String meta1, String meta2) {
        // meta1,entites | active
        String[] m1 = meta1.split(" ");
        Entities = Integer.valueOf(m1[0]);
        for (int i = 1; i < m1.length; i++) {
            Active[Integer.valueOf(m1[i])] = true;
            Actives++;
        }
        // meta2 BFULRU
        String[] m2 = meta2.split(" ");
        for (int i = 0; i < m2.length; i++) {
            BFULRU.add(Integer.valueOf(m2[i]));
        }

    }

    //
    public void savemeta(BufferedWriter bw) {
        try {
            bw.write(String.valueOf(Entities) + " ");
            for (int i = 0; i < BFUnits; i++) {
                if (Active[i])
                    bw.write(String.valueOf(i) + " ");
            }
            bw.write("\n");
            Iterator<Integer> it = BFULRU.iterator();
            while (it.hasNext()) {
                bw.write(String.valueOf(it.next()) + " ");
            }
            bw.write("\n");
        } catch (Exception e) {
            // TODO: handle exception
        }

    }

    // hash value,output range[1,m],k hash results
    public static int[] hashCassandra(byte[] value, int m, int k) {
        int[] result = new int[k];
        long hash1 = HashProvider.murmur3(0, value);
        long hash2 = HashProvider.murmur3((int) hash1, value);
        for (int i = 0; i < k; i++) {
            result[i] = (int) ((hash1 + i * hash2) % m);
        }
        return result;
    }

    public int[] getHashValues(String s) {
        Timer.Context c = getHashValuesT.time();
        try {
            return hashCassandra(s.getBytes(), BFUsize, BFUnits * HashFunctions);
        } finally {
            c.close();
        }
    }

    public void add(String s) {
        Timer.Context c = addT.time();
        int[] hashedValues = getHashValues(s);

        for (int i = 0; i < BFUnits; i++) {
            BloomFilter<String> BFU = Group.get(i);
            for (int j = 0; j < HashFunctions; j++) {
                try {
                    BFU.setBit(hashedValues[i * HashFunctions + j], true);
                } catch (Exception e) {
                    // e.printStackTrace();
                }
            }
        }
        c.close();
    }

    public boolean check(String s) {
        int[] hashedValues = getHashValues(s);
        for (int i = 0; i < BFUnits; i++) {
            BloomFilter<String> BFU = Group.get(i);
            for (int j = 0; j < HashFunctions; j++) {
                if (!BFU.getBit(hashedValues[i * HashFunctions + j])) {
                    return false;
                }
            }
        }
        return true;
    }

    public boolean checkBFU(String s, int i) {
        if (i >= BFUnits) {
            System.out.println("ERROR:check unexit BFU");
            System.exit(1);
        }
        int[] hashedValues = getHashValues(s);
        BloomFilter<String> BFU = Group.get(i);
        for (int j = 0; j < HashFunctions; j++) {
            if (!BFU.getBit(hashedValues[i * HashFunctions + j])) {
                return false;
            }
        }
        return true;
    }

    /**
     *
     * @param BFUs         specific number of one BF unit
     * @param hashedValues compelete hashed value for a key
     * @return
     */
    public boolean checkBFUWithHashValues(int BFUs, int[] hashedValues) {
        Timer.Context c = checkBFUWithHashdT.time();
        try {
            BloomFilter<String> BFU = Group.get(BFUs);

            for (int i = 0; i < HashFunctions; i++) {
                boolean result = true;
                try {
                    result = BFU.getBit(hashedValues[BFUs * HashFunctions + i]);
                } catch (Exception e) {
                    // TODO: handle exception
                    // System.out.println("BFU:"+BFU);

                }
                if (!result) {
                    return false;
                }
            }
            return true;
        } finally {
            c.close();
        }
    }

    /**
     * insert address to GroupBF,this will bring all BFUs into memory(if necessary)
     *
     * @param address
     */
    public int Insert(String address) {
        Timer.Context c = insertT.time();
        int loadin = 0;
        // load all BFU into memory
        for (int i = 0; i < BFUnits; i++) {
            if (!Active[i]) {
                // load BFU into memory
                BloomFilter<String> bf = new FilterBuilder(BFUsize, HashFunctions).buildBloomFilter();
                bf.load(path, getOffset(index, i), BFUsize);
                Group.put(i, bf);
                Active[i] = true;
                loadin++;
            }
        }
        Entities++;
        Actives = BFUnits;
        // insert new address
        /*
         * if (!check(address)) { Entities++; }
         */
        // note that,this time we dont't update BFULRU,cause they don't change BFU
        // hotness

        // update BFUs
        add(address);
        c.close();
        return loadin;
    }

    public boolean isFull() {
        return Entities >= Capacity;
    }

    public class mayExistsResponce {
        boolean exists;
        int loadin;

        mayExistsResponce(boolean r, int l) {
            exists = r;
            loadin = l;
        }
    }

    public static int loadtimes = 0;

    /**
     * First check BFUs in memory,then if needed,load in BFU to check .When a
     * negative result is obtained, move the corresponding BFU to the MRU end,
     * otherwise no change is made Load in checked unactive BFU
     */
    public mayExistsResponce mayExists(String address) {
        Timer.Context c = mayexistsT.time();
        try {
            int[] keys = getHashValues(address);

            Timer.Context c1 = mayexixsts1T.time();
            try {

                // check BFUs in memory
                for (int i = 0; i < BFUnits; i++) {
                    if (Active[i]) {
                        boolean result = checkBFUWithHashValues(i, keys);
                        if (result == false) {
                            // update BFULRU
                            // Since Deque, ew can view the tail of the linked list to avoid unnecessary
                            // operations
                            if (BFULRU.peekLast() != i) {
                                BFULRU.remove(i);
                                BFULRU.add(i);
                            }
                            // get result;
                            return (new mayExistsResponce(false, 0));
                        }
                    }
                }
                // else,need to bring other BFUs into memory
                if (Actives == BFUnits)
                    return (new mayExistsResponce(true, 0));
            } finally {
                c1.close();
            }

            Timer.Context c2 = mayexixsts2T.time();
            try { // load in needed BFUs
                // if result is true,persist all those in memory
                // if result is false,persist only the negative BFU
                HashMap<Integer, BloomFilter<String>> loadedin = new HashMap<>();
                for (int i = 0; i < BFUnits; i++) {
                    if (!Active[i]) {
                        loadtimes++;
                        // read into memory
                        BloomFilter<String> bf = new FilterBuilder(BFUsize, HashFunctions).buildBloomFilter();
                        bf.load(path, getOffset(index, i), BFUsize);
                        loadedin.put(i, bf);
                        Active[i] = true;
                        Actives++;

                        // check
                        boolean result = true;
                        for (int j = 0; j < HashFunctions; j++) {
                            if (!bf.getBit(keys[i * HashFunctions + j])) {
                                result = false;
                                break;
                            }
                        }
                        if (result == false) {
                            // The one who contributed
                            BFULRU.remove(i);
                            BFULRU.add(i);
                            Group.put(i, bf);
                            return (new mayExistsResponce(false, 1));
                        }

                    }
                }
                for (int i = 0; i < BFUnits; i++) {
                    BloomFilter<String> bf = loadedin.get(i);
                    if (bf != null) {
                        Group.put(i, bf);
                    }
                }
                return (new mayExistsResponce(true, loadedin.size()));
            } finally {
                c2.close();
            }
        } finally {
            c.close();
        }
    }

    public long getOffset(int block, int Units) {
        return ((long) block * Size + Units * BFUsize) / 8;
    }

    public BloomFilter<String> getBFU(int BFU) {
        return Group.get(BFU);
    }

    public int Actives() {
        return Actives;
    }

    public boolean isActive() {
        return Actives > 0;
    }

    public boolean isActive(int index) {
        return Active[index];
    }

    public static int savetimes = 0;

    // save and remove the LRU BFU
    public int EliminateBFU(int numbers) {
        Timer.Context c = eliminateT.time();
        int decresed = 0;
        if (Actives < numbers) {
            // System.out.println("No Enough BFU to Eliminate");
            return 0;
        }
        Iterator<Integer> it = BFULRU.iterator();
        int target = numbers;
        while (target > 0 && it.hasNext()) {

            int t;
            t = it.next();
            BloomFilter<String> bf = Group.get(t);
            if (Active[t] && bf != null) {
                // Group.get(t).save(path, getOffset(index, t), BFUsize);// save before remove
                bf.save(path, ((long) index * Size + t * BFUsize) / 8, BFUsize);
                Group.remove(t);// remove BFU
                savetimes++;
                Active[t] = false;
                Actives--;
                target--;
                decresed++;
            }
        }
        c.close();
        return decresed;
    }

    public void ShutDown() {
        // save active BFU
        for (int i = 0; i < BFUnits; i++) {
            if (Active[i]) {
                Group.get(i).save(path, getOffset(index, i), BFUsize);
            }
        }
    }

    public void LoadInBFU(int k) {

        if (!Active[k]) {
            System.out.println("Load in non-active BFU");
            return;
        }
        BloomFilter<String> bf = new FilterBuilder(BFUsize, HashFunctions).buildBloomFilter();
        bf.load(path, getOffset(index, k), BFUsize);
        Group.put(k, bf);
    }

    public void print() {
        // entitis | actives
        // BFUs
        System.out.printf("GBF:%d ", index);
        System.out.printf("Entitis:%d ", Entities);
        System.out.printf("Activs:%d", Actives);
        System.out.printf("Active:");
        for (int i = 0; i < BFUnits; i++) {
            if (Active[i]) {
                System.out.printf("%d ", i);
            }
        }
        System.out.printf("\nBFUS:");

        for (int i = 0; i < BFUnits; i++) {
            if (Group.get(i) != null) {
                System.out.printf("%d ", i);
            }
        }
        System.out.printf("\n");
    }

    public int getCapacity() {
        return Capacity;
    }
}