package com.iota.iri.levelbf;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.ListIterator;

/**
 * SBF query cache use linkedList,least used in the head of list
 */
public class SBFCache {
    private String path;
    HashProvider.HashMethod HashFunction;
    int cachePrefix = 2; // prefix length of query sub cache
    int scacheSize = 32; // size of each sub cache
    // so total cache size=27^2*32*4KB
    int cacheNumber = (int) Math.pow(27, cachePrefix);

    // MetricRegistry metrics = new MetricRegistry();
    // ConsoleReporter reporter =
    // ConsoleReporter.forRegistry(metrics).convertDurationsTo(TimeUnit.MICROSECONDS).build();

    // Timer t0 = metrics.timer(MetricRegistry.name(LeveledBloomFilter.class,
    // "linkedList TIme"));

    class cacheItem {
        int prefix;// 3 trytes
        int activenumber; // cached SBF number, note:noly active > 0 needed
        BloomFilter<String> sbf;

        cacheItem(int p, int c, HashProvider.HashMethod hf) {
            sbf = new FilterBuilder(4 * 1024 * 8, 7).hashFunction(hf).buildBloomFilter();
            prefix = p;
            activenumber = c;
        }
    }

    ArrayList<LinkedList<cacheItem>> lists = new ArrayList<>();

    int total = 0;
    int miss = 0;

    SBFCache(String p, HashProvider.HashMethod hf) {
        path=p;
        HashFunction=hf;
        for (int i = 0; i < cacheNumber; i++) {
            lists.add(new LinkedList<>());
        }
        // reporter.start(6000, TimeUnit.SECONDS);
    }

    public boolean get(int gbf, int bf, int Maxactivenumber, String address) {
        // System.out.printf("cache called for
        // %d,%d,%d,%s\n",gbf,bf,Maxactivenumber,address);
        // printCache();
        total += Maxactivenumber;
        miss +=Maxactivenumber;
        LinkedList<cacheItem> list = lists.get(gbf);

        int prefix = bf;
        boolean[] found = new boolean[Maxactivenumber];// total SBFs in one BF

        ListIterator<cacheItem> i = list.listIterator();

        LinkedList<cacheItem> buf = new LinkedList<>();
        // Context ctx=t0.time();
        while (i.hasNext()) {
            cacheItem t = i.next();
            if (t.prefix == prefix) {
                // System.out.println("found in cache");
                found[t.activenumber] = true;
                miss--;
                
                i.remove();
                buf.addFirst(t);

                if (t.sbf.contains(address)) {
                    poplist(buf, list);
                    return true;
                }
            }
        }
        poplist(buf, list);
        // ctx.stop();

        for (int j = 0; j < Maxactivenumber; j++) {
            if (!found[j]) {
                cacheItem tmp = new cacheItem(prefix, j, HashFunction);
                String filename = path+"/GBF" + gbf;
                int offset = bf * 128 * 1024 + j * 4 * 1024;
                tmp.sbf.load(filename, offset, 4 * 1024);

                if (list.size() >= scacheSize) {
                    list.removeLast();
                }
                list.addFirst(tmp);
                // printCache();
                if (tmp.sbf.contains(address)) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * insert all item from src to dst
     * 
     */
    public void poplist(LinkedList<cacheItem> src, LinkedList<cacheItem> dst) {
        ListIterator<cacheItem> it = src.listIterator();
        while (it.hasNext()) {
            cacheItem t = it.next();
            dst.addFirst(t);
        }
    }
}