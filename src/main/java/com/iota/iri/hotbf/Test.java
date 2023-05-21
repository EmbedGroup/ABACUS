package com.iota.iri.hotbf;


import java.io.*;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;


public class Test {

    public void RandomTest() {
        int UnitSize = 4;
        int BlockSize = 5 * UnitSize;
        int BlockNumber = 1000;
        int WriteTimes = 1000;
        int ReadTImes = 1000;

        // create file
        try {
            File f = new File("GBF");
            f.createNewFile();
            RandomAccessFile rf = new RandomAccessFile(f, "rw");
            rf.setLength(BlockSize * 1024 * BlockNumber);
            rf.close();
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }

        // Write test
        System.out.println("Random Write " + WriteTimes + " times");
        BloomFilter<String> bf = new FilterBuilder(4 * 1024 * 8, 10).buildBloomFilter();
        bf.add("Hello");
        byte[] obj = bf.bitSet2ByteArray(bf.getBitSet());
        try {
            File f = new File("GBF");
            RandomAccessFile rf = new RandomAccessFile(f, "rw");

            long start = System.currentTimeMillis();
            for (int i = 0; i < WriteTimes; i++) {
                rf.seek(20 * 1024 * i);
                rf.write(obj);
            }
            System.out.println("Use time:" + String.valueOf(System.currentTimeMillis() - start) + "ms");
            rf.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Random Write " + WriteTimes + " times");
        int sequenceNumber = 2;
        byte[] obj2 = new byte[4096 * 2];
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 4096; j++) {
                obj2[i * 4096 + j] = obj[j];
            }
        }

        try {
            File f = new File("GBF");
            RandomAccessFile rf = new RandomAccessFile(f, "rw");

            long start = System.currentTimeMillis();
            for (int i = 0; i < WriteTimes / sequenceNumber; i++) {
                rf.seek(20 * 1024 * i);
                rf.write(obj2);
            }
            System.out.println("Use time:" + String.valueOf(System.currentTimeMillis() - start) + "ms");
            rf.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        File f = new File("GBF");
        try {
            f.delete();
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
    }

    public void Hashtest() {
        for (int i = 0; i <= 13; i++) {
            BloomFilter<String> bf = new FilterBuilder(4 * 1024 * 8, 5).AssignHashFunction(i).buildBloomFilter();
            System.out.println("expect elements=" + bf.getExpectedElements() + " false positive="
                    + bf.getFalsePositiveProbability());
            long start = System.currentTimeMillis();
            for (int j = 0; j < 1; j++) {
                bf.add(String.valueOf(j));
            }
            System.out.println(i + ":" + String.valueOf(System.currentTimeMillis() - start));
        }

    }

    public void LRUTableTest() {
        LRUTable<String, String> table = new LRUTable<>();
        table.put("1", "value");
        table.put("2", "value");
        table.put("3", "value");
        table.put("4", "value");
        table.put("5", "value");
        table.put("6", "value");
        table.put("7", "value");

        String answer = table.Get("2");
        table.MoveToFirst("2");

        for (int i = 0; i < 6; i++) {
            table.RemoveTail();
        }
        table.PrintList();
    }

    LRUTable<String, String> table = new LRUTable<>();

    public class LRUThread extends Thread {
        int id;

        LRUThread(int i) {
            id = i;
        }

        public void run() {
            try {
                for (int i = 0; i < 1000000; i++) {
                    table.put(String.valueOf(id) + "+" + String.valueOf(i), String.valueOf(System.currentTimeMillis()));
                    double a = 3.33;
                    for (int j = 0; j < 100; j++) {
                        a *= a;
                    }
                    table.MoveToFirst(String.valueOf(id) + "+" + String.valueOf(i));
                    for (int j = 0; j < 100; j++) {
                        a *= a;
                    }
                    table.RemoveTail();

                }
            } catch (Exception e) {
                e.printStackTrace();

            }

        }
    }

    public void LRUThreadTest() {
        for (int i = 0; i < 10; i++) {
            Thread t = new LRUThread(i);
            t.start();
        }

        table.PrintList();
    }

    public void RandomnessTest() {
        Random r = new Random();
        for (int i = 0; i < 1000; i++) {
            System.out.println(r.nextDouble());
        }
    }

    // Tail means latest used，head means oldest
    public void linkedqueueTest() {
        ConcurrentLinkedQueue<String> q = new ConcurrentLinkedQueue<>();
        q.add("a");// add to tail
        q.add("b");
        q.add("c");

        // move "b" to tail
        q.remove("b");
        q.add("b");

        // check head
        System.out.println(q.peek());

        Iterator<String> it = q.iterator();// iterator from head to tail
        while (it.hasNext()) {
            System.out.println(it.next());
        }

        ConcurrentLinkedDeque<String> dq = new ConcurrentLinkedDeque<>();
        dq.add("a");
        dq.add("b");
        dq.add("c");

        dq.remove("b");
        dq.add("b");
        it = dq.iterator();
        while (it.hasNext()) {
            System.out.println(it.next());
        }
        System.out.println(dq.peekLast());

    }

    public static void filetest(){
        File f=new File("test");
        try{
            if(!f.exists()) f.createNewFile();
            BufferedWriter bw=new BufferedWriter(new FileWriter(f));
            bw.write(1+" "+2+" "+3+"\n");
            bw.write(4+" ");

            bw.close();
            BufferedReader br=new BufferedReader(new FileReader(f));
            int x=br.read();
            int y=br.read();
            int z=br.read();
            int t=br.read();
        }
        catch(Exception e){
            e.printStackTrace();
        }

    }

    // public static void zmq(){
    //     ZMQPrint p=new ZMQPrint("127.0.0.1", 5556);
    //     //http://173.212.200.186:14265
    //     //161.117.83.53
    //     p.start();
    //     Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    //         p.lo.write();
    //         p.lo.summary();
    //     }, "Shutdown Hook"));
    // }
}