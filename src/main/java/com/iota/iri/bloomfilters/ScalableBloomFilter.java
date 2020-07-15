package com.iota.iri.bloomfilters;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.iota.iri.metrics.ConsoleReporter;
import com.iota.iri.metrics.MetricRegistry;
import com.iota.iri.metrics.Timer;
import com.iota.iri.metrics.Timer.Context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




public class ScalableBloomFilter {
    private static final Logger log = LoggerFactory.getLogger(ScalableBloomFilter.class);
    private final List<BloomFilter<String>> BFs = new ArrayList<>();
    private double P0;
    private int m0;
    private final List<Integer> k = new ArrayList<>();// hash function的数量
    private final List<Integer> M = new ArrayList<>();// 过滤器的实际大小
    private final List<Integer> n = new ArrayList<>();// 过滤器保证false positive的情况下可容纳的最大元素数
    private final List<Integer> num = new ArrayList<>();// 过滤器中当前的元素数
    private double s;
    private double r;
    private String storePath;


    static public MetricRegistry registry = new MetricRegistry();
    ConsoleReporter reporter = ConsoleReporter.forRegistry(registry).convertDurationsTo(TimeUnit.MICROSECONDS).build();
    static Timer timer;
    
    
    public ScalableBloomFilter(double p, int m, double ss, double rr, String path) {
        P0 = p;
        m0 = m;
        s = ss;
        r = rr;
        storePath = path;

        timer = registry.timer(MetricRegistry.name(ScalableBloomFilter.class, "Initialize SBF"));
        reporter.start(10, TimeUnit.SECONDS);
    }

    public List<Integer> getM() {
        System.out.println(k);
        return M;
    }

    /**
     * 若storepath中存在过滤器文件,则从中读出过滤器内容 否则新建一个过滤器
     */
    public void buildBF() {
        File f = new File(storePath);
        if (!f.exists()) {
            f.mkdirs();
            addFilter(0);
        } else {
            loadBFs();
        }
    }

    /**
     * 从本地文件中读入之前保存的过滤器 M,k,n重新计算 过滤器的bitset以及num从文件中读入
     */
    public void loadBFs() {
        // 清空lists
        M.clear();
        k.clear();
        n.clear();
        num.clear();

        // 统计子文件数目
        File bfsFile = new File(storePath);
        File[] files = bfsFile.listFiles();
        int numOfFiles = 0;
        for (File child : files) {
            if (child.isFile()) {
                numOfFiles++;
            }
        }
        numOfFiles--;//num.txt排除
        // 计算每个过滤器的M,k以及n
        double p = P0;
        Integer m = new Integer(String.valueOf(m0));
        for (int i = 0; i < numOfFiles; i++) {
            Integer kk = pTok(p);
            Integer mm = mkToM(m, kk);
            k.add(kk);
            M.add(mm);
            n.add(mpTon(mm, p));

            p *= r;
            m *= (int)s;
        }

        // 导入过滤器
        for (int i = 0; i < numOfFiles; i++) {
            String paths = storePath + "/" + i;
            BloomFilter<String> bf = new FilterBuilder(M.get(i), k.get(i)).buildBloomFilter();
            Context ctx=timer.time();
        
            bf.load(paths);
            ctx.stop();
            BFs.add(bf);
        }

        // 读入num
        String pathNum = storePath + "/num.txt";
        try {
            BufferedReader bf = new BufferedReader(new FileReader(pathNum));
            String line = null;
            while ((line = bf.readLine()) != null) {
                Integer buf = new Integer(line);
                if (buf.intValue() > 0) {
                    num.add(buf);
                }
            }
            bf.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        reporter.report();
        listMetadate();
    }

    /**
     * 将过滤器的bitset保存在本地文件中 以及每隔过滤器的num
     */
    public void saveBFs() {
        clearLocalFile();
        File f=new File(storePath);
        f.mkdirs();
        // 保存bitset
        for (int i = 0; i < BFs.size(); i++) {
            String paths = storePath + "/" + i;
            BFs.get(i).save(paths);
        }
        // 保存num
        try {
            String pathNum = storePath + "/num.txt";
            FileOutputStream os = new FileOutputStream(pathNum);
            PrintWriter pw = new PrintWriter(os);
            for (int i = 0; i < num.size(); i++) {
                pw.println(num.get(i));
            }
            pw.close();
            os.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void add(String obj) {
        //log.info("---Saving Address: "+obj);
        int i = BFs.size() - 1;// 当前的最后一个过滤器
        if (num.get(i) >= n.get(i)) {
            System.out.println("full");
            // 若是当前的过滤器满了就新建一个
            addFilter(++i);
        }
        BFs.get(i).add(obj);// 插入过滤器
        num.set(i, num.get(i) + 1);// 更新当前数量
    }

    public boolean mayExists(String obj) {
        for (int i = 0; i < BFs.size(); i++) {
            if (BFs.get(i).contains(obj)) {
                return true;
            }
        }
        return false;
    }

    public void clearLocalFile() {
        deleteDir(storePath);
    }
/*
    public static void main(String args[]) {

        ScalableBloomFilter bfs=new ScalableBloomFilter(0.01,100,2,0.8,"BFs");
        //BFs.clearLocalFile();
        bfs.buildBF();
        bfs.listMetadate();

        String pathNum = "addresses1000.txt";
        try {
            BufferedReader bf = new BufferedReader(new FileReader(pathNum));
            String line = null;
            while ((line = bf.readLine()) != null) {
                BFs.add(line);
            }
            bf.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        BFs.listMetadate();

        String address="ZPDICZ9QKWLT9DDDNCRFSHGUDLLVGASKGOZMZDMYZVHJWKZWCZLJXXYTWHTTJCBCETFBJEFVBLLXAFWMQ";
        if(BFs.mayExists(address)){
            System.out.println("mayExists");
        }else{
            System.out.println("certainly not exists");
        }
        BFs.saveBFs();
    }
*/
    public void listMetadate() {
        System.out.println("ListMetadate:");
        System.out.print("M:");
        for(int i=0;i<M.size();i++)
        {
            System.out.print(M.get(i)+" ");
        }
        System.out.println(" ");

        System.out.print("k:");
        for(int i=0;i<k.size();i++)
        {
            System.out.print(k.get(i)+" ");
        }
        System.out.println(" ");

        System.out.print("n:");
        for(int i=0;i<n.size();i++)
        {
            System.out.print(n.get(i)+" ");
        }
        System.out.println(" ");

        System.out.print("num:");
        for(int i=0;i<num.size();i++)
        {
            System.out.print(num.get(i)+" ");
        }
        System.out.println(" ");
    }

    /**
     * k=log2(1/p) 结果取上整
     */
    public Integer pTok(double p) {
        return new Integer(String.valueOf((int) (Math.log(1 / p) / Math.log(2)) + 1));

    }

    /**
     * n=M*(ln2)^2 / |lnP|
     * 
     */
    public static Integer mpTon(Integer m, double p) {
        Double mm = new Double(m);
        mm *= Math.log(2) * Math.log(2) / Math.abs(Math.log(p));
        return mm.intValue();
    }

    /**
     * M=m*K
     * 
     * @param i
     * @return
     */
    public Integer mkToM(Integer m, Integer k) {
        return k * m;
    }

    /**
     * 添加第i个过滤器,包括其M,k,n数组的计算与设置,新建一个过滤器并插入BFs数组,新建一个0的数插入num数组
     * 
     * @param i
     */
    private void addFilter(int i) {
        System.out.println("--addfilter "+i);
        double pi = Math.pow(r, i) * P0;// pi=p0*r^i
        Double tmp = new Double(Math.pow(s, i) * m0);
        Integer mi = tmp.intValue();// mi=m0*s^i

        Integer resultK = pTok(pi);
        Integer resultM = mkToM(mi, resultK);
        k.add(resultK);
        M.add(resultM);
        n.add(mpTon(resultM, pi));
        System.out.println("****** "+resultM);
        BloomFilter<String> bf = new FilterBuilder(resultM, resultK).buildBloomFilter();
        BFs.add(bf);

        Integer nums = new Integer(String.valueOf(0));
        num.add(nums);
    }

    public void deleteDir(String dirPath) {
        File file = new File(dirPath);
        if (file.isFile()) {
            file.delete();
        } else {
            File[] files = file.listFiles();
            if (files == null) {
                file.delete();
            } else {
                for (int i = 0; i < files.length; i++) {
                    deleteDir(files[i].getAbsolutePath());
                }
                file.delete();
            }
        }
    }
    
}
