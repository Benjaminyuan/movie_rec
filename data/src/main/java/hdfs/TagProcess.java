package hdfs;

import dto.Tag;
import org.apache.arrow.flatbuf.Int;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import utils.NlpUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

public class TagProcess {

    public Vector<Tag> tags = new Vector<>();
    int[] fa = new int[2000];
    public HashMap<Integer, Integer> tagIdMap = new HashMap<>(); // 保存tagId的值
    Vector<Vector<Integer>> G = new Vector<Vector<Integer>>(); // 保存在tags中的编号
    Vector<Vector<Integer>> clusters = new Vector<>(); // 保存在tags中的编号

    // 计算距离
    public int getDistance(List<String> a, List<String> b) {
        // Jaccard 距离
        HashMap<String, Integer> frequency = new HashMap<>();
        AtomicInteger score = new AtomicInteger();
        a.forEach(word -> {
            frequency.put(word, 1);
        });
        b.forEach(word -> {
            if (frequency.containsKey(word))
                score.addAndGet(1);
        });
        if (score.get() != 0) {
            // 相差多少单词
            return Integer.min(a.size(), b.size()) - score.get();
        }
        String s = String.join(" ", a);
        String t = String.join(" ", b);

        // 最小编辑距离
        int[][] dis = new int[s.length() + 1][t.length() + 1];
        // 初始化
        for (int i = 0; i <= s.length(); i++)
            dis[i][0] = i;
        for (int j = 0; j <= t.length(); j++)
            dis[0][j] = j;
        // 动态规划
        for (int i = 1; i <= s.length(); i++) {
            for (int j = 1; j <= t.length(); j++) {
                dis[i][j] = Integer.min(dis[i - 1][j] + 1, dis[i][j - 1] + 1);
                if (s.charAt(i - 1) == t.charAt(j - 1))
                    dis[i][j] = Integer.min(dis[i][j], dis[i - 1][j - 1]);
                else dis[i][j] = Integer.min(dis[i][j], dis[i - 1][j - 1] + 1);
            }
        }
        return dis[s.length()][t.length()] * 9 / Integer.min(s.length(), t.length()) ;/// Integer.min(s.length(), t.length());
    }

    public int lookup(int x){
        if(fa[x] == x)
            return x;
        return fa[x] = lookup(fa[x]);
    }

    public void graphWalk(){
        for(int i = 0; i < tags.size(); i++)
            fa[i] = i;
        for(int i = 0; i < tags.size(); i++){
            Vector<Integer> neighbors = G.get(i);
            for(int j : neighbors){
                if(lookup(i) != lookup(j)){
                    fa[lookup(i)] = lookup(j);
                }
            }
        }
        HashMap<Integer, Integer> orderMap = new HashMap<>();
        int clusterCnt = 0;
        for(int i = 0; i< tags.size(); i++){
            // 记录映射关系
            int root = lookup(i);
            if(orderMap.containsKey(root))
                tagIdMap.put(tags.get(i).tagId, orderMap.get(root));
            else{
                orderMap.put(root, clusterCnt);
                clusterCnt += 1;
            }
        }
        for(int i = 0; i< clusterCnt; i++) {
            Vector<Integer> v = new Vector<Integer>();
            clusters.add(v);
        }
        for(int i = 0; i< tags.size(); i++)
            clusters.get(orderMap.get(lookup(i))).add(i);
    }

    public void output() {
        AtomicInteger cnt = new AtomicInteger();
        clusters.forEach(c -> {
            System.out.printf("*********************** %d ***********************\n", cnt.get());
            for (Integer index : c) {
                System.out.println(tags.get(index).toString());
                cnt.addAndGet(1);
            }
        });
        System.out.printf("总共%d个cluster\n", clusters.size());
    }


    public void process() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://localhost:9000");
        FileSystem fs = FileSystem.get(conf);

        FSDataInputStream inputStream = fs.open(new Path("/input/data/genome-tags.csv"));
        String s;

        // 输入tag
        while ((s = inputStream.readLine()) != null) {
            if (Character.isLetter(s.charAt(0)))
                continue;
            String[] line = s.split(",");
            Tag t = new Tag();
            t.setTagId(Integer.parseInt(line[0]));
            t.setOriginalName(line[1]);
            tags.add(t);
        }


        // 预处理tag
        for (int i = 0; i < tags.size(); i++) {
            String originalName = tags.get(i).originalName;
            String middleName = originalName.replaceAll("\\(.*\\)", "").replaceAll("'s", "").replaceAll("-", " ");
            List<String> processedTagName = NlpUtil.getLema(middleName);
            Tag t = tags.get(i);
            t.setTruncatedName(processedTagName);
        }

        // 计算tag之间的距离并连边
        for (int i = 0; i < tags.size(); i++) {
            Vector<Integer> v = new Vector<>();
            G.add(v);
            for (int j = 0; j < i; j++) {
                int dis = getDistance(tags.get(i).truncatedName, tags.get(j).truncatedName);
                if (dis <= 2) {
                    G.get(i).add(j);
                    G.get(j).add(i);
                }
            }
        }

        graphWalk();
        output();
//        FSDataOutputStream outStream = fs.create(new Path("hdfs:/mydir/wordcount.txt"));
//        outStream.write("ttt  tta  a afsdf dasf ".getBytes());
//        outStream.close();
//        InputStream inStream = fs.open(new Path("hdfs:/mydir/wordcount.txt"));
//        IOUtils.copyBytes(inStream,System.out,1024,false);
//        inStream.close();
        fs.close();
    }
}
