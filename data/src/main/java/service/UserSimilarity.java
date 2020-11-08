package service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import hbase.HBaseOperator;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

public class UserSimilarity {
    private static final String userTagsTableName = "user_tags";
    private static final String userDistanceTableName = "user_distance";
    private static final String userClosestTableName = "user_distance";
    private static final int SIZE = 50;
    private PriorityQueue<Pair<Long, Double>> pairPriorityQueue = new PriorityQueue<>((o1, o2) -> {
        if (o1.getRight() - o2.getRight() < 1e-3) {
            return 0;
        }
        return o1.getRight() > o2.getRight() ? 1 : -1;
    });

    public void count(long userId, long totalSize) {
        Map<Integer, Double> userTags = getUserTagsFromHBase(userId);
        long countStart = userId + 1;
        while (countStart <= totalSize) {
            double dis = countDistance(userTags, getUserTagsFromHBase(countStart));
            HBaseOperator.insert(String.format("%s:%s", userId, countStart), "f1", "distance", String.valueOf(dis), userDistanceTableName);
            countStart++;

        }
    }

    public static Map<Integer, Double> getUserTagsFromHBase(long userId) {
        Map<String, String> r = HBaseOperator.get(String.valueOf(userId), userTagsTableName);
        String tagStr = r.get("f1:tags");
        return JSON.parseObject(tagStr, new TypeReference<Map<Integer, Double>>() {
        });
    }

    public double countDistance(Map<Integer, Double> user1Tags, Map<Integer, Double> user2Tags) {
        double sum = 0.;
        System.out.println("userTags1: " + user1Tags);
        System.out.println("userTags2: " + user2Tags);
        for (Integer tag : user1Tags.keySet()) {
            double tagValue1 = user1Tags.get(tag);
            double tagValue2 = user2Tags.getOrDefault(tag, 0.0);
            sum += Math.pow(Math.abs(tagValue1 - tagValue2), 2);
        }
        return Math.sqrt(sum);
    }

    public void countTest() {
        count(2, 3);
    }

    public void getKthSimilarityUser(long userId, long totalSize) {
        pairPriorityQueue.clear();
        for (long i = 1; i < userId; i++) {
            double distance = Double.parseDouble(HBaseOperator.get(String.format("%d:%d", i, userId), userDistanceTableName).get("f1:distance"));
            pairPriorityQueue.add(Pair.of(i, distance));
            if (pairPriorityQueue.size() > SIZE) {
                pairPriorityQueue.poll();
            }
        }
        for (long i = userId + 1; i <= totalSize; i++) {
            double distance = Double.parseDouble(HBaseOperator.get(String.format("%d:%d", userId, i), userDistanceTableName).get("f1:distance"));
            pairPriorityQueue.add(Pair.of(i, distance));
            if (pairPriorityQueue.size() > SIZE) {
                pairPriorityQueue.poll();
            }
        }
        List<Long> res = new ArrayList<>(pairPriorityQueue.size());
        while (pairPriorityQueue.size() > 0) {
            res.add(pairPriorityQueue.poll().getLeft());
        }
        HBaseOperator.insert(String.valueOf(userId), "f1", "close_list", JSON.toJSONString(res), userClosestTableName);


    }
}
