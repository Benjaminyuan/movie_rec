package service;

import dto.MovieInfo;
import org.apache.hadoop.hbase.util.Pair;

import java.util.ArrayList;
import java.util.List;

public class DataProcess {
    // 用户观看电影数据集拆分,pair<testList,trainList>
    public static Pair<List<MovieInfo>, List<MovieInfo>> splitUserRatingRawData(List<MovieInfo> list) {
        int cap = list.size() / 2;
        List<MovieInfo> testList = new ArrayList<>(cap);
        List<MovieInfo> trainList = new ArrayList<>(cap);
        int len = list.size() % 2 == 0 ? list.size() : list.size() - 1;
        //数据集拆分，对半分,后续需要根据正太分布拆分。
        for (int i = 0; i < len; ) {
            testList.add(list.get(i++));

            trainList.add(list.get(i++));
        }
        if (len != list.size()) {
            trainList.add(list.get(list.size() - 1));
        }
        return Pair.newPair(testList, trainList);
    }
}
