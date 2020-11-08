package utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomUtil {

    public static List<Integer> getNormalDistributionNumList(int start, int end, int n) {
        Random random = new Random();
        List<Integer> res = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            res.add((int) (random.nextGaussian() * (end - start) / 2 + (start + end) / 2));
        }
        return res;
    }
}
