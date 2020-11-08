package task;

import hdfs.RatingsProcess;
import service.UserSimilarity;

import java.util.List;

import hdfs.TagProcess;
import utils.NlpUtil;
import utils.SplitRatings;


public class Main{

    public static void main(String[] args) throws Exception{
        TagProcess tagProcess = new TagProcess();
        tagProcess.loadTagToHBase();
        RatingsProcess rp = new RatingsProcess();
        String template = "ratings-%d.csv";
        rp.chainProcess(String.format(template,0));
    }
}