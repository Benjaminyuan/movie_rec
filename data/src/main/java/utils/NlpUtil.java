package utils;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class NlpUtil {

    // 词型还原
    public static List<String> getLema(String text) {
        //单词集合
        List<String> wordslist = new ArrayList<>();
        //StanfordCoreNLP词形还原
        Properties props = new Properties();  // set up pipeline properties
        props.put("annotators", "tokenize, ssplit, pos, lemma");   //分词、分句、词性标注和次元信息。
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        Annotation document = new Annotation(text);
        pipeline.annotate(document);
        List<CoreMap> words = document.get(CoreAnnotations.SentencesAnnotation.class);
        for (CoreMap word_temp : words) {
            for (CoreLabel token : word_temp.get(CoreAnnotations.TokensAnnotation.class)) {
                String pos = token.get(CoreAnnotations.PartOfSpeechAnnotation.class); // 获取词性
                String lema = token.get(CoreAnnotations.LemmaAnnotation.class);  // 获取对应上面word的词元信息，即所需要的词形还原后的单词
                // 过滤各种助词
                String[] filters = new String[]{"in", "the", "on", "a", "an", "of", "to", "too",
                        "as", "very", "so", "off", "for", "in", "with", "from", ":", "movie",
                        "film", "&", "is", "than", "but", "base", "adapt", "make", "as hell", "south", "world"};
                boolean miss = false;
                for(String w : filters){
                    if(w.equals(lema)){
                        miss = true;
                        break;
                    }
                }
                if(!miss)
                    wordslist.add(lema);
            }
        }
        return wordslist;
    }
}
