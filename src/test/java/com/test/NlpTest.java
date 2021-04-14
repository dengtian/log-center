package com.test;

import com.flink.logcenter.util.NlpUtil;
import com.hankcs.hanlp.seg.Dijkstra.DijkstraSegment;
import com.hankcs.hanlp.seg.NShort.NShortSegment;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.IndexTokenizer;
import com.hankcs.hanlp.tokenizer.StandardTokenizer;

import java.util.List;
import java.util.stream.Collectors;

public class NlpTest {

    public static void main(String[] args) {

        String text = "周杰伦对外开放项目攻坚：蓬安在共享电动车的投放问题上应该怎么办？";

        List<Term> terms = shortSegment(text);
        System.out.println(terms);

    }


    /**
     * 标准分词
     */
    public static Object standardTokenizer(String text) {
        return StandardTokenizer.seg2sentence(text);
    }

    /**
     * 索引分词
     */
    public static Object indexTokenizer(String text) {
        return IndexTokenizer.seg2sentence(text);
    }

    /**
     * 最短路劲分词
     */
    public static List<Term> shortSegment(String text) {
        Segment nShortSegment = new NShortSegment().enableCustomDictionary(true).enablePlaceRecognize(true);
        List<Term> seg = nShortSegment.seg(text);

        return seg.stream().filter(e->e.word.length()>1&&(e.nature.toString().contains("n")||e.nature.toString().contains("v"))).collect(Collectors.toList());
    }


    public static List<Term> dijkstraSegment(String text) {
        Segment di = new DijkstraSegment().enableCustomDictionary(true).enablePlaceRecognize(true);
        return di.seg(text);
    }


}
