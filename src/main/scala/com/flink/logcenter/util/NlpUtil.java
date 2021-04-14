package com.flink.logcenter.util;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.corpus.tag.Nature;
import com.hankcs.hanlp.seg.NShort.NShortSegment;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

public class NlpUtil {
    public static String getKeysByPredict(String text) {
        try {
            //去掉特殊符号
            text = StringEscapeUtils.unescapeHtml4(text);

            //取TOP10关键词
            List<String> keywordList = HanLP.extractKeyword(text, 20);
            List<String> keys = new ArrayList<>();
            int size = 0;
            for (int i = 0; i < keywordList.size(); i++) {
                String key = keywordList.get(i);
                List<Term> terms = HanLP.segment(key);
                for (Term t : terms) {
                    Nature aa = t.nature;
                    //只有类名称才能形成标签，并取TOP5
                    if (aa.toString().contains("nx")) {
                        continue;
                    }
                    if (aa.toString().contains("n") && size != 10) {
                        int count = org.apache.commons.lang.StringUtils.countMatches(text, key);
                        keys.add(key + ":" + count);
                        size++;
                    }
                }
            }

            return String.join(",", keys);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    public static List<String> shortSegment(String text) {
        Segment nShortSegment = new NShortSegment().enableCustomDictionary(true).enablePlaceRecognize(true);
        List<Term> seg = nShortSegment.seg(text);

        return seg.stream()
                .filter(e->e.word.length()>1&&(e.nature.toString().contains("n")||e.nature.toString().contains("v")))
                .map(e->e.word)
                .collect(Collectors.toList());
    }

    public static List<String> getKeysByPredictOnlyWord(String text, int keyCount) {
        try {
            //去掉特殊符号
            text = StringEscapeUtils.unescapeHtml4(text);

            //取TOP10关键词
            List<String> keywordList = HanLP.extractKeyword(text, 20);
            if (CollectionUtils.isNotEmpty(keywordList)) {
                keywordList = keywordList.stream().filter(e -> e != null && e.length() > 1).collect(Collectors.toList());
            }
            List<KeyWord> keys = new ArrayList<>();
            int size = 0;
            for (int i = 0; i < keywordList.size(); i++) {
                String key = keywordList.get(i);
                List<Term> terms = HanLP.segment(key);
                for (Term t : terms) {
                    Nature aa = t.nature;
                    //只有类名称才能形成标签，并取TOP5
                    if (aa.toString().contains("nx")) {
                        continue;
                    }
                    if (aa.toString().contains("n") && size != 10) {
                        int count = org.apache.commons.lang.StringUtils.countMatches(text, key);
                        keys.add(new KeyWord().setKey(key).setPower(count));
                        size++;
                    }
                }
            }

            if (CollectionUtils.isNotEmpty(keys)) {
                keys.sort(Comparator.comparing(KeyWord::getPower));
                List<String> collect = keys.stream().map(KeyWord::getKey).collect(Collectors.toList());
                if (collect.size() > keyCount) {
                    collect = collect.subList(0, keyCount);
                }
                return collect;
            } else {
                return Collections.emptyList();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String getKeyWord(String text) {
        try {
            //去掉特殊符号
            text = StringEscapeUtils.unescapeHtml4(text);

            //取TOP10关键词
            List<String> keywordList = HanLP.extractKeyword(text, 10);
            int size = 0;
            String kw = "";
            int score = 0;
            for (int i = 0; i < keywordList.size(); i++) {
                String key = keywordList.get(i);
                List<Term> terms = HanLP.segment(key);
                for (Term t : terms) {
                    Nature aa = t.nature;
                    //只有类名称才能形成标签，并取TOP5
                    if (aa.toString().contains("nx")) {
                        continue;
                    }
                    if (aa.toString().contains("n") && size != 10) {
                        int count = org.apache.commons.lang.StringUtils.countMatches(text, key);
                        if (count > score) {
                            kw = key;
                        }
                        size++;
                    }
                }
            }

            return kw;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String extractPhrase(String text) {
        List<String> strings = HanLP.extractPhrase(text, 1);
        if (CollectionUtils.isNotEmpty(strings)) {
            return strings.get(0);
        } else {
            return null;
        }
    }

    public static Set<String> getLabelsFromContent(String title, String content) {
        Set<String> set = new HashSet<>();
        if (StringUtils.isNotEmpty(title)) {
            set.addAll(getKeysByPredictOnlyWord(title, 5));
        }
        if (StringUtils.isNotEmpty(content)) {
            set.addAll(getKeysByPredictOnlyWord(content, 5));
        }
        return set;
    }

    static class KeyWord {
        private String key;
        private int power;

        public String getKey() {
            return key;
        }

        public KeyWord setKey(String key) {
            this.key = key;
            return this;
        }

        public int getPower() {
            return power;
        }

        public KeyWord setPower(int power) {
            this.power = power;
            return this;
        }
    }
}
