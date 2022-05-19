package com.atguigu.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {

    public static List<String> splitKeyWord(String keyword) throws IOException {

        //创建集合用于存放结果数据
        ArrayList<String> result = new ArrayList<>();

        //创建分词器对象
        StringReader reader = new StringReader(keyword);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);

        //提取分词
        Lexeme next = ikSegmenter.next();

        while (next != null) {
            String word = next.getLexemeText();
            result.add(word);

            next = ikSegmenter.next();
        }

        //返回结果
        return result;
    }

    public static void main(String[] args) throws IOException {

        List<String> list = splitKeyWord("尚硅谷大数据Flink实时数仓项目");
        System.out.println(list);

    }

}
