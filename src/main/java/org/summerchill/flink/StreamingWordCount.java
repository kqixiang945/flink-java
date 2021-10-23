package org.summerchill.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author kxh
 * @description 从指定的socket读取数据, 对单词进行计算
 * @date 20210624_22:38
 */
public class StreamingWordCount {
    public static void main(String[] args) throws Exception {
        //创建flink流式计算的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建DataStream  DataStreamSource是DataStream的子类
        //Source
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        ////调用Transformation开始
        //调用Transformation
        //https://www.bilibili.com/video/BV1SJ411H7ru?p=1 有对应的flatMap算子的使用图解. 最后把输入的line行的元素切分后,每个元素再单独发送出去.
        SingleOutputStreamOperator<String> wordsDataStream = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            //第一个参数是输入,然后返回多行,多行放入到Collector中.
            public void flatMap(String line, Collector<String> collector) throws Exception {
                //对传入来的一行数据切分成了数组
                String[] words = line.split(" ");
                //循环遍历这个数组
                for (String word : words) {
                    //输出需要用到collector, 把切分后的结果都发送出去.
                    collector.collect(word);
                }
            }
        });
        //将单词和1组合在一起.
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = wordsDataStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                //把单词 和 1 组合到Tuple2中.
                return Tuple2.of(word, 1);
            }
        });
        //再进行分组(把key相同的搞到一个组中)
        //在scala中元组的角标是从1开始的,在java中元组的角标也是从0开始的.
        //keyBy中传入的是tuple,使用tuple中的f0这个字段来进行分组.
        //如下 keyed0 和 keyed1 和 keyed2的三种方式的写法的都是可以的.
        //KeyedStream<Tuple2<String, Integer>, String> keyed0 = wordAndOne.keyBy(t -> t.f0);//t代表的是tuple元素,f0代表的是tuple中的第一个元素.
        //KeyedStream<Tuple2<String, Integer>, String> keyed1 = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
        //    @Override
        //    public String getKey(Tuple2<String, Integer> tuple) throws Exception {
        //        return tuple.f0;
        //    }
        //});
        KeyedStream<Tuple2<String, Integer>, String> keyed2 = wordAndOne.keyBy((KeySelector<Tuple2<String, Integer>, String>) tuple -> tuple.f0);

        //聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = keyed2.sum(1);

        //调用Transformation结束

        //调用Sink
        sumed.print();

        //flink程序要一直运行,在挂起的状态.
        //这个地方不能处理异常,因为要根据异常指定一些重启策略.
        env.execute("StreamingWordCount");

    }
}
