package com.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Int;

public class WorldCount
{

    public static void main(String args[]) throws Exception
    {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = env.readTextFile("C:\\workspace\\flinkmoviepoc-master\\ml-latest-small\\movies.csv")
                .flatMap(new WorldCountSplitter())
                .keyBy(0)
                .sum(1);

        dataStream.print();
        env.execute();
    }

    public static class WorldCountSplitter implements FlatMapFunction<String, Tuple2<String, Integer>>
    {
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception
        {
            for ( String world : sentence.split(","))
            {
                out.collect(new Tuple2<String, Integer> (world, 1) );
            }

        }


    }
}
