package com.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class Words
{
    public static void main(String args[]) throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream= env.readTextFile("C:\\workspace\\flinkmoviepoc-master\\ml-latest-small\\movies.csv")
             .flatMap(new Spiltter());

        dataStream.print();
        env.execute();

    }

    public static class Spiltter implements FlatMapFunction<String, String>
    {
        public void flatMap(String sentence, Collector<String> out) throws Exception
        {
            for ( String word:sentence.split(","))
            {
                out.collect(word);
            }
        }
    }

    public static class MovieFr implements MapFunction<String, String >
    {

        public String map(String inpput) throws Exception
        {
            return inpput + "****";
        }
    }

    public static class Filter implements FilterFunction<String>
    {

        public boolean filter(String s) throws Exception
        {
            if ( s.contains("Comedy"))
            {
                return false;
            }
            else
                return true;

        }
    }
}
