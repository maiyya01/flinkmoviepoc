package com.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterStrings
{
    public static void main(String args[]) throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream= env.readTextFile("C:\\workspace\\flinkmoviepoc-master\\ml-latest-small\\movies.csv")
                .filter(new Filter());
        dataStream.print();
        env.execute();

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
