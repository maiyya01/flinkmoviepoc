package com.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class LanguageControlStream
{

    public static void main(String[] args)
    {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        Properties props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, "ChClkJirU37VxrXBLwxpo5SAP");
        props.setProperty(TwitterSource.CONSUMER_SECRET,"pm3I3ndhI01Q82cCxD4RFdHaIjXpC1HZDJEfuP6O2hDhq3vfys");
        props.setProperty(TwitterSource.TOKEN,"3039197883-eLKqVaH4MNNk8YxDZWIg4ij9fuvmWfBw1JAH1oW");
        props.setProperty(TwitterSource.TOKEN_SECRET,"z9oXO9KM2XzZTA4s1Ojmgj6CO2WDh5SHUQgbwRiPJHlc3");

        DataStream<LanguageConfig> controlStream = env.socketTextStream("localhost", 9876)
                .flatMap(new FlatMapFunction<String, LanguageConfig>() {
                    @Override
                    public void flatMap(String s, Collector<LanguageConfig> collector) throws Exception {
                        for(String langConfig : s.split(","))
                        {
                            String[] keyPair = langConfig.split("=");
                            collector.collect( new LanguageConfig(keyPair[0], Boolean.parseBoolean(keyPair[1])));

                        }
                    }
                });


        env.addSource(new TwitterSource(props))
                .map( new MaptoTweet())
                .keyBy(new KeySelector<Tweet, String>() {
                    @Override
                    public String getKey(Tweet tweet) throws Exception
                    {
                        return tweet.getLanguage();
                    }
                })
                .connect(controlStream.keyBy(new KeySelector<LanguageConfig, String>() {
                        @Override
                        public String getKey(LanguageConfig languageConfig) throws Exception
                        {
                            return languageConfig.getLanguage();
                        }
                    }))
                        .flatMap(new RichCoFlatMapFunction<Tweet, LanguageConfig, Tuple2<String,String>>() {

                            ValueStateDescriptor<Boolean> shouldProcess = new ValueStateDescriptor<Boolean>(
                                    "langaugeCOnfig", Boolean.class
                            );

                            @Override
                            public void flatMap1(Tweet tweet,Collector<Tuple2<String, String>> collector) throws Exception
                            {
                                Boolean processLangauge = getRuntimeContext().getState(shouldProcess).value();
                                if ( processLangauge != null && processLangauge )
                                {
                                    for( String tag : tweet.getTags())
                                    {
                                        collector.collect(new Tuple2<>(tweet.getLanguage(), tag));
                                    }
                                }
                            }

                            @Override
                            public void flatMap2(LanguageConfig languageConfig, Collector<Tuple2<String, String>> collector) throws Exception
                            {
                                getRuntimeContext().getState(shouldProcess).update(languageConfig.isShouldProcess());

                            }
                        })
                .print();




    }

    public static class LanguageConfig
    {
        private String language;
        private boolean shouldProcess;

        public LanguageConfig(String language, boolean shouldProcess)
        {
            this.language = language;
            this.shouldProcess = shouldProcess;
        }

        public String getLanguage()
        {
            return language;
        }

        public boolean isShouldProcess()
        {
            return shouldProcess;
        }
    }

    public static class MaptoTweet implements MapFunction<String, Tweet>
    {

        static private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public Tweet map(String tweetJsonStr) throws Exception
        {
            JsonNode tweetJson = mapper.readTree(tweetJsonStr);
            JsonNode textNode = tweetJson.get("text");
            JsonNode langNode = tweetJson.get("lang");

            String text = textNode == null ? "" : textNode.toString();
            String lang = langNode == null ? "" : langNode.toString();

            List<String> tags = new ArrayList<>();

            JsonNode entities = tweetJson.get("entities");
            if ( entities != null)
            {
                JsonNode hashTags = entities.get("hashtags");

                for (Iterator<JsonNode> iter = hashTags.elements(); iter.hasNext();)
                {
                    JsonNode node = iter.next();
                    String hashtag = node.get("text").toString();
                    tags.add(hashtag);

                }
            }
            return new Tweet(lang, text, tags);

        }
    }

    public static class Tweet
    {
        private String language;
        private String text;
        private List<String> tags;


        public Tweet(String language, String text, List<String> tags)
        {
            this.language = language;
            this.text = text;
            this.tags = tags;
        }



        public String getLanguage()
        {
            return language;
        }

        public List<String> getTags()
        {
            return tags;
        }

        public String getText()
        {
            return text;
        }

        @Override
        public String toString()
        {
            return " Tweet {" +
                    " Lang:" + this.language +
                    " Text: " + this.text;
        }
    }
}
