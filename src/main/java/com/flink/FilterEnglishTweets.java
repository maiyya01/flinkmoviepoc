package com.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.util.Properties;

public class FilterEnglishTweets
{
    public static void main(String[] args) throws Exception
    {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, "ChClkJirU37VxrXBLwxpo5SAP");
        props.setProperty(TwitterSource.CONSUMER_SECRET,"pm3I3ndhI01Q82cCxD4RFdHaIjXpC1HZDJEfuP6O2hDhq3vfys");
        props.setProperty(TwitterSource.TOKEN,"3039197883-eLKqVaH4MNNk8YxDZWIg4ij9fuvmWfBw1JAH1oW");
        props.setProperty(TwitterSource.TOKEN_SECRET,"z9oXO9KM2XzZTA4s1Ojmgj6CO2WDh5SHUQgbwRiPJHlc3");


        env.addSource(new TwitterSource(props))
                .map(new MaptoTweet())
                .filter(new FilterFunction<Tweet>() {
                    @Override
                    public boolean filter(Tweet tweet) throws Exception {
                        return tweet.getLanguage().contains("en");
                    }
                })
                .print();

        env.execute();


    }

    private static class MaptoTweet implements MapFunction<String, Tweet>
    {

        static private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public Tweet map(String s) throws Exception
        {
            JsonNode tweetJson = mapper.readTree(s);
            JsonNode textNode = tweetJson.get("text");
            JsonNode langNode = tweetJson.get("lang");

            String text = textNode == null ? "" : textNode.toString();
            String lang = langNode == null ? "" : langNode.toString();
            return new Tweet(lang, text);

        }
    }

    public static class Tweet
    {
        private String language;
        private String text;

        public Tweet(String language, String text)
        {
            this.language = language;
            this.text = text;
        }

        public String getLanguage()
        {
            return language;
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
