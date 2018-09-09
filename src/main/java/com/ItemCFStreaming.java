package com;

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.util.*;

public class ItemCFStreaming {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 通过socket监听来输入查询请求
        DataStreamSource<String> userQueryStream = env.socketTextStream("localhost", 10080);
        // 通过读取文件来输入数据
        DataStreamSource<String> userRatingStream = env.addSource(new MovieLenSourceFunc());
        // 合并流
        DataStream<String> sourceStream = userQueryStream.union(userRatingStream);

        //查询流
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> queryStream = sourceStream
                .filter(s -> s.startsWith("1"))
                .map(ItemCFStreaming::convertRating);

        //查询流转换，最终结果输出到redis，方便计算准确率/召回率
        queryStream
                .map(ItemCFStreaming::getAllLiked)
                .flatMap(new PredFlatMapFunc())
                .addSink(buildPredRedisConnector());

        //更新流
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> updateStream = sourceStream
                .filter(x -> x.startsWith("0"))
                .map(ItemCFStreaming::convertRating);

        //更新数据
        updateStream.addSink(buildItemRedisConnector());
        updateStream.addSink(buildUserRedisConnector());
        updateStream.addSink((buildAllItemRedisConnector()));

        //更新受影响的所有相似度，并存储
        updateStream
                .flatMap(new FlatMapFunction<Tuple3<Integer, Integer, Integer>, Tuple2<Tuple2<Integer, List<String>>, Tuple2<String, List<String>>>>() {
                    @Override
                    public void flatMap(Tuple3<Integer, Integer, Integer> rating, Collector<Tuple2<Tuple2<Integer, List<String>>, Tuple2<String, List<String>>>> collector) throws Exception {
                        Jedis redis = JedisConnectionPool.getJedis();
                        List<String> myratings = redis.lrange("item_" + rating.f1, 0, -1);
                        Set<String> items = redis.smembers("items");
                        for (String item : items) {
                            List<String> itemVector = redis.lrange("item_" + item, 0, -1);
                            if (itemVector != null && itemVector.size() > 0) {
                                collector.collect(new Tuple2(new Tuple2(rating.f1, myratings), new Tuple2(item, itemVector)));
                            }
                        }
                        redis.close();
                    }
                })
                .map(new MapFunction<Tuple2<Tuple2<Integer, List<String>>, Tuple2<String, List<String>>>, Tuple2<Integer, Tuple2<String, Double>>>() {
                    @Override
                    public Tuple2<Integer, Tuple2<String, Double>> map(Tuple2<Tuple2<Integer, List<String>>, Tuple2<String, List<String>>> value) throws Exception {
                        Double similarity = calcuSim(value.f0.f1, value.f1.f1);
                        return new Tuple2(value.f0.f0, new Tuple2(value.f1.f0, similarity));
                    }
                }).addSink(buildSimilarityRedisConnector());
        env.execute("ItemCfTest");
    }


    private static SinkFunction<Tuple3<Integer, Integer, Double>> buildPredRedisConnector() {
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
        return new RedisSink<Tuple3<Integer, Integer, Double>>(conf, new RedisMapper<Tuple3<Integer, Integer, Double>>() {

            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.RPUSH, "");
            }

            @Override
            public String getKeyFromData(Tuple3<Integer, Integer, Double> t) {
                return "recommodation";
            }

            @Override
            public String getValueFromData(Tuple3<Integer, Integer, Double> t) {
                return t.f0 + ":" + t.f1 + ":" + t.f2;
            }
        });
    }

    private static Tuple3<Integer, Integer, Double> pred(Tuple3<Integer, List<Tuple2<Integer, Integer>>, Integer> tt) {
        final AtomicDouble total = new AtomicDouble(0);
        tt.f1.stream().forEach(t -> {
            total.addAndGet(t.f1 * findSimilarity(t.f0, tt.f2));
        });
        return new Tuple3(tt.f0, tt.f2, total.doubleValue());
    }

    public static Double findSimilarity(Integer item1, Integer item2) {
        String res = null;
        Jedis redis = JedisConnectionPool.getJedis();
        if (item1 < item2) {
            res = redis.get("similary_" + item1 + ":" + item2);
        } else {
            res = redis.get("similary_" + item2 + ":" + item1);
        }
        redis.close();
        return (res == null) ? 0.0d : Double.parseDouble(res);
    }

    private static Tuple2<Integer, List<Tuple2<Integer, Integer>>> getAllLiked(Tuple3<Integer, Integer, Integer> source) {
        Jedis redis = JedisConnectionPool.getJedis();
        List<String> items = redis.lrange("user_" + source.f0, 0, -1);
        List<Tuple2<Integer, Integer>> resList = new ArrayList<>();
        items.stream().forEach(x -> {
            String[] itemRating = x.split(":");
            resList.add(new Tuple2(Integer.parseInt(itemRating[0]), Integer.parseInt(itemRating[1])));
        });
        redis.close();
        return new Tuple2(source.f0, resList);
    }

    private static SinkFunction<Tuple2<Integer, Tuple2<String, Double>>> buildSimilarityRedisConnector() {
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
        return new RedisSink<Tuple2<Integer, Tuple2<String, Double>>>(conf, new RedisMapper<Tuple2<Integer, Tuple2<String, Double>>>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.SET, "");
            }

            @Override
            public String getKeyFromData(Tuple2<Integer, Tuple2<String, Double>> t) {
                if (t.f0 < Integer.parseInt(t.f1.f0)) {
                    return "similary_" + t.f0 + ":" + t.f1.f0;
                } else {
                    return "similary_" + t.f1.f0 + ":" + t.f0;
                }
            }

            @Override
            public String getValueFromData(Tuple2<Integer, Tuple2<String, Double>> t) {
                return t.f1.f1.toString();
            }
        });
    }

    public static Double calcuSim(List<String> v1, List<String> v2) {
        Map<Integer, Integer> map = new HashMap<>();

        int dinominator1 = 0;
        for (String s : v1) {
            String[] userRating = s.split(":");
            int user = Integer.parseInt(userRating[0]);
            int rating = Integer.parseInt(userRating[1]);
            map.put(user, rating);
            dinominator1 += rating * rating;
        }

        int dinominator2 = 0;
        int numerator = 0;
        for (String s : v2) {
            String[] userRating = s.split(":");
            int user = Integer.parseInt(userRating[0]);
            int rating = Integer.parseInt(userRating[1]);
            dinominator2 += rating * rating;
            if (map.containsKey(user)) {
                numerator += rating * map.get(user);
            }
        }

        return numerator / (Math.sqrt(dinominator1) * Math.sqrt(dinominator2));
    }

    private static Map<Integer, Integer> buildRatingMap(List<String> v1) {
        Map<Integer, Integer> res = new HashMap<Integer, Integer>();
        v1.stream().forEach(x -> {
            String[] userRating = x.split(":");
            res.put(Integer.parseInt(userRating[0]), Integer.parseInt(userRating[1]));
        });
        return res;
    }

    private static SinkFunction<Tuple3<Integer, Integer, Integer>> buildUserRedisConnector() {
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
        return new RedisSink<Tuple3<Integer, Integer, Integer>>(conf, new RedisMapper<Tuple3<Integer, Integer, Integer>>() {

            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.RPUSH, "");
            }

            @Override
            public String getKeyFromData(Tuple3<Integer, Integer, Integer> t) {
                return "user_" + t.f0.toString();
            }

            @Override
            public String getValueFromData(Tuple3<Integer, Integer, Integer> t) {
                return t.f1 + ":" + t.f2;
            }
        });
    }

    private static SinkFunction<Tuple3<Integer, Integer, Integer>> buildItemRedisConnector() {
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
        return new RedisSink<Tuple3<Integer, Integer, Integer>>(conf, new RedisMapper<Tuple3<Integer, Integer, Integer>>() {

            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.RPUSH, "");
            }

            @Override
            public String getKeyFromData(Tuple3<Integer, Integer, Integer> t) {
                return "item_" + t.f1.toString();
            }

            @Override
            public String getValueFromData(Tuple3<Integer, Integer, Integer> t) {
                return t.f0 + ":" + t.f2;
            }
        });
    }

    private static SinkFunction<Tuple3<Integer, Integer, Integer>> buildAllItemRedisConnector() {
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
        return new RedisSink<Tuple3<Integer, Integer, Integer>>(conf, new RedisMapper<Tuple3<Integer, Integer, Integer>>() {

            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.SADD, "");
            }

            @Override
            public String getKeyFromData(Tuple3<Integer, Integer, Integer> t) {
                return "items";
            }

            @Override
            public String getValueFromData(Tuple3<Integer, Integer, Integer> t) {
                return t.f1.toString();
            }
        });
    }

    public static Tuple3<Integer, Integer, Integer> convertRating(String line) {
        String[] lineText = line.split("\t");
        return new Tuple3<>(Integer.parseInt(lineText[1]), Integer.parseInt(lineText[2]), Integer.parseInt(lineText[3]));
    }
}