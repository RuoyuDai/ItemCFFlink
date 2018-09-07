package com;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.stream.Collectors;

public class PredFlatMapFunc implements FlatMapFunction<Tuple2<Integer, List<Tuple2<Integer, Integer>>>, Tuple3<Integer, Integer, Double>> {
    @Override
    public void flatMap(Tuple2<Integer, List<Tuple2<Integer, Integer>>> userLiked, Collector<Tuple3<Integer, Integer, Double>> collector) throws Exception {
        final List<Tuple2<Integer, Integer>> userLikedItems = userLiked.f1;
        final int userId = userLiked.f0;

        if (userLikedItems.size() == 0) {
            System.out.println("User:" + userLiked.f0 + " has no liked");
        }

        Map<Integer, Integer> likedItemMap = toMap(userLikedItems);
        List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Double>>> simiItems = new ArrayList<>();

        userLikedItems.stream().forEach(oneItemAndRating -> {
            List<Tuple2<Integer, Double>> likelyItem = findLikely(oneItemAndRating.f0, likedItemMap, ItemCFStreaming.redis);
            likelyItem.stream().forEach(x -> {
                simiItems.add(new Tuple2(oneItemAndRating, x));
            });
        });

        Map<Integer, Double> predMap = new HashMap<>();
        simiItems.forEach(x -> {
            if (predMap.containsKey(x.f1.f0)) {
                double predVSoFar = predMap.get(x.f1.f0);
                predMap.put(x.f1.f0, predVSoFar + x.f0.f1 * x.f1.f1);

            } else {
                predMap.put(x.f1.f0, x.f0.f1 * x.f1.f1);
            }
        });

        List<Tuple2<Integer, Double>> predList = new ArrayList<>();
        predMap.entrySet()
                .forEach(entry -> predList.add(new Tuple2(entry.getKey(), entry.getValue())));

        predList.sort(new Comparator<Tuple2<Integer, Double>>() {
            @Override
            public int compare(Tuple2<Integer, Double> o1, Tuple2<Integer, Double> o2) {
                return o1.f1 < o2.f1 ? 1 : -1;
            }
        });

        for (int i = 0; i < 3; i++) {
            if (predList.size() > i) {
                collector.collect(new Tuple3(userId, predList.get(i).f0, predList.get(i).f1));
            }
        }
    }

    private static Map<Integer,Integer> toMap(List<Tuple2<Integer,Integer>> userLikedItems) {
        Map<Integer, Integer> likeMap = new HashMap<>();
        userLikedItems.forEach(x -> likeMap.put(x.f0, 1));
        return likeMap;
    }

    private static List<Tuple2<Integer, Double>> findLikely(Integer user, Map<Integer, Integer> userLikedItems, Jedis redis) {
        List<Tuple2<Integer, Double>> res = new LinkedList<>();

        Set<String> items = redis.smembers("items");
        items.stream().forEach(x -> {
            Double simi = ItemCFStreaming.findSimilarity(user, Integer.parseInt(x));
            if(simi > 0) {
                res.add(new Tuple2(Integer.parseInt(x), simi));
            }
        });

        res.sort(new Comparator<Tuple2<Integer, Double>>() {
            @Override
            public int compare(Tuple2<Integer, Double> o1, Tuple2<Integer, Double> o2) {
                if(o1.f1 < o2.f1){
                    return 1;
                }
                else {
                    return -1;
                }
            }
        });


        List<Tuple2<Integer, Double>> result = new LinkedList<>();
        int count = 0;
        for(int i = 0; i < res.size(); i++){
            if(!userLikedItems.containsKey(res.get(i).f0) && count < 3){
                result.add(res.get(i));
                count++;
            }
        }
        return result;
    }
}
