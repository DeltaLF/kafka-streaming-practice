package com.learnkafkastreams.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.Revenue;
import com.learnkafkastreams.serdes.SerdesFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OrdersTopology {
    public static final String ORDERS = "orders";
    public static final String STORES = "stores";

    public static final String RESTAURANT_ORDERS = "restaurant_orders";
    public static final String GENERAL_ORDERS = "general_orders";

    public static Topology buildTopology() {

        Predicate<String, Order> generalPredicate = (key, order) -> order.orderType().equals(OrderType.GENERAL);
        Predicate<String, Order> restaurantPredicate = (key, order) -> order.orderType().equals(OrderType.RESTAURANT);

        ValueMapper<Order, Revenue> revenueMapper = order -> new Revenue(order.locationId(), order.finalAmount());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var orderStream = streamsBuilder
                .stream(ORDERS,
                        Consumed.with(Serdes.String(), SerdesFactory.orderSerdes()));

        orderStream.print(Printed.<String, Order>toSysOut().withLabel("orders"));

        orderStream.split(Named.as("General-restaurant-stream"))
                .branch(generalPredicate,
                        Branched.withConsumer(generalOrdersStream -> {
                            generalOrdersStream.print(Printed.<String, Order>toSysOut().withLabel("generalStream"));

                            generalOrdersStream
                                    .mapValues((readonlyKey, value) -> revenueMapper.apply(value))
                                    .to(GENERAL_ORDERS,
                                            Produced.with(Serdes.String(), SerdesFactory.revenueSerdes()));
                        }))
                .branch(restaurantPredicate,
                        Branched.withConsumer(restaurantOrdersStream -> {
                            restaurantOrdersStream
                                    .print(Printed.<String, Order>toSysOut().withLabel("restaurantStream"));

                            restaurantOrdersStream
                                    .mapValues((readonlyKey, value) -> revenueMapper.apply(value))
                                    .to(RESTAURANT_ORDERS,
                                            Produced.with(Serdes.String(), SerdesFactory.revenueSerdes()));
                        }));

        return streamsBuilder.build();
    }
}