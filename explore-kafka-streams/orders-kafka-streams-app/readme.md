### docker kafka broker

```

docker exec --interactive --tty broker kafka-topics --bootstrap-server broker:9092 --list


# producer
docker exec --interactive --tty broker kafka-console-producer --bootstrap-server broker:9092 --topic stores


# consumer
 docker exec --interactive --tty broker kafka-console-consumer --bootstrap-server broker:9092 --topic orders
 docker exec --interactive --tty broker kafka-console-consumer --bootstrap-server broker:9092 --topic restaurant_orders
 docker exec --interactive --tty broker kafka-console-consumer --bootstrap-server broker:9092 --topic general_orders

```
