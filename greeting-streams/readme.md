### docker kafka broker

```

docker exec --interactive --tty broker kafka-topics --bootstrap-server broker:9092 --list


# producer
docker exec --interactive --tty broker kafka-console-producer --bootstrap-server broker:9092 --topic greetings

# consumer
 docker exec --interactive --tty broker kafka-console-consumer --bootstrap-server broker:9092 --topic greetings_uppercase
```
