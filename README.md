# Kafka Simulator - User Manual

## Required Order for execution:
1. Zookeeper
2. Broker1
3. Broker2
4. Broker3
5. Producers/Consumers

## Steps:
1. Run Zookeeper: python3 zookeper.py
2. Run Broker1: python3 brokers/broker1/broker.py
3. Run Broker2: python3 brokers/broker2/broker.py
4. Run Broker3: python3 brokers/broker3/broker.py

- Note: After starting broker1 run the other 2 brokers within 5 seconds (because there is a sleep of 5 seconds before establishing a connection between the brokers).
- This is needed only at the time of initial connection.

5. Run 'N' producers parallelly. They are assigned random and unique port numbers: python3 producers/producer.py
6. Run 'N' consumers parallelly. They are assigned random and unique port numbers: python3 consumers/consumer.py
- Note: To see messages from beginning: python3 consumers/consumer.py --from-beginning

## Zookeeper:
_The Zookeeper polls all the brokers every 15 seconds for heartbeat. If the leader has died, it elects a new leader (based on FCFS, i.e., next in line). All the active clients get connected automatically to the newly elected leader broker. If the newly elected broker also dies later on, that also is handled._
