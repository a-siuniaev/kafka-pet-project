#About
Simple Kafka app with having 
- producer which reads ticker data from [CryptoCompare](https://min-api.cryptocompare.com/documentation/websockets?key=Channels&cat=Ticker) and sends it to Kafka topic
- consumer which reads data from Kafka topic and sends it to ElasticSearch 