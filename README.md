# SmarttMininggKafka
Lex course on real world application

# Running Process

- Create jar file
```
mvn clean compile assembly:single
```
- Put it in the same dir as dataset
- Run the below command
```
java -jar <jar-name> <topic-name> <data-set-name> <output-file-name>
```
- Filtered data is stored in new topic named "<original-topic>_FILTERED"
