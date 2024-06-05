FROM --platform=linux/x86_64 sanketikahub/flink:1.15.2-scala_2.12-jdk-11 as neo4j-connector-image
USER flink
COPY ./target/neo4j-connector-1.0.0.jar $FLINK_HOME/lib