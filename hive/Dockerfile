FROM alpine AS downloader
RUN wget -q \
    "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar" \
    -O /mysql-connector-j.jar

FROM apache/hive:4.0.0
USER root
COPY --from=downloader /mysql-connector-j.jar /opt/hive/lib/mysql-connector-j.jar
USER hive
