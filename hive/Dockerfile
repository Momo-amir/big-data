FROM alpine AS downloader
RUN wget -q \
    "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar" \
    -O /mysql-connector-j.jar

FROM apache/hive:4.0.0
USER root
COPY --from=downloader /mysql-connector-j.jar /opt/hive/lib/mysql-connector-j.jar
# Fix Hive 4.0 bug: startup script detects its own bash wrapper (PID 7) as a
# running HiveServer2 instance via PID file check. We wrap the entrypoint to
# delete all PID files before calling the real entrypoint.
RUN mkdir -p /home/hive/.beeline && chown -R hive:hive /home/hive
RUN printf '#!/bin/sh\nrm -f /tmp/hive/*.pid 2>/dev/null\nexec /entrypoint.sh "$@"\n' \
    > /start.sh && chmod +x /start.sh
USER hive
ENTRYPOINT ["/start.sh"]
