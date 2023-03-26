FROM foundationdb/foundationdb:7.1.26
FROM openjdk:8-jdk

COPY --from=0 /usr/lib/fdb/multiversion/ /usr/local/lib/fdb/

RUN echo "/usr/local/lib/fdb">>/etc/ld.so.conf.d/libc.conf
RUN /sbin/ldconfig

COPY ./fdb.cluster /etc/foundationdb/fdb.cluster
COPY ./*.jar /app.jar
COPY ./*.json /
CMD ["java","-jar","-Xms1024m","-Xmx1536m","app.jar"]

