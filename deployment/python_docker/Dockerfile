FROM foundationdb/foundationdb:7.1.26
WORKDIR /


FROM python:3.7

WORKDIR /

COPY --from=0 /usr/lib/fdb/multiversion/ /usr/local/lib/fdb/
COPY ./fdb.cluster /var/fdb/fdb.cluster

RUN pip3 install -i https://pypi.tuna.tsinghua.edu.cn/simple  foundationdb==7.1.26 sqlparse sql-metadata
RUN pip3 install -i https://pypi.tuna.tsinghua.edu.cn/simple  flask gunicorn tornado

RUN echo "/usr/local/lib/fdb">>/etc/ld.so.conf.d/libc.conf

RUN /sbin/ldconfig

COPY ./src /src

WORKDIR /src

#CMD ["python","-m","flask","run","--host","0.0.0.0"]

CMD ["gunicorn","-w","5","--worker-class=tornado","-b","0.0.0.0:5000","app:app"]
#CMD ["gunicorn","-b","0.0.0.0:5000","app:app"]



