FROM mysql:8.0

ADD init.sql /docker-entrypoint-initdb.d

RUN chown -R mysql:mysql /docker-entrypoint-initdb.d/
