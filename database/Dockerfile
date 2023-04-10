FROM postgres:latest
ENV POSTGRES_PASSWORD=0PZ9TVXV
ENV POSTGRES_USER=acquisition
ENV POSTGRES_DB=data_acquisition
COPY create_fixtures.sql /docker-entrypoint-initdb.d/create_fixtures.sql
