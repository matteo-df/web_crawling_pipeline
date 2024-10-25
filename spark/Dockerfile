FROM bitnami/spark:latest
USER root
RUN install_packages curl
RUN pip install dnspython requests pycountry
USER 1001
RUN curl https://jdbc.postgresql.org/download/postgresql-42.7.4.jar -o /opt/bitnami/spark/jars/postgresql-42.7.4.jar