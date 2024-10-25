FROM apache/airflow:latest

USER root

RUN curl -s https://jdbc.postgresql.org/download/postgresql-42.7.4.jar -o /opt/postgresql-42.7.4.jar

# Install OpenJDK
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre \
  && apt-get install -y procps \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

COPY requirements.txt .
RUN pip install -r requirements.txt

RUN rm requirements.txt