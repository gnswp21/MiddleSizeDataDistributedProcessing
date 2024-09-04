FROM openjdk:11
# install spark


# step 1: install spark
RUN curl -O https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
RUN tar xvf spark-3.5.1-bin-hadoop3.tgz
RUN mv spark-3.5.1-bin-hadoop3 /usr/lib/spark

## step 2: Add required JARs
ARG JAR_HOME=/usr/lib/spark/jars/
# Update permissions
RUN chmod -R +r /usr/lib/spark/jars
ENV SPARK_HOME=/usr/lib/spark
ENV PATH=$PATH:$SPARK_HOME/bin



## step 3: install third party python packages
# install Python packages
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get install -y zip
RUN pip3 install pyspark


# # step 4: copy over custom ETL python modules
COPY process_dev/src /etl/src
RUN cd /etl \
    && zip -r dependency_packages.zip src/ \
    && chmod +x /etl/dependency_packages.zip \
    && cd -
# COPY data /etl/data

## step 5: copy Java/Spark/Hadoop related config
COPY process_dev/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf
RUN mkdir $SPARK_HOME/logs
WORKDIR /etl