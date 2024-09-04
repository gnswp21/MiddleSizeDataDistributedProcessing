FROM openjdk:11
# install spark


# step 1: install spark
RUN curl -O https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
RUN tar xvf spark-3.5.1-bin-hadoop3.tgz
RUN mv spark-3.5.1-bin-hadoop3 /usr/lib/spark

## step 2: Add required JARs
ARG JAR_HOME=/usr/lib/spark/jars/
# hadoop aws 3.3.4 - Download jar at build
# org.apache.hadoop#hadoop-aws added as a dependency ...
# downloading https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar ...
# downloading https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar ...
# downloading https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar ...
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar $JAR_HOME
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar $JAR_HOME
ADD https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar $JAR_HOME
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