FROM 996579266876.dkr.ecr.ap-northeast-2.amazonaws.com/spark/emr-6.5.0:latest
# step 0: preparation
USER root
# step 1: install os level pkg
RUN yum -y install zip

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
RUN pip3 install nltk


# # step 4: copy over custom ETL python modules
COPY process/src /etl/src
RUN cd /etl \
    && zip -r dependency_packages.zip src/ \
    && chmod +x /etl/dependency_packages.zip \
    && cd -
# COPY data /etl/data

## step 5: copy Java/Spark/Hadoop related config
WORKDIR /etl