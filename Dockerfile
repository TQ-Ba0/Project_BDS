FROM apache/airflow:2.7.1-python3.10

USER root

# Install OpenJDK-11
RUN apt-get update && \ 
    apt-get install -y openjdk-11-jdk && \ 
    apt install xvfb -y &&\
    apt-get install -y ant && \
    apt-get -yqq install curl unzip &&\
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME
RUN apt-get install -y wget
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN apt-get install -y ./google-chrome-stable_current_amd64.deb

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip && \
    pip install -r /requirements.txt && \
    pip install apache-airflow

