FROM apache/airflow:2.10.0-python3.12

USER root
RUN rm -f /etc/apt/sources.list.d/debian.sources \
    && echo "deb http://deb.debian.org/debian bookworm main contrib non-free" > /etc/apt/sources.list \
    && echo "deb http://deb.debian.org/debian-security bookworm-security main contrib non-free" >> /etc/apt/sources.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

COPY requirements.txt .
USER airflow
RUN pip install --no-cache-dir -r requirements.txt