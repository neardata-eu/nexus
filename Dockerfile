# Stage 1: Build the JAR
# After some research and trials, JDK 21 is the most comptabile version with the current Nexus implementation
# As a result, Gradle 8.3 will be used since it is the most compatabile one with said JDK
FROM openjdk:21-jdk-slim AS build

# Installing Gradle
RUN apt-get update && \
    apt-get install -y wget unzip && \
    wget https://services.gradle.org/distributions/gradle-8.3-bin.zip -P /tmp && \
    unzip /tmp/gradle-8.3-bin.zip -d /opt && \
    ln -s /opt/gradle-8.3/bin/gradle /usr/bin/gradle && \
    rm -rf /var/lib/apt/lists/* /tmp/gradle-8.3-bin.zip

WORKDIR /app
COPY . .

# Using gradle's shadowJar to build a single JAR with its dependencies 
RUN gradle shadowJar

#########################
# Stage 2: Copy the generated JAR from the previous stage and run it
FROM openjdk:21-jdk-slim

#Setting up default environment variables for Nexus's services
ENV \
    #Redis Config
    REDIS_HOST="0.0.0.0" \
    REDIS_PORT=6379 \
    # S3Proxy Config
    S3PROXY_IDENTITY="dev-identity" \
    S3PROXY_CREDENTIAL="dev-credential" \
    S3PROXY_ENDPOINT="http://0.0.0.0:8181" \
    LOG_LEVEL="info" \
    S3PROXY_AUTHORIZATION="none" \
    S3PROXY_VIRTUALHOST="" \
    S3PROXY_KEYSTORE_PATH="keystore.jks" \
    S3PROXY_KEYSTORE_PASSWORD="password" \
    S3PROXY_CORS_ALLOW_ALL="false" \
    S3PROXY_CORS_ALLOW_ORIGINS="" \
    S3PROXY_CORS_ALLOW_METHODS="" \
    S3PROXY_CORS_ALLOW_HEADERS="" \
    S3PROXY_CORS_ALLOW_CREDENTIAL="" \
    S3PROXY_IGNORE_UNKNOWN_HEADERS="false" \
    S3PROXY_ENCRYPTED_BLOBSTORE="" \
    S3PROXY_ENCRYPTED_BLOBSTORE_PASSWORD="" \
    S3PROXY_ENCRYPTED_BLOBSTORE_SALT="" \
    S3PROXY_READ_ONLY_BLOBSTORE="false" \
    #JClouds Config
    JCLOUDS_PROVIDER="filesystem" \
    JCLOUDS_IDENTITY="dev-identity" \
    JCLOUDS_CREDENTIAL="dev-credential" \
    JCLOUDS_FILESYSTEM_BASEDIR="/tmp/blobstore" \
    JCLOUDS_ENDPOINT="http://0.0.0.0:9000" \
    JCLOUDS_REGION="" \
    JCLOUDS_REGIONS="us-east-1" \
    JCLOUDS_KEYSTONE_VERSION="" \
    JCLOUDS_KEYSTONE_SCOPE="" \
    JCLOUDS_KEYSTONE_PROJECT_DOMAIN_NAME="" 

WORKDIR /app

COPY --from=build /app/build/libs/nexus-java.jar /app
#Shell command that properly routes the environment variables
COPY /src/main/resources/docker_entrypoint.sh /app

RUN chmod +x docker_entrypoint.sh

#Web server port
EXPOSE 8080
EXPOSE 8181

ENTRYPOINT ["/app/docker_entrypoint.sh"]