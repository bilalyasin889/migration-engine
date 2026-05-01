# Stage 1: Grab Flyway
FROM alpine:3.20 as flyway-provider
ARG FLYWAY_VERSION=12.5.0

RUN apk add --no-cache curl
RUN curl -L https://download.red-gate.com/maven/release/com/redgate/flyway/flyway-commandline/${FLYWAY_VERSION}/flyway-commandline-${FLYWAY_VERSION}-linux-x64.tar.gz -o flyway.tar.gz \
    && tar -xzf flyway.tar.gz

# Stage 2: Final Image
FROM public.ecr.aws/lambda/python:3.11

# Install Java 17 (Flyway requirement)
RUN yum install -y java-17-amazon-corretto-devel unzip && yum clean all

# Copy Flyway
COPY --from=flyway-provider /flyway-* /flyway
RUN ln -s /flyway/flyway /usr/local/bin/flyway

# Setup App
WORKDIR ${LAMBDA_TASK_ROOT}
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY src/ src/

# Default for Lambda
CMD [ "src.engine.lambda_handler" ]