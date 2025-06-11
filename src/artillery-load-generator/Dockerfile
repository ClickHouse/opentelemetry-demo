FROM artilleryio/artillery:latest

WORKDIR /app

# Copy scripts and test definition
COPY flows.js .
COPY load-test.template.yaml ./load-test.template.yaml
COPY run.sh .
COPY package.json .
COPY package-lock.json .

# Install dependencies (faker)
RUN npm install

# Make the run script executable
RUN chmod +x run.sh

RUN apt-get update && apt-get install -y gettext-base

ENTRYPOINT ["/bin/sh", "./run.sh"]
