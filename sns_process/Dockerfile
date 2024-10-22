# hhttps://hub.docker.com/_/node
FROM node:22

LABEL maintainer="Zella Zhong <zella@mask.io>"

RUN apt-get update -y && \
    apt-get upgrade -y --force-yes && \
    apt-get install -y --force-yes supervisor

WORKDIR /node_app

COPY package*.json ./

RUN npm install -g npm@10

RUN npm install --only=production

COPY . .

ENTRYPOINT ["./run.sh"]