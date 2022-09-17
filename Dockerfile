# syntax=docker/dockerfile:1

FROM node:16.17-alpine3.16

WORKDIR /app

COPY package.json ./
COPY package-lock.json ./
RUN npm install

COPY src ./src/
COPY build.js tsconfig.json ./
RUN npm run build

COPY ./wrangler.toml ./

EXPOSE 8787

ENTRYPOINT [ "npm", "start" ]
