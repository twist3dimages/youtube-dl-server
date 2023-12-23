#
# youtube-dl Server Dockerfile
#
# https://github.com/nbr23/youtube-dl-server
#

FROM --platform=$BUILDPLATFORM node:21-alpine as nodebuild

WORKDIR /app
COPY ./front/package*.json /app
RUN npm ci
COPY ./front /app
RUN npm run build


FROM python:alpine3.18 as wheels

RUN apk add --no-cache g++
RUN pip install --upgrade --no-cache-dir pip && pip wheel --no-cache-dir --no-deps --wheel-dir /out/wheels brotli pycryptodomex websockets pyyaml

FROM python:alpine3.18
ARG YOUTUBE_DL=youtube_dl
ARG ATOMICPARSLEY=0
ARG YDLS_VERSION
ARG YDLS_RELEASE_DATE

ENV YDLS_VERSION=$YDLS_VERSION
ENV YDLS_RELEASE_DATE=$YDLS_RELEASE_DATE

VOLUME "/youtube-dl"
VOLUME "/app_config"

COPY --from=wheels /out/wheels /wheels
RUN pip install --no-cache /wheels/*

RUN mkdir -p /usr/src/app
RUN apk add --no-cache ffmpeg tzdata mailcap
RUN if [ $ATOMICPARSLEY == 1 ]; then apk add --no-cache -X http://dl-cdn.alpinelinux.org/alpine/edge/testing atomicparsley; ln /usr/bin/atomicparsley /usr/bin/AtomicParsley || true; fi
COPY ./requirements.txt /usr/src/app/
RUN pip install --upgrade pip && pip install --no-cache-dir -r <(cat /usr/src/app/requirements.txt| grep -v yt-dlp)

COPY ./config.yml /usr/src/app/default_config.yml
COPY ./ydl_server /usr/src/app/ydl_server
COPY ./youtube-dl-server.py /usr/src/app/

COPY --from=nodebuild /app/dist /usr/src/app/ydl_server/static

WORKDIR /usr/src/app

EXPOSE 8080

ENV YOUTUBE_DL=$YOUTUBE_DL
ENV YDL_CONFIG_PATH='/app_config'
# Set default environment variables, can be overridden at runtime
ENV DB_TYPE=sqlite
ENV DB_HOST=localhost
ENV DB_PORT=3306
ENV DB_NAME=your_database_name
ENV DB_USER=your_database_user
ENV DB_PASSWORD=your_database_password
CMD [ "python", "-u", "./youtube-dl-server.py" ]

HEALTHCHECK CMD wget 127.0.0.1:8080/api/info --spider -q
