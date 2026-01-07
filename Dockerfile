FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS build-stage

RUN mkdir "/src"
WORKDIR "/src"

COPY . .

RUN go build -v -o main
RUN chmod 755 main

FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS final-stage

RUN addgroup -S csv-worker-group && adduser -S csv-worker-user -G csv-worker-group
RUN mkdir -p "/opt/csv-worker"
RUN chown csv-worker-user:csv-worker-group /opt/csv-worker

WORKDIR "/opt/csv-worker"
COPY --from=build-stage /src/main .
RUN chmod 550 /opt/csv-worker/main
RUN chown csv-worker-user:csv-worker-group /opt/csv-worker/main

USER csv-worker-user

CMD "./main"
