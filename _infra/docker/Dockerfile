FROM --platform=$BUILDPLATFORM golang:1.26.5-alpine AS build-stage
ARG TARGETOS
ARG TARGETARCH

RUN mkdir "/src"
WORKDIR "/src"

COPY . .

RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -v -o worker
RUN chmod 755 worker

FROM alpine:3.24 AS final-stage

RUN apk add --no-cache ca-certificates

RUN addgroup -S csv-worker-group && adduser -S csv-worker-user -G csv-worker-group
RUN mkdir -p "/opt/csv-worker"
RUN chown csv-worker-user:csv-worker-group /opt/csv-worker

WORKDIR "/opt/csv-worker"
COPY --from=build-stage /src/worker .
RUN chmod 550 /opt/csv-worker/worker
RUN chown csv-worker-user:csv-worker-group /opt/csv-worker/worker

USER csv-worker-user

CMD "./worker"
