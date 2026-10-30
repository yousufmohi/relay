# One multi-stage Dockerfile for every Go component:
#   docker build --build-arg COMPONENT=router -t relay/router .
# COMPONENT ∈ router | sidecar | autoscaler | loadgen | simworker
FROM golang:1.26-alpine AS build
ARG COMPONENT=router
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./${COMPONENT}/cmd/${COMPONENT}

FROM alpine:3.22
# curl: used by the model container's preStop hook to call the sidecar's /drain.
RUN apk add --no-cache ca-certificates curl && adduser -D -u 10001 relay
COPY --from=build /out/app /usr/local/bin/app
COPY experiments/workloads /workloads
USER relay
ENTRYPOINT ["/usr/local/bin/app"]
