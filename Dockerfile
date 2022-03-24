# Compile stage
FROM golang:1.17 AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -ldflags="-s -w"

# Image build stage
FROM alpine:latest  
RUN apk --no-cache add ca-certificates; \
    adduser -h / -s /sbin/nologin -G users -S -D -H lowrunner
WORKDIR /
COPY --from=build /app/low-runner .
EXPOSE 1323

USER lowrunner
CMD ["/low-runner"]

