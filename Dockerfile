# Build the worker binary, then run it on a slim base.
FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/worker ./cmd/worker

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/worker /worker
EXPOSE 2112
ENTRYPOINT ["/worker"]
