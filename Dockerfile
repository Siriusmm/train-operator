# Build the manager binary
#FROM hub.kubesphere.com.cn/public/golang:1.22-arm as builder
FROM golang:1.22 as builder

WORKDIR /workspace
# Copy the Go Modules manifests
COPY ./go.mod go.mod
COPY ./go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go env -w GOPROXY=https://goproxy.cn,direct
RUN go mod download

# Copy the go source
COPY . .
RUN  go mod tidy

# Build
RUN CGO_ENABLED=0 GOOS=linux GO111MODULE=on go build -a -o manager cmd/training-operator.v1/main.go

# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM hub.kubesphere.com.cn/public/distroless/static:latest
WORKDIR /
COPY --from=builder /workspace/manager .
ENTRYPOINT ["/manager"]
