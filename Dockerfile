# Build stage
FROM golang:1.24-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the temporal server
RUN make temporal-server

# Runtime stage
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Create temporal user
RUN addgroup -g 1000 temporal && \
    adduser -D -s /bin/sh -u 1000 -G temporal temporal

# Set working directory
WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/temporal-server /app/temporal-server

# Copy config files
COPY --from=builder /app/config /app/config

# Change ownership
RUN chown -R temporal:temporal /app

# Switch to temporal user
USER temporal

# Expose ports
EXPOSE 7233 7234 7235 7239 6933 6934 6935 6939 9090

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:9090/stats/prometheus || exit 1

# Run the server
ENTRYPOINT ["/app/temporal-server"]
CMD ["--env", "development-mongodb", "--allow-no-auth", "start"] 