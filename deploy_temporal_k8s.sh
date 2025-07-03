#!/bin/bash

echo "Deploying Temporal to Kubernetes cluster..."
echo "=========================================="

# Create a ConfigMap with the Temporal configuration
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: temporal-config
  namespace: rafay-core
data:
  docker.yaml: |
    log:
      stdout: true
      level: info
    
    persistence:
      numHistoryShards: 4
      defaultStore: default
      visibilityStore: visibility
      datastores:
        default:
          mongodb:
            connectAddr: rafay-mongodb-0.rafay-mongodb-svc.rafay-core.svc.cluster.local:27017
            databaseName: temporal
            username: aisrvdbuser
            password: aisrvdbuser
            maxConns: 20
            maxIdleConns: 20
            maxConnLifetime: 1h
            writeConcern: majority
            readConcern: majority
            readPreference: primary
        visibility:
          mongodb:
            connectAddr: rafay-mongodb-0.rafay-mongodb-svc.rafay-core.svc.cluster.local:27017
            databaseName: temporal
            username: aisrvdbuser
            password: aisrvdbuser
            maxConns: 10
            maxIdleConns: 10
            maxConnLifetime: 1h
            writeConcern: majority
            readConcern: majority
            readPreference: primary
    
    clusterMetadata:
      enableGlobalNamespace: false
      failoverVersionIncrement: 10
      masterClusterName: "active"
      currentClusterName: "active"
      clusterInformation:
        active:
          enabled: true
          initialFailoverVersion: 1
          rpcName: "frontend"
          rpcAddress: "127.0.0.1:7233"
          httpAddress: "127.0.0.1:7243"
    
    services:
      frontend:
        rpc:
          grpcPort: 7233
          membershipPort: 6933
          bindOnIP: "0.0.0.0"
          httpPort: 7243
      matching:
        rpc:
          grpcPort: 7235
          membershipPort: 6935
          bindOnIP: "0.0.0.0"
      history:
        rpc:
          grpcPort: 7234
          membershipPort: 6934
          bindOnIP: "0.0.0.0"
      worker:
        rpc:
          grpcPort: 7239
          membershipPort: 6939
          bindOnIP: "0.0.0.0"
    
    dcRedirectionPolicy:
      policy: "noop"
    
    archival:
      history:
        state: "enabled"
        enableRead: true
        provider:
          filestore:
            fileMode: "0666"
            dirMode: "0766"
      visibility:
        state: "enabled"
        enableRead: true
        provider:
          filestore:
            fileMode: "0666"
            dirMode: "0766"
    
    namespaceDefaults:
      archival:
        history:
          state: "disabled"
          URI: "file:///tmp/temporal_archival/development"
        visibility:
          state: "disabled"
          URI: "file:///tmp/temporal_vis_archival/development"
    
    dynamicConfigClient:
      filepath: "/etc/temporal/config/dynamicconfig/development-mongodb.yaml"
EOF

# Deploy Temporal
kubectl apply -f k8s-temporal-deployment.yaml

echo ""
echo "Temporal deployment created!"
echo "Check the status with:"
echo "kubectl get pods -n rafay-core -l app=temporal-server"
echo ""
echo "View logs with:"
echo "kubectl logs -n rafay-core -l app=temporal-server -f" 