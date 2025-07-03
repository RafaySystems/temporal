const { Buffer } = require('buffer');

// This is the base64-encoded protobuf data for a basic cluster metadata
// ClusterName: "active"
// HistoryShardCount: 4
// ClusterId: "temporal-cluster-1"
// ClusterAddress: "127.0.0.1:7233"
// HttpAddress: "127.0.0.1:7243"
// FailoverVersionIncrement: 10
// InitialFailoverVersion: 1
// IsGlobalNamespaceEnabled: false
// IsConnectionEnabled: true

const base64Data = "CgZhY3RpdmUQBBoVdGVtcG9yYWwtY2x1c3Rlci0xKggxMjcuMC4wLjEQBDIICgExLjAiB2FjdGl2ZQ==";

console.log("MongoDB Console Command:");
console.log("========================");
console.log();
console.log("use temporal;");
console.log();
console.log("db.cluster_metadata_info.insertOne({");
console.log("  metadata_partition: 0,");
console.log("  cluster_name: \"active\",");
console.log("  data: BinData(0, \"" + base64Data + "\"),");
console.log("  data_encoding: \"proto3\",");
console.log("  version: 1");
console.log("});");
console.log();
console.log("Verification command:");
console.log("db.cluster_metadata_info.findOne({metadata_partition: 0, cluster_name: \"active\"});"); 