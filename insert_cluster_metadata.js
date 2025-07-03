// MongoDB script to insert cluster metadata
// Run this in MongoDB shell or MongoDB Compass

use temporal;

// Create cluster metadata document
// The data field contains a protobuf-serialized ClusterMetadata message
// This is a sample cluster metadata for the "active" cluster

db.cluster_metadata_info.insertOne({
    metadata_partition: 0,
    cluster_name: "active",
    data: BinData(0, "CgZhY3RpdmUQBBoVdGVtcG9yYWwtY2x1c3Rlci0xKggxMjcuMC4wLjEQBDIICgExLjAiB2FjdGl2ZQ=="),
    data_encoding: "proto3",
    version: 1
});

print("Cluster metadata inserted successfully!");
print("Collection now contains:");
db.cluster_metadata_info.find().pretty(); 