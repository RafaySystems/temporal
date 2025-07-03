const { Buffer } = require('buffer');

// The issue is that the cluster metadata document structure doesn't match what Temporal expects
// Let's create a minimal cluster metadata that matches the config

console.log("MongoDB Console Commands:");
console.log("========================");
console.log();
console.log("// First, delete the existing document:");
console.log("use temporal;");
console.log("db.cluster_metadata_info.deleteOne({");
console.log("  metadata_partition: 0,");
console.log("  cluster_name: \"active\"");
console.log("});");
console.log();
console.log("// Now let Temporal auto-populate the cluster metadata");
console.log("// by starting it without any existing cluster metadata document");
console.log();
console.log("// The cluster metadata should be created automatically by Temporal");
console.log("// based on the configuration in config/docker.yaml");
console.log();
console.log("// If you want to verify after Temporal starts:");
console.log("db.cluster_metadata_info.findOne({metadata_partition: 0, cluster_name: \"active\"});"); 