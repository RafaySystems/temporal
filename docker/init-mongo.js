// MongoDB initialization script for Temporal
// This script creates the necessary databases and users for Temporal

print('Starting MongoDB initialization for Temporal...');

// Switch to admin database to create users
db = db.getSiblingDB('admin');

// Create temporal user if it doesn't exist
try {
  db.createUser({
    user: 'temporal',
    pwd: 'temporal123',
    roles: [
      { role: 'readWrite', db: 'temporal' },
      { role: 'readWrite', db: 'temporal_visibility' }
    ]
  });
  print('Created temporal user successfully');
} catch (error) {
  if (error.code === 51003) {
    print('Temporal user already exists');
  } else {
    print('Error creating temporal user:', error.message);
  }
}

// Switch to temporal database
db = db.getSiblingDB('temporal');

// Create collections for Temporal
const collections = [
  'executions',
  'tasks',
  'shards',
  'metadata',
  'cluster_metadata',
  'nexus_endpoints',
  'queues'
];

collections.forEach(collectionName => {
  try {
    db.createCollection(collectionName);
    print(`Created collection: ${collectionName}`);
  } catch (error) {
    if (error.code === 48) {
      print(`Collection ${collectionName} already exists`);
    } else {
      print(`Error creating collection ${collectionName}:`, error.message);
    }
  }
});

// Switch to temporal_visibility database
db = db.getSiblingDB('temporal_visibility');

// Create visibility collections
const visibilityCollections = [
  'visibility',
  'visibility_tasks'
];

visibilityCollections.forEach(collectionName => {
  try {
    db.createCollection(collectionName);
    print(`Created visibility collection: ${collectionName}`);
  } catch (error) {
    if (error.code === 48) {
      print(`Visibility collection ${collectionName} already exists`);
    } else {
      print(`Error creating visibility collection ${collectionName}:`, error.message);
    }
  }
});

print('MongoDB initialization completed successfully!');
print('Temporal databases and collections are ready.'); 