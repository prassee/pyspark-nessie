#!/bin/bash

# MongoDB Replica Set Initialization Script

echo "🔧 Initializing MongoDB Replica Set..."

# Wait for MongoDB instances to be ready
sleep 30

echo "📝 Configuring replica set..."

# Initialize replica set
mongosh --host mongo1:27017 -u admin -p admin123 --authenticationDatabase admin --eval "
try {
  rs.initiate({
    _id: 'rs0',
    members: [
      { _id: 0, host: 'mongo1:27017', priority: 2 },
      { _id: 1, host: 'mongo2:27018', priority: 1 },
      { _id: 2, host: 'mongo3:27019', priority: 1 }
    ]
  });
  print('✅ Replica set initialized successfully');
} catch (e) {
  print('⚠️ Replica set may already be initialized: ' + e);
}
"

# Wait for replica set to stabilize
sleep 10

echo "🔍 Checking replica set status..."
mongosh --host mongo1:27017 -u admin -p admin123 --authenticationDatabase admin --eval "
rs.status();
"

echo "📊 Creating sample collections and data..."
mongosh --host mongo1:27017 -u admin -p admin123 --authenticationDatabase admin --eval "
use sampledb;

// Create users collection with sample data
db.users.insertMany([
  { name: 'John Doe', email: 'john@example.com', age: 30, department: 'Engineering', created_at: new Date() },
  { name: 'Jane Smith', email: 'jane@example.com', age: 25, department: 'Marketing', created_at: new Date() },
  { name: 'Bob Johnson', email: 'bob@example.com', age: 35, department: 'Sales', created_at: new Date() }
]);

// Create products collection with sample data
db.products.insertMany([
  { name: 'Laptop', category: 'Electronics', price: 999.99, stock: 50, created_at: new Date() },
  { name: 'Phone', category: 'Electronics', price: 699.99, stock: 100, created_at: new Date() },
  { name: 'Desk', category: 'Furniture', price: 299.99, stock: 25, created_at: new Date() }
]);

// Create orders collection with sample data
db.orders.insertMany([
  { 
    user_id: ObjectId(), 
    items: [
      { product: 'Laptop', quantity: 1, price: 999.99 },
      { product: 'Phone', quantity: 2, price: 699.99 }
    ],
    total: 2399.97,
    status: 'completed',
    created_at: new Date()
  },
  { 
    user_id: ObjectId(), 
    items: [
      { product: 'Desk', quantity: 1, price: 299.99 }
    ],
    total: 299.99,
    status: 'pending',
    created_at: new Date()
  }
]);

print('✅ Sample data created successfully');
print('📊 Collections created:');
print('  - users: ' + db.users.countDocuments());
print('  - products: ' + db.products.countDocuments());
print('  - orders: ' + db.orders.countDocuments());
"

echo "✅ MongoDB replica set setup completed!"
