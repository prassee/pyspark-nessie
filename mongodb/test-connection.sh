#!/bin/bash

# MongoDB Connection Test Script

echo "🔍 Testing MongoDB Replica Set Connection..."

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 5

# Test connection to primary
echo "📡 Testing connection to mongo1 (primary)..."
docker exec mongo1 mongosh --eval "
db.adminCommand('ping');
print('✅ mongo1 connection successful');
" 2>/dev/null || echo "❌ mongo1 connection failed"

# Test connection to secondary nodes
echo "📡 Testing connection to mongo2..."
docker exec mongo2 mongosh --eval "
db.adminCommand('ping');
print('✅ mongo2 connection successful');
" 2>/dev/null || echo "❌ mongo2 connection failed"

echo "📡 Testing connection to mongo3..."
docker exec mongo3 mongosh --eval "
db.adminCommand('ping');
print('✅ mongo3 connection successful');
" 2>/dev/null || echo "❌ mongo3 connection failed"

# Test replica set status
echo "🔍 Checking replica set status..."
docker exec mongo1 mongosh -u admin -p admin123 --authenticationDatabase admin --eval "
try {
  var status = rs.status();
  print('✅ Replica Set Status: ' + status.ok);
  print('📊 Replica Set Members:');
  status.members.forEach(function(member) {
    print('  - ' + member.name + ' (' + member.stateStr + ')');
  });
} catch (e) {
  print('❌ Replica set not initialized: ' + e);
}
" 2>/dev/null

# Test MongoDB Express
echo "🌐 Testing MongoDB Express..."
curl -s -o /dev/null -w "%{http_code}" http://localhost:8082 | grep -q "200" && echo "✅ MongoDB Express accessible" || echo "❌ MongoDB Express not accessible"

# Test sample data
echo "📊 Checking sample data..."
docker exec mongo1 mongosh -u admin -p admin123 --authenticationDatabase admin --eval "
use sampledb;
print('📈 Sample Data Counts:');
print('  Users: ' + db.users.countDocuments());
print('  Products: ' + db.products.countDocuments());
print('  Orders: ' + db.orders.countDocuments());
" 2>/dev/null

echo "🎉 MongoDB connection test completed!"
