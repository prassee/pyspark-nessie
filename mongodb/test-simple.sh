#!/bin/bash

# Simplified MongoDB Replica Set Test Script (No Auth)

echo "🧪 Testing MongoDB Replica Set Connection..."

# Test connection to primary
echo "📍 Testing primary node (mongo1:27017)..."
mongosh --host localhost:27017 --eval "
  print('Connected to: ' + db.serverStatus().host);
  print('MongoDB version: ' + db.version());
  print('Connection OK ✅');
" --quiet

# Test replica set status
echo "🔍 Checking replica set status..."
mongosh --host localhost:27017 --eval "
  var status = rs.status();
  print('Replica Set: ' + status.set);
  print('Members: ' + status.members.length);
  status.members.forEach(function(member) {
    print('  ' + member.name + ' - ' + member.stateStr + ' (health: ' + member.health + ')');
  });
" --quiet

# Test sample data
echo "📊 Checking sample data..."
mongosh --host localhost:27017 --eval "
  use sampledb;
  print('Users count: ' + db.users.countDocuments());
  print('Orders count: ' + db.orders.countDocuments());
  print('Sample user: ' + JSON.stringify(db.users.findOne()));
" --quiet

echo "✅ All tests completed!"
