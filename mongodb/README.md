# MongoDB Replica Set Configuration

This directory contains the configuration and scripts for a MongoDB replica set with 3 nodes.

## 🏗️ Architecture

```
MongoDB Replica Set (rs0)
├── mongo1:27017 (Primary, Priority: 2)
├── mongo2:27018 (Secondary, Priority: 1)
└── mongo3:27019 (Secondary, Priority: 1)
```

## 🚀 Services

### **MongoDB Nodes:**
- **mongo1**: Primary node on port 27017
- **mongo2**: Secondary node on port 27018  
- **mongo3**: Secondary node on port 27019
- **mongo-setup**: Initialization container (runs once)
- **mongo-express**: Web UI on port 8082

## 🔐 Authentication

- **Admin Username**: `admin`
- **Admin Password**: `admin123`
- **Keyfile**: Shared authentication between replica set members

## 📊 Sample Data

The setup automatically creates sample collections:

### **Users Collection:**
```javascript
{
  name: "John Doe",
  email: "john@example.com", 
  age: 30,
  department: "Engineering",
  created_at: ISODate()
}
```

### **Products Collection:**
```javascript
{
  name: "Laptop",
  category: "Electronics",
  price: 999.99,
  stock: 50,
  created_at: ISODate()
}
```

### **Orders Collection:**
```javascript
{
  user_id: ObjectId(),
  items: [
    { product: "Laptop", quantity: 1, price: 999.99 }
  ],
  total: 999.99,
  status: "completed",
  created_at: ISODate()
}
```

## 🛠️ Usage

### **Start MongoDB Replica Set:**
```bash
docker-compose -f docker-compose-olake.yaml up -d mongo1 mongo2 mongo3 mongo-setup
```

### **Test Connection:**
```bash
./mongodb/test-connection.sh
```

### **Connect via Command Line:**
```bash
# Connect to primary
docker exec -it mongo1 mongosh -u admin -p admin123 --authenticationDatabase admin

# Connect with replica set URI
mongosh "mongodb://admin:admin123@localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0&authSource=admin"
```

### **Access Web UI:**
- **URL**: http://localhost:8082
- **Username**: admin
- **Password**: admin123

## 🔍 Monitoring

### **Check Replica Set Status:**
```javascript
rs.status()
rs.conf()
```

### **Check Node Health:**
```javascript
db.adminCommand("replSetGetStatus")
```

### **View Oplog:**
```javascript
use local
db.oplog.rs.find().sort({ts: -1}).limit(5)
```

## 🔧 Configuration

### **Replica Set Configuration:**
```javascript
{
  _id: "rs0",
  members: [
    { _id: 0, host: "mongo1:27017", priority: 2 },
    { _id: 1, host: "mongo2:27018", priority: 1 }, 
    { _id: 2, host: "mongo3:27019", priority: 1 }
  ]
}
```

### **Connection Strings:**

**Internal (Docker):**
```
mongodb://admin:admin123@mongo1:27017,mongo2:27018,mongo3:27019/?replicaSet=rs0&authSource=admin
```

**External (Host):**
```
mongodb://admin:admin123@localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0&authSource=admin
```

## 🔄 Failover Testing

### **Stop Primary Node:**
```bash
docker stop mongo1
```

### **Check New Primary:**
```bash
docker exec mongo2 mongosh -u admin -p admin123 --authenticationDatabase admin --eval "rs.status()"
```

### **Restart Node:**
```bash
docker start mongo1
```

## 📝 Scripts

- **init-replica-set.sh**: Initializes replica set and creates sample data
- **test-connection.sh**: Tests connectivity and replica set status
- **keyfile**: Shared authentication key for replica set members

## 🧹 Maintenance

### **Backup Database:**
```bash
docker exec mongo1 mongodump --host mongo1:27017 -u admin -p admin123 --authenticationDatabase admin --out /backup
```

### **View Logs:**
```bash
docker logs mongo1
docker logs mongo2  
docker logs mongo3
```

### **Clean Restart:**
```bash
docker-compose -f docker-compose-olake.yaml down
docker volume rm $(docker volume ls -q | grep mongo)
docker-compose -f docker-compose-olake.yaml up -d mongo1 mongo2 mongo3 mongo-setup
```

## 🚀 Integration

This MongoDB replica set is designed to work with:
- **Change Data Capture (CDC)** for real-time streaming
- **Spark MongoDB Connector** for analytics
- **Application connections** with high availability
- **Data replication** for disaster recovery

The replica set provides:
- ✅ **High Availability** with automatic failover
- ✅ **Read Scaling** across secondary nodes
- ✅ **Data Durability** with replication
- ✅ **Consistency** with write concerns
