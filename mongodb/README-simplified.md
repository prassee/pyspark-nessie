# MongoDB Replica Set - Simplified for Local Development

This MongoDB setup provides a 3-node replica set optimized for local development with simplified configuration.

## Architecture

- **mongo1**: Primary node (port 27017)
- **mongo2**: Secondary node (port 27018) 
- **mongo3**: Secondary node (port 27019)
- **mongo-setup**: Initialization container (runs once)
- **mongo-express**: Web UI (port 8082)

## Key Simplifications

✅ **No keyfile authentication** - Simplified for local development
✅ **Embedded initialization** - No external scripts needed
✅ **Faster health checks** - Reduced intervals for quicker startup
✅ **Minimal volumes** - Only data directories, no config volumes
✅ **Streamlined commands** - Direct mongod commands instead of bash scripts

## Quick Start

```bash
# Start the MongoDB replica set
docker-compose -f docker-compose-olake.yaml up -d mongo1 mongo2 mongo3 mongo-setup

# Check status
docker-compose -f docker-compose-olake.yaml logs mongo-setup

# Start MongoDB Express (optional)
docker-compose -f docker-compose-olake.yaml up -d mongo-express
```

## Connection Details

### MongoDB Replica Set
- **Connection String**: `mongodb://admin:admin123@localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0&authSource=admin`
- **Username**: `admin`
- **Password**: `admin123`
- **Database**: `admin` (authentication) or `sampledb` (sample data)

### MongoDB Express Web UI
- **URL**: http://localhost:8082
- **Username**: `admin`
- **Password**: `admin123`

## Sample Data

The setup automatically creates a `sampledb` database with sample collections:

```javascript
// Users collection
db.users.find()
// Orders collection  
db.orders.find()
```

## Development Commands

```bash
# Connect to primary node
mongosh "mongodb://admin:admin123@localhost:27017/?authSource=admin"

# Check replica set status
rs.status()

# List databases
show dbs

# Use sample database
use sampledb
show collections
```

## Troubleshooting

### Check container health
```bash
docker-compose -f docker-compose-olake.yaml ps
```

### View logs
```bash
docker-compose -f docker-compose-olake.yaml logs mongo1
docker-compose -f docker-compose-olake.yaml logs mongo-setup
```

### Reset replica set
```bash
docker-compose -f docker-compose-olake.yaml down -v
docker-compose -f docker-compose-olake.yaml up -d
```

## Production Considerations

⚠️ **This setup is optimized for local development only**

For production, consider:
- Adding keyfile authentication
- Implementing proper security settings
- Using persistent storage with backup strategies
- Configuring monitoring and alerting
- Setting up proper network security
