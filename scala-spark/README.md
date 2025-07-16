# Spark Scala + Iceberg + Nessie Demo

This is a Scala SBT project equivalent to the PySpark demo, showcasing Apache Spark with Iceberg tables and Nessie catalog integration.

## 🏗️ Project Structure

```
scala-spark/
├── build.sbt                          # SBT build configuration
├── project/
│   ├── build.properties              # SBT version
│   └── plugins.sbt                   # SBT plugins
├── src/
│   ├── main/
│   │   ├── scala/
│   │   │   └── IcebergNessieDemo.scala # Main demo application
│   │   └── resources/
│   │       └── logback.xml            # Logging configuration
│   └── test/
│       └── scala/
│           └── IcebergNessieDemoTest.scala # Unit tests
└── README.md                          # This file
```

## 🚀 Features Demonstrated

- ✅ **Spark Session Configuration** with Iceberg extensions
- ✅ **Iceberg Table Creation** and operations
- ✅ **Nessie Catalog Integration** for data versioning
- ✅ **Schema Evolution** examples
- ✅ **Time Travel Queries** for historical data
- ✅ **Advanced Analytics** with window functions
- ✅ **Functional Programming** patterns in Scala
- ✅ **Unit Testing** with ScalaTest

## 📋 Prerequisites

- **Scala 2.12.18**
- **SBT 1.9.7**
- **Java 8 or 11**
- **Docker environment** running (Spark, Nessie, Minio)

## 🛠️ Build and Run

### 1. **Compile the project:**
```bash
cd scala-spark
sbt compile
```

### 2. **Run tests:**
```bash
sbt test
```

### 3. **Create fat JAR:**
```bash
sbt assembly
```

### 4. **Run locally (for testing):**
```bash
sbt run
```

### 5. **Submit to Spark cluster:**
```bash
# From the main project directory
docker exec -it spark-master spark-submit \
  --class com.example.iceberg.IcebergNessieDemo \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars-custom/*.jar \
  /opt/spark/jobs/scala-spark/target/scala-2.12/spark-iceberg-nessie-scala-assembly-0.1.0-SNAPSHOT.jar
```

## 🔧 Development

### **Interactive Scala REPL:**
```bash
sbt console
```

### **Continuous compilation:**
```bash
sbt ~compile
```

### **Code formatting:**
```bash
sbt scalafmt
```

## 📊 Key Differences from PySpark

### **Type Safety:**
```scala
// Scala - Compile-time type checking
val df: DataFrame = employeesData.toDF("id", "name", "age", "salary", "department")
val avgSalary: Double = df.agg(avg("salary")).collect()(0).getDouble(0)
```

### **Functional Programming:**
```scala
// Scala - Functional operations
val engineeringEmployees = employeesDF
  .filter(col("department") === "Engineering")
  .orderBy(desc("salary"))
```

### **Pattern Matching:**
```scala
// Scala - Pattern matching for error handling
result match {
  case Success(dataFrame) => processData(dataFrame)
  case Failure(exception) => handleError(exception)
}
```

### **Implicits and DSL:**
```scala
// Scala - Rich DSL with implicits
import spark.implicits._
$"salary" > 75000  // Column expressions
```

## 🐳 Docker Integration

### **Mount Scala project:**
Update `docker-compose.yml` to mount the Scala project:

```yaml
volumes:
  - ./src:/opt/spark/jobs
  - ./scala-spark:/opt/spark/jobs/scala-spark  # Add this line
```

### **Build in container:**
```bash
# Access Spark master container
docker exec -it spark-master bash

# Navigate to Scala project
cd /opt/spark/jobs/scala-spark

# Install SBT (if not available)
curl -fL https://github.com/coursier/launchers/raw/master/cs-x86_64-pc-linux.gz | gzip -d > cs
chmod +x cs
./cs install sbt

# Build the project
sbt assembly
```

## 🧪 Testing Strategy

### **Unit Tests:**
- **Local Spark testing** with in-memory catalog
- **DataFrame operations** validation
- **Business logic** verification

### **Integration Tests:**
- **End-to-end workflows** with Docker environment
- **Iceberg table operations** testing
- **Nessie catalog** integration testing

## 📈 Performance Considerations

### **Spark Configuration:**
```scala
// Optimized for Iceberg
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
.config("spark.sql.adaptive.skewJoin.enabled", "true")
```

### **Memory Management:**
```scala
// Efficient memory usage
.config("spark.executor.memory", "2g")
.config("spark.executor.memoryFraction", "0.8")
.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

## 🔍 Monitoring and Debugging

### **Logging Configuration:**
- **Structured logging** with Logback
- **Different log levels** for components
- **Console and file** output options

### **Spark UI Access:**
- **Application monitoring**: http://localhost:8080
- **Job tracking** and performance metrics
- **Stage and task** level analysis

## 🚀 Next Steps

1. **Add more complex transformations** and business logic
2. **Implement data quality checks** and validations
3. **Create streaming applications** with Structured Streaming
4. **Add metrics and monitoring** with custom listeners
5. **Implement CI/CD pipelines** for automated testing and deployment

## 🤝 Comparison with PySpark

| Feature | PySpark | Spark Scala |
|---------|---------|-------------|
| **Type Safety** | Runtime | Compile-time |
| **Performance** | Good | Excellent |
| **Development Speed** | Fast | Medium |
| **Ecosystem** | Rich (Python) | Rich (JVM) |
| **Learning Curve** | Easy | Medium |
| **Debugging** | Good | Excellent |

The Scala implementation provides better performance and type safety, while the PySpark version offers easier development and a more accessible syntax for data scientists.
