#!/bin/bash

# Build script for Scala Spark project

set -e

PROJECT_DIR="scala-spark"
TARGET_JAR="target/scala-2.12/spark-iceberg-nessie-scala-assembly-0.1.0-SNAPSHOT.jar"

echo "🏗️ Building Scala Spark project..."

cd $PROJECT_DIR

# Check if SBT is available
if ! command -v sbt &> /dev/null; then
    echo "❌ SBT not found. Installing..."
    # Install SBT using coursier
    if ! command -v cs &> /dev/null; then
        echo "Installing Coursier..."
        curl -fL https://github.com/coursier/launchers/raw/master/cs-x86_64-pc-linux.gz | gzip -d > cs
        chmod +x cs
        sudo mv cs /usr/local/bin/
    fi
    cs install sbt
    export PATH="$PATH:~/.local/share/coursier/bin"
fi

echo "📦 Compiling Scala code..."
sbt compile

echo "🧪 Running tests..."
sbt test

echo "📋 Creating assembly JAR..."
sbt assembly

if [ -f "$TARGET_JAR" ]; then
    echo "✅ Build successful! JAR created at: $TARGET_JAR"
    echo "📊 JAR size: $(du -h $TARGET_JAR | cut -f1)"
else
    echo "❌ Build failed - JAR not found"
    exit 1
fi

echo "🚀 Ready to submit to Spark cluster!"
echo "Run: make scala-demo"

cd ..
