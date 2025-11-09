#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

console.log('🔄 Converting Parquet files to CSV for Evidence...');

// Find the latest run directory
const goldKpisPath = 'data/gold/kpis';
const evidenceDataPath = 'evidence/sources/data';

// Create evidence data directory
if (!fs.existsSync(evidenceDataPath)) {
  fs.mkdirSync(evidenceDataPath, { recursive: true });
}

try {
  // Find the latest run directory
  const runDirs = fs.readdirSync(goldKpisPath)
    .filter(dir => dir.startsWith('run_'))
    .sort()
    .reverse();
  
  if (runDirs.length === 0) {
    console.log('❌ No run directories found. Please run the Gold job first.');
    process.exit(1);
  }
  
  const latestRunDir = path.join(goldKpisPath, runDirs[0]);
  console.log(`📁 Using latest run: ${runDirs[0]}`);
  
  // List of datasets to convert
  const datasets = ['weekly_trip_volume', 'weekly_trips_revenue', 'time_vs_distance', 'kpi_summary'];
  
  for (const dataset of datasets) {
    const parquetPath = path.join(latestRunDir, dataset);
    const csvPath = path.join(evidenceDataPath, `${dataset}.csv`);
    
    if (fs.existsSync(parquetPath)) {
      console.log(`🔄 Converting ${dataset}...`);
      
      try {
        // Use Python with pandas to convert Parquet to CSV
        const pythonScript = `
import pandas as pd
import sys
import os

try:
    # Read all parquet files in the directory
    df = pd.read_parquet('${parquetPath}')
    # Write to CSV
    df.to_csv('${csvPath}', index=False)
    print(f"✅ Converted ${dataset}")
except Exception as e:
    print(f"❌ Error converting ${dataset}: {e}")
    sys.exit(1)
`;
        
        fs.writeFileSync('/tmp/convert_parquet.py', pythonScript);
        execSync('python3 /tmp/convert_parquet.py', { stdio: 'inherit' });
        
      } catch (error) {
        console.log(`⚠️  Python conversion failed for ${dataset}, trying alternative method...`);
        
        // Alternative: Use Spark if available
        try {
          const sparkScript = `
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ParquetToCSV").getOrCreate()
df = spark.read.parquet("${parquetPath}")
df.coalesce(1).write.mode("overwrite").option("header", "true").csv("/tmp/${dataset}_csv")
spark.stop()
`;
          
          fs.writeFileSync('/tmp/spark_convert.py', sparkScript);
          execSync('python3 /tmp/spark_convert.py', { stdio: 'inherit' });
          
          // Find the generated CSV file and copy it
          const tempCsvDir = `/tmp/${dataset}_csv`;
          if (fs.existsSync(tempCsvDir)) {
            const csvFiles = fs.readdirSync(tempCsvDir).filter(f => f.endsWith('.csv'));
            if (csvFiles.length > 0) {
              fs.copyFileSync(path.join(tempCsvDir, csvFiles[0]), csvPath);
              console.log(`✅ Converted ${dataset} using Spark`);
            }
          }
        } catch (sparkError) {
          console.log(`❌ Could not convert ${dataset}: ${sparkError.message}`);
        }
      }
    } else {
      console.log(`⚠️  ${dataset} parquet directory not found, skipping...`);
    }
  }
  
  // List converted files
  console.log('\n📊 Evidence data files:');
  const dataFiles = fs.readdirSync(evidenceDataPath);
  dataFiles.forEach(file => {
    const filePath = path.join(evidenceDataPath, file);
    const stats = fs.statSync(filePath);
    console.log(`  ✅ ${file} (${Math.round(stats.size / 1024)}KB)`);
  });
  
  console.log('\n🎉 Conversion complete! You can now run: npm run dev');
  
} catch (error) {
  console.error('❌ Error during conversion:', error.message);
  process.exit(1);
}
