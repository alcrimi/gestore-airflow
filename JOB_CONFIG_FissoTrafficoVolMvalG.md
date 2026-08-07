## 🔧 Configurazione Hadoop/YARN

```json
{
  "yarn:yarn.nodemanager.resource.memory-mb": 28288,
  "yarn:yarn.nodemanager.resource.cpu-vcores": 8,
  "yarn:yarn.resourcemanager.nodemanager-graceful-decommission-timeout-secs": 86400,
  "yarn:yarn.resourcemanager.decommissioning-nodes-watcher.decommission-if-no-shuffle-data": true,
  "yarn:yarn.scheduler.maximum-allocation-mb": 28288,
  "yarn:yarn.scheduler.minimum-allocation-mb": 1,
  "yarn-env:YARN_NODEMANAGER_HEAPSIZE": 3276,
  "yarn-env:YARN_RESOURCEMANAGER_HEAPSIZE": 4000,
  "yarn-env:YARN_TIMELINESERVER_HEAPSIZE": 4000,
  
  "hdfs:dfs.namenode.handler.count": 120,
  "hdfs:dfs.namenode.service.handler.count": 60,
  "hadoop-env:HADOOP_DATANODE_OPTS": "-Xmx512m",
  
  "mapred:mapreduce.job.maps": 837,
  "mapred:mapreduce.job.reduces": 279,
  "mapred:mapreduce.map.memory.mb": 3536,
  "mapred:mapreduce.reduce.memory.mb": 3536,
  "mapred:mapreduce.map.java.opts": "-Xmx2828m",
  "mapred:mapreduce.reduce.java.opts": "-Xmx2828m",
  "mapred:mapreduce.task.io.sort.mb": 256,
  "mapred:mapreduce.map.maxattempts": 10,
  "mapred:mapreduce.reduce.maxattempts": 10,
  
  "capacity-scheduler:yarn.scheduler.capacity.root.default.ordering-policy": "fair"
}
```

---

## ⚡ Configurazione Spark Ottimizzata

**File**: `spark_config.json` (configurazione per job ETL con BigQuery)

```json
{
  "spark.driver.memory": "12G",
  "spark.executor.cores": "3",
  "spark.executor.memory": "13G",
  "spark.executor.memoryOverhead": "1400m",

  "spark.sql.broadcastTimeout": "1200",

  "hive.exec.dynamic.partition": "true",
  "hive.exec.dynamic.partition.mode": "nonstrict",

  "spark.driver.extraClassPath": "cus-gen-cobu.jar",
  "spark.driver.extraJavaOptions": "-Dconfig.file=application.conf",
  "spark.executor.extraJavaOptions": "-Dconfig.file=application.conf",

  "spark.sql.shuffle.partitions": "1440",

  "spark.dynamicAllocation.maxExecutors": "96",
  "spark.dynamicAllocation.minExecutors": "20",
  "spark.dynamicAllocation.executorAllocationRatio": "0.8",
  "spark.dynamicAllocation.shuffleTracking.enabled": "true",
  "spark.dynamicAllocation.shuffleTracking.timeout": "600s",
  "spark.dynamicAllocation.cachedExecutorIdleTimeout": "600s",
  "spark.dynamicAllocation.schedulerBacklogTimeout": "1s",
  "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout": "5s",

  "spark.shuffle.registration.timeout": "15000",
  "spark.shuffle.registration.maxAttempts": "5",
  "spark.shuffle.io.retryWait": "30s",
  "spark.shuffle.io.maxRetries": "10",

  "spark.task.maxFailures": "8",
  "spark.stage.maxConsecutiveAttempts": "8",

  "spark.network.timeout": "600s",
  "spark.rpc.askTimeout": "600s",
  "spark.rpc.lookupTimeout": "600s",
  "spark.executor.heartbeatInterval": "30s",

  "spark.sql.adaptive.enabled": "true",
  "spark.sql.adaptive.coalescePartitions.enabled": "true",
  "spark.sql.adaptive.coalescePartitions.minPartitionSize": "64MB",
  "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
  "spark.sql.adaptive.skewJoin.enabled": "true",
  "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
  "spark.sql.adaptive.localShuffleReader.enabled": "true",

  "spark.sql.autoBroadcastJoinThreshold": "256MB",

  "spark.memory.fraction": "0.8",
  "spark.memory.storageFraction": "0.2",

  "spark.sql.files.maxPartitionBytes": "134217728",
  "spark.sql.files.openCostInBytes": "134217728",

  "spark.extraListeners": ""
}
```

### Parametri Critici Spiegati

| Parametro | Valore | Razionale |
|-----------|--------|-----------|
| `spark.executor.cores` | 3 | 2 executor per nodo (8 vCPU / 3 core = ~2.67) → massimizza parallelismo |
| `spark.executor.memory` | 13G | Calibrato su ~27.6GB YARN per nodo / 2 executor |
| `spark.executor.memoryOverhead` | 1400m | ~10% dell'executor memory (best practice) |
| `spark.sql.shuffle.partitions` | 1440 | 96 executor × 3 core × 5 tasks/core |
| `spark.dynamicAllocation.minExecutors` | 20 | Evita cold start dopo preemption |
| `spark.sql.adaptive.enabled` | true | Conversione automatica SMJ → Broadcast Join, gestione skew |
| `spark.sql.autoBroadcastJoinThreshold` | 256MB | Permette broadcast di tabelle fino a 256MB |

---

## 🐛 Problemi Identificati e Soluzioni

### Problema 1: FetchFailedException durante Shuffle

**Sintomo**: 
```
Executor is not registered (appId=..., execId=95)
org.apache.spark.shuffle.FetchFailedException
```

**Root Cause**: Executor deallocato da Dynamic Allocation mentre altri executor tentano di leggere shuffle data.

**Soluzione**:
- ✅ `spark.dynamicAllocation.shuffleTracking.enabled=true` → non dealloca executor con shuffle data attivo
- ✅ `spark.dynamicAllocation.executorAllocationRatio=0.8` → riduce oscillazioni aggressive

### Problema 2: Preemption YARN da cluster autoscaling

**Sintomo**:
```
YARN event (e.g., preemption) ... Executor for container exited
java.net.UnknownHostException: dproc21-test-edlgcp-w-23...
```

**Root Cause**: DataProc scala down worker node mentre job in esecuzione; nodo rimosso → hostname non risolvibile.

**Soluzione**:
- ✅ `spark.task.maxFailures=8` → retry aggressivi
- ✅ `spark.stage.maxConsecutiveAttempts=8` → retry stage intero
- ✅ `spark.network.timeout=600s` → tolleranza timeout prolungati
- ✅ `spark.shuffle.io.maxRetries=10` → retry fetch aggresivi
- ✅ `yarn.resourcemanager.nodemanager-graceful-decommission-timeout-secs=86400` → 24h timeout per graceful decommission

### Problema 3: Executor Under-utilization (cores=5)

**Sintomo**: Solo 1 executor per nodo con 5 cores su n2-standard-8 → 3 vCPU sempre idle.

**Root Cause**: Dimensionamento senza considerare vCPU totali del nodo (8).

**Soluzione**:
- ✅ `spark.executor.cores=3` → 2-3 executor per nodo → 96 executor totali (+100% parallelismo)

---

## 📊 Performance Tuning

### Dati di Input

- **Sorgente**: BigQuery (BQ Storage API)
- **Volume**: ~180GB
- **Tempo baseline**: ~60 minuti

### Impatto Stimato Ottimizzazioni

| Intervento | Impatto Stimato |
|-----------|-----------------|
| Executor rightsizing (cores 5→3) | −20/25% |
| AQE + skew join | −15/30% |
| `autoBroadcastJoinThreshold` 10MB→256MB | −30/50% (se join broadcastable) |
| BigQuery parallelism tuning | −15/20% |
| **Totale Stimato** | **da 60 min → 25/35 min** |

### BigQuery Storage API Optimization

Se usi `spark-bigquery-connector`, aggiungi:

```json
{
  "spark.datasource.bigquery.viewsEnabled": "true",
  "spark.datasource.bigquery.parallelism": "192"
}
```

`parallelism=192` apre 192 stream paralleli verso BQ Storage API (96 executor × 2 task concorrenti).

---

## 🔄 Monitoraggio e Troubleshooting

### Spark UI Dashboard

Verifica in `http://<master-node>:4040`:
- **Stage Analysis**: Identificare stage lenti (sort, join, shuffle)
- **Executor Tab**: Verificare utilizzo CPU, memoria, shuffle reads/writes
- **SQL Tab**: Analizzare query plan e identificare join types (BroadcastHashJoin vs SortMergeJoin)

### Log Principali

```bash
# HDFS NameNode
/var/log/hadoop/hdfs/namenode.log

# YARN ResourceManager
/var/log/hadoop/yarn/resourcemanager.log

# Spark Driver
/usr/local/spark/logs/

# Dataproc Cluster Logs
gsutil ls gs://dataproc-staging-europe-west8-560671659573-t3rdebej/google-cloud-dataproc-metainfo/*/
```
