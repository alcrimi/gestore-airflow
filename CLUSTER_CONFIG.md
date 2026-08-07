# DataProc Cluster Configuration Reference

**Data di creazione**: 2026-07-17  
**Cluster**: `dproc21-test-edlgcp`  
**Progetto**: `tim-cdc-prd-cu00001873p7-l30`

---

## 📍 Configurazione DataProc

### Infrastruttura

| Parametro | Valore |
|-----------|--------|
| **Region** | `europe-west8` |
| **Zone** | `europe-west8-c` |
| **Image Version** | `2.1.114-debian11` |
| **Autoscaling** | Enabled (`autoscaling_dataproc_test`) |
| **Staging Bucket** | `dataproc-staging-europe-west8-560671659573-t3rdebej` |
| **Subnetwork** | `projects/tim-net-prd-dc1b01p29-l0/regions/europe-west8/subnetworks/dc1-bc-ita-10-56-68-0-26` |
| **Internal IP Only** | Yes |

### Node Configuration

**Master Node:**
- Machine Type: `n2-standard-8` (8 vCPU, 32GB RAM)
- Primary Disk: pd-ssd 500GB
- Count: 1

**Worker Nodes:**
- Machine Type: `n2-standard-8` (8 vCPU, 32GB RAM)
- Primary Disk: pd-ssd 500GB
- Count: **48**
- Secondary Workers: 0

**Cluster Summary:**
- Total vCPU: 384 (48 workers × 8 + 1 master × 8)
- Total RAM: ~1.5TB
- Effective YARN Resources per Worker: 28,288 MB RAM, 8 vCPU

### Security

- Secure Boot: Enabled
- vTPM: Enabled
- Integrity Monitoring: Enabled
- Confidential Computing: Disabled

---

**Ultimo Aggiornamento**: 2026-07-17  
**Contesto**: Ottimizzazione ETL BigQuery → Spark → HDFS su DataProc europe-west8
