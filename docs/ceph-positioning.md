# Ceph Positioning for Data Lake

This document explains why the project uses Ceph RGW as the main Data Lake
storage layer, how it differs from the local MinIO baseline, and how to present
the fault-tolerance and benchmark results.

## Core Message

Ceph is not used in this project only as another S3-compatible endpoint. It is
used to represent a distributed, on-premise object storage layer for a Data Lake.

The project uses MinIO as a local development and benchmark baseline. Ceph RGW
is the target storage platform because it provides distributed storage,
replication, fault tolerance, recovery, and S3-compatible access for processing
engines such as Spark and Trino.

Short presentation version:

> MinIO is useful as a lightweight local S3-compatible baseline. Ceph is used
> when the Data Lake needs distributed storage across multiple nodes, replicated
> data, fault tolerance, recovery, and an on-premise object storage platform for
> Spark, Trino, and other analytics workloads.

## Why Ceph for a Data Lake

In a modern Data Lake, object storage is the central storage layer. It stores:

- raw/bronze data;
- processed/silver data;
- curated/gold analytics data;
- intermediate artifacts and system objects.

Compute engines should be replaceable. Spark, Trino, Airflow, notebooks, and
ML jobs can all access the same data through an S3-compatible API. Ceph RGW
fits this model because it exposes object storage through S3 while storing data
on a distributed Ceph cluster.

The important Ceph strengths for this project are:

| Strength | Meaning in this project |
|---|---|
| Distributed storage | Data is stored across multiple Ceph OSDs/nodes instead of one local service. |
| Replication | Objects have redundant placement across the cluster. |
| Fault tolerance | The cluster can keep serving data when one storage node is unavailable. |
| Recovery | When the failed node returns, Ceph can recover back to `HEALTH_OK`. |
| S3-compatible API | Spark, Trino, and Python clients can use the same object API. |
| On-premise fit | Ceph can be deployed in a private lab, private cloud, or enterprise data center. |
| Unified platform | Ceph can also provide block and file storage in larger deployments. |

## When Ceph Makes Sense

Ceph is a good fit when the environment needs:

- an on-premise or private-cloud Data Lake;
- storage across multiple physical or virtual nodes;
- tolerance for disk or node failure;
- a single storage platform that can grow over time;
- S3-compatible object storage for analytics and AI/ML workloads;
- operational control instead of depending entirely on public cloud object
  storage.

Ceph is less attractive when the goal is only:

- a very small local development setup;
- a simple single-node object store;
- a lightweight demo where distributed storage is not required;
- the lowest possible operational complexity.

## Ceph vs MinIO in This Project

The project does not claim that Ceph is always faster than MinIO. The two
systems are used for different roles.

| Dimension | MinIO local baseline | Ceph RGW lab |
|---|---|---|
| Project role | Local/dev S3 baseline | Main distributed Data Lake storage |
| Deployment | Docker Compose on one machine | Three VirtualBox Ubuntu VMs |
| Storage model | Lightweight object storage for local validation | Ceph RGW backed by replicated Ceph OSDs |
| Fault-tolerance demo | Not the focus of the local baseline | Central part of the Ceph validation |
| Query path | Spark/Trino can read through S3-compatible API | Spark/Trino can read through S3-compatible API |
| Benchmark purpose | Local performance baseline | Ceph integration and lab performance baseline |
| Best use in report | Baseline comparison | Main storage architecture and fault-tolerance evidence |

MinIO is still useful. It gives a fast local path for development and repeatable
baseline testing. Ceph is the storage architecture being studied and validated.

Presentation version:

> MinIO helps validate the pipeline quickly on a local machine. Ceph is the
> storage layer that demonstrates the distributed Data Lake architecture:
> multiple nodes, replicated storage, degraded operation, and recovery.

## Demonstrated Ceph Value

The strongest project evidence is not that Ceph beats MinIO in a laptop
benchmark. The strongest evidence is that the same Data Lake pipeline can use a
real distributed object storage cluster and continue serving data during a
controlled node outage.

Validated evidence:

| Capability | Evidence |
|---|---|
| S3 compatibility | `make health` and `make storage-smoke` passed against RGW. |
| Data Lake layout | Bronze, silver, gold, and system buckets were created on Ceph. |
| ETL processing | Spark wrote silver and gold Parquet outputs to Ceph. |
| Query processing | Spark SQL and Trino queried Ceph-backed data. |
| Fault tolerance | After `hadoop-worker2` was shut down, Ceph stayed in quorum and Trino still queried gold data. |
| Recovery | After `hadoop-worker2` returned, Ceph recovered to `HEALTH_OK`. |

## Fault-Tolerance Narrative

The recommended demo narrative is:

```text
1. Start with Ceph HEALTH_OK.
2. Query gold data through Trino.
3. Shut down hadoop-worker2.
4. Show Ceph HEALTH_WARN:
   - 1/3 MONs down
   - quorum remains hadoop-master,hadoop-worker1
   - osd.2 down
   - data redundancy degraded
5. Run make trino-smoke again.
6. Show that Trino still returns gold data from Ceph RGW.
7. Run make health and make storage-smoke.
8. Restart hadoop-worker2.
9. Show Ceph recovery to HEALTH_OK.
```

What this proves:

- Ceph did not remain `HEALTH_OK`; it correctly reported a degraded state.
- The cluster did not become unavailable; it kept quorum with two MONs.
- RGW continued serving S3 requests through `hadoop-master`.
- Trino could still query curated gold data from Ceph while one storage node
  was unavailable.
- After the node returned, Ceph restored the healthy state.

This is a practical Data Lake story: the storage layer can degrade safely while
analytics users continue to read curated data.

## Benchmark Interpretation

The current benchmark should be presented as a lab baseline, not as a universal
Ceph-vs-MinIO performance conclusion.

In the current laptop lab, MinIO is faster because it runs locally through
Docker, while Ceph RGW uses:

- VirtualBox networking;
- an RGW service;
- replicated Ceph pools;
- three small virtual OSD disks;
- limited host RAM and CPU.

The correct interpretation is:

> The benchmark proves that both backends are S3-compatible with the same
> benchmark runner. MinIO is faster in this constrained local lab, while Ceph
> adds distributed storage, replication, fault tolerance, and recovery. The
> benchmark is a baseline for this lab, not a production ranking.

Avoid saying:

```text
Ceph is faster than MinIO.
```

Prefer saying:

```text
Ceph is more suitable when the storage requirement is distributed, replicated,
fault-tolerant, and on-premise. MinIO is simpler and faster for this local
single-machine baseline.
```

## Likely Questions and Answers

### Why use Ceph if MinIO is faster in the benchmark?

Because the goal is not only raw local throughput. The project studies a
distributed Data Lake storage layer. Ceph provides replication, node-level
fault tolerance, recovery, and a multi-node architecture. MinIO is used as a
local baseline.

### In what situation would Ceph be chosen?

Ceph is chosen for on-premise or private-cloud environments where data must be
stored across multiple nodes, remain available during failures, and be accessed
through S3-compatible APIs by analytics engines.

### What did the node-failure demo prove?

It proved that when one Ceph worker node was shut down, the cluster retained
quorum, reported degraded redundancy, continued serving S3 requests, and still
allowed Trino to query curated gold data.

### What did the node-failure demo not prove?

It did not prove full production high availability. RGW currently runs as one
daemon on `hadoop-master`; if that host is down, the S3 endpoint would be
unavailable unless RGW is deployed redundantly behind a load balancer.

### Why query with Trino instead of Spark SQL during the failure demo?

Trino is lighter for this lab and directly represents the read-side analytics
use case. It only needs the Trino service to query the gold Parquet data through
RGW. Spark SQL is still validated separately, but it requires Spark master,
worker, and submit containers.

### Is Ceph only replacing MinIO?

No. Ceph is the distributed storage layer. MinIO is the local baseline. The
same S3-compatible interface makes the pipeline portable, but Ceph adds the
distributed storage behavior that MinIO local mode does not demonstrate.

### Why not run the full Airflow DAG on Ceph during the final demo?

The host is memory constrained. The DAG tasks were validated independently:
configuration check, storage health, manifest generation, bronze upload, Spark
silver transform, Spark gold transform, and query checks. Running the entire
Airflow and Spark stack together is an operational limitation of the laptop
lab, not a storage design limitation.

## Recommended Slide Emphasis

Use the slides to emphasize:

1. Ceph as the Data Lake storage backbone.
2. Spark/Trino/Airflow as clients of the S3-compatible storage layer.
3. Bronze/silver/gold buckets on Ceph RGW.
4. Node outage demo:
   - `hadoop-worker2` down;
   - `osd.2` down;
   - quorum remains;
   - Trino query still succeeds;
   - recovery to `HEALTH_OK`.
5. Benchmark as a lab baseline with clear limitations.

The main story should be:

> The pipeline proves S3 compatibility. The fault-tolerance demo proves why
> Ceph matters as a distributed Data Lake storage layer.
