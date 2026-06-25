# CephFS and RBD Demo

This document records two optional Ceph capability demos beyond the main
Ceph RGW Data Lake path:

- CephFS: distributed POSIX-compatible shared filesystem.
- RBD: distributed block storage mapped as a Linux block device.

The main project pipeline uses Ceph RGW as S3-compatible object storage for
bronze, silver, and gold Data Lake layers. These two demos show why Ceph is a
broader distributed storage platform than a single-purpose object store.

## Environment

- Ceph version: `18.2.8 reef`
- Deployment: `cephadm`
- Cluster hosts:
  - `hadoop-master` / `192.168.56.101`
  - `hadoop-worker1` / `192.168.56.102`
  - `hadoop-worker2` / `192.168.56.103`
- OSD layout: 3 OSDs, one OSD per VM
- Baseline health before demo:

```text
health: HEALTH_OK
mon: 3 daemons, quorum hadoop-master,hadoop-worker1,hadoop-worker2
osd: 3 osds: 3 up, 3 in
rgw: 1 daemon active
```

## Summary

| Capability | Demo object | Mounted as | Main evidence |
|---|---|---|---|
| CephFS | `datalake_fs` | `/mnt/cephfs-datalake` | Files written on one node were visible on another node. |
| RBD | `rbd-demo/demo-block` | `/dev/rbd0` -> `/mnt/rbd-demo` | Ceph image became a Linux block device, formatted as `ext4`. |

## CephFS Demo

### Purpose

CephFS demonstrates that the same Ceph cluster can expose a distributed
filesystem, not only S3-compatible object storage.

In a data platform, CephFS can be used for:

- shared logs;
- pipeline checkpoints;
- shared configuration;
- small operational files;
- workloads that need filesystem semantics instead of S3 object semantics.

This is a capability that MinIO does not provide.

### Create CephFS Volume

The CephFS volume was created from `hadoop-master`:

```bash
sudo cephadm shell -- ceph fs volume create datalake_fs
sudo cephadm shell -- ceph fs ls
```

Observed filesystem:

```text
name: datalake_fs
metadata pool: cephfs.datalake_fs.meta
data pools: [cephfs.datalake_fs.data]
```

Ceph automatically created two pools for the filesystem:

| Pool | Purpose |
|---|---|
| `cephfs.datalake_fs.meta` | CephFS metadata |
| `cephfs.datalake_fs.data` | CephFS file data |

The MDS daemons were deployed by cephadm:

```bash
sudo cephadm shell -- ceph orch ps --daemon_type mds
```

Observed MDS state:

```text
mds.datalake_fs.hadoop-master.*   running
mds.datalake_fs.hadoop-worker2.*  running
```

The cluster health output showed:

```text
mds: 1/1 daemons up, 1 standby
volumes: 1/1 healthy
```

### Authorize CephFS Client

A client identity was created for the demo:

```bash
sudo cephadm shell -- ceph fs authorize datalake_fs client.datalakefs / rw
```

The command returns a secret key for `client.datalakefs`. The key must not be
committed to the repository. On each client host, save only the key value in:

```text
/etc/ceph/ceph.client.datalakefs.secret
```

Example format:

```text
<cephfs-client-secret-key>
```

Then restrict permissions:

```bash
sudo chmod 600 /etc/ceph/ceph.client.datalakefs.secret
```

### Mount on `hadoop-master`

Install the Ceph client tools if needed:

```bash
sudo apt install -y ceph-common
```

Create the mount point:

```bash
sudo mkdir -p /mnt/cephfs-datalake
```

Mount CephFS:

```bash
sudo mount -t ceph 192.168.56.101:6789:/ /mnt/cephfs-datalake \
  -o name=datalakefs,fs=datalake_fs,secretfile=/etc/ceph/ceph.client.datalakefs.secret
```

Validate the mount:

```bash
df -h | grep ceph
mount | grep ceph
```

Observed result on `hadoop-master`:

```text
192.168.56.101:6789:/  15G  0  15G  0%  /mnt/cephfs-datalake
192.168.56.101:6789:/ on /mnt/cephfs-datalake type ceph
```

Write sample files from `hadoop-master`:

```bash
echo "CephFS demo from hadoop-master at $(date)" | sudo tee /mnt/cephfs-datalake/demo.txt
sudo mkdir -p /mnt/cephfs-datalake/pipeline-logs
echo "Spark job log placeholder" | sudo tee /mnt/cephfs-datalake/pipeline-logs/spark-job.log
```

Validate local reads:

```bash
cat /mnt/cephfs-datalake/demo.txt
cat /mnt/cephfs-datalake/pipeline-logs/spark-job.log
```

Observed output:

```text
CephFS demo from hadoop-master at Thu Jun 25 08:51:38 AM UTC 2026
Spark job log placeholder
```

### Mount on `hadoop-worker1`

Install the client tools:

```bash
sudo apt install -y ceph-common
```

Create the same secret file and mount point:

```bash
sudo mkdir -p /etc/ceph
sudo nano /etc/ceph/ceph.client.datalakefs.secret
sudo chmod 600 /etc/ceph/ceph.client.datalakefs.secret
sudo mkdir -p /mnt/cephfs-datalake
```

Mount the same CephFS volume:

```bash
sudo mount -t ceph 192.168.56.101:6789:/ /mnt/cephfs-datalake \
  -o name=datalakefs,fs=datalake_fs,secretfile=/etc/ceph/ceph.client.datalakefs.secret
```

Validate the mount:

```bash
df -h | grep ceph
mount | grep ceph
```

Observed result on `hadoop-worker1`:

```text
192.168.56.101:6789:/  15G  0  15G  0%  /mnt/cephfs-datalake
192.168.56.101:6789:/ on /mnt/cephfs-datalake type ceph
```

The client printed warnings about missing `ceph.conf` and keyring files, but
the mount still succeeded because the monitor address and `secretfile` were
provided explicitly. For a cleaner production-style client setup, copy
`ceph.conf` and use a keyring file.

Read files created on `hadoop-master`:

```bash
ls -lah /mnt/cephfs-datalake
cat /mnt/cephfs-datalake/demo.txt
cat /mnt/cephfs-datalake/pipeline-logs/spark-job.log
```

Observed output:

```text
CephFS demo from hadoop-master at Thu Jun 25 08:51:38 AM UTC 2026
Spark job log placeholder
```

Write a file from `hadoop-worker1`:

```bash
echo "CephFS demo from hadoop-worker1 at $(date)" | sudo tee /mnt/cephfs-datalake/worker1.txt
```

Read it back on `hadoop-master`:

```bash
cat /mnt/cephfs-datalake/worker1.txt
```

Observed output:

```text
CephFS demo from hadoop-worker1 at Thu Jun 25 09:02:09 AM UTC 2026
```

### CephFS Interpretation

The CephFS demo proves that:

- `hadoop-master` and `hadoop-worker1` can mount the same distributed
  filesystem;
- files written by one node are visible from the other node;
- Ceph provides file storage in addition to RGW object storage;
- the filesystem is backed by Ceph pools and MDS daemons.

Short presentation version:

> CephFS shows that Ceph can provide a distributed shared filesystem. In this
> demo, a file written from `hadoop-master` was immediately readable from
> `hadoop-worker1`, and a file written from `hadoop-worker1` was readable from
> `hadoop-master`. This is a storage mode that MinIO does not provide.

## RBD Demo

### Purpose

RBD demonstrates Ceph distributed block storage. It exposes a Ceph-backed image
as a Linux block device, similar to a virtual disk.

In a data platform or private cloud, RBD can be used for:

- VM disks;
- database disks;
- persistent volumes;
- application data volumes that need block-device semantics.

This is also a capability that MinIO does not provide.

### Verify Local Client Tools

The RBD tool was available on `hadoop-master`:

```bash
which rbd
```

Observed output:

```text
/usr/bin/rbd
```

Ceph config and admin keyring were also available:

```bash
ls /etc/ceph
```

Observed files:

```text
ceph.client.admin.keyring
ceph.conf
ceph.pub
rbdmap
```

### Create RBD Pool

Create and initialize an RBD pool:

```bash
sudo cephadm shell -- ceph osd pool create rbd-demo 32
sudo cephadm shell -- ceph osd pool application enable rbd-demo rbd
sudo cephadm shell -- rbd pool init rbd-demo
```

Validate that the pool exists:

```bash
sudo cephadm shell -- ceph osd pool ls
sudo cephadm shell -- ceph osd pool get rbd-demo size
```

Observed result:

```text
rbd-demo
size: 3
```

`size: 3` means data in this pool is replicated three times across the Ceph
cluster, subject to the CRUSH placement rules and available OSDs.

### Create RBD Image

Create a 1 GiB image:

```bash
sudo rbd create rbd-demo/demo-block --size 1G --image-feature layering
```

Validate the image:

```bash
sudo rbd ls rbd-demo
sudo rbd info rbd-demo/demo-block
```

Observed image:

```text
rbd image 'demo-block':
        size 1 GiB in 256 objects
        format: 2
        features: layering
```

The image size is `1 GiB`. Internally it is split into RADOS objects and stored
in the `rbd-demo` pool.

### Map RBD as a Block Device

Map the image into the Linux host:

```bash
sudo rbd map rbd-demo/demo-block
```

Observed output:

```text
/dev/rbd0
```

Validate the mapping:

```bash
lsblk
sudo rbd showmapped
```

Observed mapping:

```text
rbd0  251:0  0  1G  0 disk

id  pool      namespace  image       snap  device
0   rbd-demo             demo-block  -     /dev/rbd0
```

At this point, Linux sees the Ceph image as a normal block device.

### Format and Mount

Format the device with `ext4`:

```bash
sudo mkfs.ext4 /dev/rbd0
```

Create a mount point and mount the device:

```bash
sudo mkdir -p /mnt/rbd-demo
sudo mount /dev/rbd0 /mnt/rbd-demo
```

Validate:

```bash
df -h | grep rbd
lsblk
```

Observed result:

```text
/dev/rbd0  974M  280K  906M  1%  /mnt/rbd-demo
rbd0       251:0  0    1G    0 disk /mnt/rbd-demo
```

### Write and Read Files

Write sample files:

```bash
echo "RBD demo block storage from hadoop-master at $(date)" | sudo tee /mnt/rbd-demo/rbd-demo.txt
sudo mkdir -p /mnt/rbd-demo/app-data
echo "This looks like a persistent disk for an application" | sudo tee /mnt/rbd-demo/app-data/data.txt
```

Read them back:

```bash
cat /mnt/rbd-demo/rbd-demo.txt
cat /mnt/rbd-demo/app-data/data.txt
```

Observed output:

```text
RBD demo block storage from hadoop-master at Thu Jun 25 09:23:54 AM UTC 2026
This looks like a persistent disk for an application
```

Check RBD space usage:

```bash
sudo rbd du rbd-demo/demo-block
```

Observed result:

```text
NAME        PROVISIONED  USED
demo-block        1 GiB  32 MiB
```

This shows thin provisioning: the image is provisioned as 1 GiB, but only a
small amount is actually allocated after writing the demo files and filesystem
metadata.

Check pool usage:

```bash
sudo cephadm shell -- ceph df
```

Observed pool entry:

```text
rbd-demo  32 PGS  STORED 1.0 MiB  OBJECTS 13  USED 3.1 MiB
```

The exact `rbd du` and `ceph df` numbers do not need to match one-to-one.
`rbd du` reports RBD image usage, while `ceph df` reports pool-level RADOS
usage after object allocation and replication accounting.

### RBD Interpretation

The RBD demo proves that:

- Ceph can create block storage images;
- an RBD image can be mapped into Linux as `/dev/rbd0`;
- the block device can be formatted and mounted like a normal disk;
- the data is stored in a replicated Ceph pool, not only on the local host.

Short presentation version:

> RBD shows that Ceph can provide distributed block storage. In this demo,
> `rbd-demo/demo-block` was mapped into `hadoop-master` as `/dev/rbd0`,
> formatted as `ext4`, mounted at `/mnt/rbd-demo`, and used like a normal
> disk. The backing pool uses `replica: x3`, so the storage is managed by Ceph
> rather than by a single local disk.

### Important RBD Safety Note

Do not mount the same `ext4` RBD image on multiple hosts at the same time.
RBD is block storage. A normal filesystem such as `ext4` expects one writer at
a time. If the same image is mounted read-write on multiple nodes
simultaneously, data corruption can occur.

To move the image to another host:

```bash
sudo umount /mnt/rbd-demo
sudo rbd unmap /dev/rbd0
```

Then map and mount it on the other host.

## Dashboard Evidence

The Ceph Dashboard Pools page can be used to show both CephFS and RBD pools.
Because the table page size may show only ten rows, `rbd-demo` can appear on
the second page when the cluster has eleven pools.

Useful columns:

| Column | Expected evidence |
|---|---|
| Name | `cephfs.datalake_fs.meta`, `cephfs.datalake_fs.data`, `rbd-demo` |
| Data Protection | `replica: x3` |
| Applications | `cephfs` for CephFS pools, `rbd` for RBD pool |
| PG Status | `active+clean` |

## How This Supports the Project

The main Data Lake path remains:

```text
Spark / Trino / Python -> S3 API -> Ceph RGW -> Ceph OSDs
```

CephFS and RBD are not required for the bronze/silver/gold pipeline, but they
make the Ceph value proposition clearer:

| Storage interface | Ceph component | Project relevance |
|---|---|---|
| Object storage | RGW | Main Data Lake buckets and S3-compatible analytics path |
| File storage | CephFS | Shared filesystem demo for logs, checkpoints, config, shared files |
| Block storage | RBD | Persistent disk demo for VM, database, or application volumes |

Final presentation message:

> The project uses RGW for the Data Lake, but Ceph is not only an S3 endpoint.
> The CephFS and RBD demos show that the same Ceph cluster can also provide
> distributed file storage and block storage. This is why Ceph is positioned as
> a unified storage platform, while MinIO is used only as a lightweight local
> S3-compatible baseline.

## Cleanup Commands

Only run cleanup after screenshots or video capture are finished.

Unmount CephFS from each mounted host:

```bash
sudo umount /mnt/cephfs-datalake
```

Remove the CephFS client:

```bash
sudo cephadm shell -- ceph auth rm client.datalakefs
```

Remove the CephFS volume:

```bash
sudo cephadm shell -- ceph fs volume rm datalake_fs --yes-i-really-mean-it
```

Unmount and remove RBD demo objects:

```bash
sudo umount /mnt/rbd-demo
sudo rbd unmap /dev/rbd0
sudo rbd rm rbd-demo/demo-block
sudo cephadm shell -- ceph osd pool rm rbd-demo rbd-demo --yes-i-really-really-mean-it
```

