# Local S3-Compatible Storage

Phase 1 validates the Data Lake storage contract through the S3 API:

1. connect to an S3-compatible endpoint;
2. create the Data Lake buckets idempotently;
3. upload a small object;
4. verify checksum through metadata and download;
5. delete the smoke-test object.

For local development this repository uses MinIO because it starts quickly on
Windows/Docker Desktop. The same Python scripts work with Ceph RGW by changing
the S3 endpoint and credentials in `.env`.

## Local MinIO

Start storage:

```bash
make up
```

The default local endpoints are:

- S3 API: `http://localhost:9000`
- Console: `http://localhost:9001`

Default local credentials are defined in `.env.example`:

```text
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin123
```

Create buckets:

```bash
make init-buckets
```

Run a smoke test:

```bash
make storage-smoke
```

Check reachability:

```bash
make health
```

Stop storage:

```bash
make down
```

## Using Ceph RGW Instead

After you have a Ceph RGW endpoint and S3 user, update `.env`:

```text
S3_ENDPOINT=http://<rgw-host>:7480
S3_ACCESS_KEY=<ceph-s3-access-key>
S3_SECRET_KEY=<ceph-s3-secret-key>
S3_PATH_STYLE_ACCESS=true
S3_USE_SSL=false
```

Then run the same commands:

```bash
make init-buckets
make storage-smoke
```

## Expected Buckets

- `datalake-bronze`
- `datalake-silver`
- `datalake-gold`
- `datalake-system`

Bucket creation is idempotent. Re-running `make init-buckets` should print
`exists` for buckets that are already present and still finish successfully.
