# Debezium PostgreSQL CDC: Operational Runbook & Production Playbooks

This runbook covers operational hazards, failure modes, monitoring metrics, and recovery playbooks essential for operating Debezium CDC at scale.

---

## ⚠️ Incident 1: Unconsumed WAL Bloat & Disk Exhaustion

### The Problem
PostgreSQL will **never delete WAL segments** that have not been acknowledged by all active logical replication slots (`confirmed_flush_lsn`). 
If Kafka Connect stops, crashes, or suffers prolonged network disconnects while the database receives heavy write traffic, WAL files accumulate indefinitely in `pg_wal/`, eventually filling the host disk and causing PostgreSQL to crash into read-only recovery mode.

### Diagnosis Query
```sql
SELECT
    slot_name,
    plugin,
    active,
    confirmed_flush_lsn,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag_size,
    wal_status
FROM pg_replication_slots;
```

### Remediation Playbook
1. **Short-term relief (Prevent immediate database outage):**
   If disk capacity exceeds 90% and connector recovery is not imminent:
   ```sql
   -- Drop the replication slot to allow Postgres checkpoint to purge WAL
   SELECT pg_drop_replication_slot('debezium_slot');
   ```
   > [!CAUTION]
   > Dropping the replication slot means Debezium will miss interim transactions. You must configure the connector with `snapshot.mode = 'when_needed'` to re-snapshot tables upon restart.

2. **Long-term Safeguard (`max_slot_wal_keep_size`):**
   Configure Postgres to invalidate the slot rather than exhaust disk space:
   ```sql
   ALTER SYSTEM SET max_slot_wal_keep_size = '50GB';
   SELECT pg_reload_conf();
   ```
   If lag exceeds 50GB, Postgres invalidates the slot (`wal_status = 'lost'`), protecting the database cluster at the expense of requiring connector re-snapshot.

---

## 💓 Incident 2: Stalled LSN on Low-Traffic Databases (Heartbeat Query)

### The Problem
If a database contains tables that are rarely updated, the connector LSN does not advance because no events are emitted into the publication stream. PostgreSQL retains WAL logs that contain changes to *other* non-captured tables because the replication slot's `confirmed_flush_lsn` is stalled.

### Solution: Debezium Heartbeat Action Query
In `connectors/postgres-debezium-connector.json`:
```json
{
  "heartbeat.interval.ms": "5000",
  "heartbeat.action.query": "INSERT INTO debezium_signal (id, type, data) VALUES ('heartbeat', 'HEARTBEAT', NOW()::text) ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data;"
}
```
*How it works*: Every 5 seconds, Debezium executes an UPDATE query on Postgres. This forces a write to the WAL, advancing the LSN and allowing Postgres to safely recycle obsolete WAL segments.

---

## 📸 Playbook 3: Zero-Downtime Ad-Hoc & Incremental Snapshots

### The Problem
Initial snapshotting of multi-terabyte tables can take hours or days. If a new table is added to the publication later, restarting the connector with `snapshot.mode = 'initial'` would re-read all tables from scratch, causing duplicate events and massive load.

### Solution: Watermarked Incremental Snapshots via Signaling Table
Debezium supports triggering table snapshots on demand via SQL:

```sql
INSERT INTO debezium_signal (id, type, data)
VALUES (
    'ad-hoc-customers-01',
    'execute-snapshot',
    '{"data-collections": ["public.customers"], "type": "INCREMENTAL"}'
);
```

### How Incremental Snapshots Work:
1. Debezium reads the table in configurable chunk sizes (e.g. 10,000 rows).
2. It writes a **low watermark** to the signaling table, queries the chunk, and writes a **high watermark**.
3. Any streaming CDC events that occur during the chunk read are reconciled in-memory, ensuring **no exclusive table locks** and zero downtime.

---

## 📈 Key Production Metrics (JMX & Prometheus)

| Metric | Threshold | Meaning |
|---|---|---|
| `debezium.postgres:type=connector-metrics,context=streaming,server=inventory:MilliSecondsBehindSource` | `> 5000 ms` | Connector lag behind PostgreSQL WAL generation. |
| `debezium.postgres:type=connector-metrics,context=streaming,server=inventory:QueueRemainingCapacity` | `< 10%` | Connector internal buffer exhaustion (Kafka producer backpressure). |
| `pg_replication_slots.lag_bytes` | `> 10 GB` | Disk retention risk in `pg_wal/`. |
