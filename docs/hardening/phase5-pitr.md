# E05 — physical backup and WAL recovery qualification

`tests/test_native_pitr.py` is an opt-in destructive test on the guarded local
Compose project. It configures archiving, completes two of three handlers, stops
runtime writers, takes `pg_basebackup`, and creates a named restore point. It then
runs the third external task and publishes the final message AFTER that point.

A fresh PostgreSQL container restores the physical backup and archived WAL using
`recovery_target_name`, with no engine worker touching it until recovery promotes.
An after-target marker and deliberately late-started workflow must be absent.
The pre-target marker and two committed handler results must be present. The
broker and independent external ledger are not rolled back. Runtime reconciliation
must finish the restored workflow; the third task may be called again but its
stable idempotency key must prevent a second external effect. Final publication
may be delivered again with the same logical event identity.

This is not a `pg_dump` test relabeled as PITR. It uses actual base-backup files,
archived WAL segments and a fresh restored PostgreSQL cluster. Qualification
records actual elapsed recovery time and deliberately lost post-target work;
it does NOT promise zero RPO, no duplicate delivery, or production RTO.

Primary operational references:
- https://www.postgresql.org/docs/16/continuous-archiving.html
- https://www.postgresql.org/docs/16/app-pgbasebackup.html
- https://www.postgresql.org/docs/16/runtime-config-wal.html#RUNTIME-CONFIG-WAL-RECOVERY-TARGET

Test WAL/base backups contain disposable data and are never uploaded as artifacts.
Production restore requires fencing ALL old writers, validating archive continuity,
reviewing post-target external work and start requests, and checking actual broker
retention/idempotency horizons before resuming. This fixture is not an automated
production failover controller. A test file existing is not verification evidence;
see the staged CI run and `qualification-results.json` for executed outcomes.
