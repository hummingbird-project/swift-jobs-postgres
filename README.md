<p align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://github.com/hummingbird-project/hummingbird/assets/9382567/48de534f-8301-44bd-b117-dfb614909efd">
  <img src="https://github.com/hummingbird-project/hummingbird/assets/9382567/e371ead8-7ca1-43e3-8077-61d8b5eab879">
</picture>
</p>  
<p align="center">
<a href="https://swift.org">
  <img src="https://img.shields.io/badge/swift-5.9-brightgreen.svg"/>
</a>
<a href="https://github.com/hummingbird-project/swift-jobs-postgres/actions?query=workflow%3ACI">
  <img src="https://github.com/hummingbird-project/swift-jobs-postgres/actions/workflows/ci.yml/badge.svg?branch=main"/>
</a>
<a href="https://discord.gg/7ME3nZ7mP2">
  <img src="https://img.shields.io/badge/chat-discord-brightgreen.svg"/>
</a>
</p>

# Postgres driver for Jobs

PostgreSQL is a powerful, open source object-relational database system.

## Documentation

Reference documentation for JobsPostgres can be found [here](https://docs.hummingbird.codes/2.0/documentation/jobspostgres)

## Breaking changes from 1.0.0-beta.1

The all jobs related tables have been moved to `swift_jobs` schema

The following tables have also been renamed

* _hb_pg_job_queue -> swift_jobs.queues
* _hb_pg_jobs -> swift_jobs.jobs
* _hb_pg_job_queue_metadata -> swift_jobs.queues_metadata

if you have jobs queues in the previous schema, you'll need to move these jobs over to the new schema as follow

```SQL
INSERT INTO swift_jobs.jobs(id, status, last_modified, job)
SELECT
    id,
    status,
    last_modified,
    job
FROM _hb_pg_jobs
```
should you have any pending jobs from the previous queue, you'll need to also run the following query

```SQL
INSERT INTO swift_jobs.queues(job_id, created_at, delayed_until, queue_name)
SELECT
    job_id,
    createdAt,
    delayed_until,
    'default' -- UNLESS queueName was changed, you'll need to match the name of the queue here
FROM _hb_pg_job_queue
```
and finally
* `DROP TABLE _hb_pg_jobs`
* `DROP TABLE _hb_pg_job_queue`
* `DROP TABLE _hb_pg_job_queue_metadata`  

Should you also want to preseve the job metadata, yoo'll need to run

```SQL
INSERT INTO swift_jobs.queues_metadata(key, value, queue_name)
SELECT
    key,
    value,
    'default' -- UNLESS queueName was changed, you'll need to match the name of the queue here
FROM _hb_pg_job_queue_metadata
```
