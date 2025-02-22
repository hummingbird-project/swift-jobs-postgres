//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2024 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Foundation
import Jobs
import Logging
import NIOConcurrencyHelpers
import NIOCore
import PostgresMigrations
import PostgresNIO

/// Postgres Job queue implementation
///
/// The Postgres driver uses the database migration service ``/PostgresMigrations/DatabaseMigrations``
/// to create its database tables. Before the server is running you should run the migrations
/// to build your table.
/// ```
/// let migrations = PostgresMigrations()
/// let jobqueue = await JobQueue(
///     PostgresQueue(
///         client: postgresClient,
///         migrations: postgresMigrations,
///         configuration: configuration,
///         logger: logger
///    ),
///    numWorkers: numWorkers,
///    logger: logger
/// )
/// var app = Application(...)
/// app.beforeServerStarts {
///     try await migrations.apply(client: postgresClient, logger: logger, dryRun: applyMigrations)
/// }
/// ```
public final class PostgresJobQueue: JobQueueDriver {
    public typealias JobID = UUID

    /// what to do with failed/processing jobs from last time queue was handled
    public enum JobCleanup: Sendable {
        case doNothing
        case rerun
        case remove
    }

    /// Errors thrown by PostgresJobQueue
    public enum PostgresQueueError: Error, CustomStringConvertible {
        case failedToAdd

        public var description: String {
            switch self {
            case .failedToAdd:
                return "Failed to add job to queue"
            }
        }
    }

    /// Job Status
    enum Status: Int16, PostgresCodable {
        case pending = 0
        case processing = 1
        case failed = 2
    }

    /// Queue configuration
    public struct Configuration: Sendable {
        /// Queue poll time to wait if queue empties
        let pollTime: Duration

        ///  Initialize configuration
        /// - Parameter pollTime: Queue poll time to wait if queue empties
        public init(
            pollTime: Duration = .milliseconds(100)
        ) {
            self.pollTime = pollTime
        }
    }

    /// Postgres client used by Job queue
    public let client: PostgresClient
    /// Job queue configuration
    public let configuration: Configuration
    /// Logger used by queue
    public let logger: Logger

    let migrations: DatabaseMigrations
    let isStopped: NIOLockedValueBox<Bool>

    /// Initialize a PostgresJobQueue
    public init(client: PostgresClient, migrations: DatabaseMigrations, configuration: Configuration = .init(), logger: Logger) async {
        self.client = client
        self.configuration = configuration
        self.jobRegistry = .init()
        self.logger = logger
        self.isStopped = .init(false)
        self.migrations = migrations
        await migrations.add(CreateJobs())
        await migrations.add(CreateJobQueue())
        await migrations.add(CreateJobQueueMetadata())
        await migrations.add(CreateJobDelay())
        await migrations.add(UpdateJobDelay())
    }

    public func onInit() async throws {
        self.logger.info("Waiting for JobQueue migrations")
        /// Need migrations to have completed before job queue processing can start
        try await self.migrations.waitUntilCompleted()
    }

    ///  Cleanup job queues
    ///
    /// This function is used to re-run or delete jobs in a certain state. Failed jobs can be
    /// pushed back into the pending queue to be re-run or removed. When called at startup in
    /// theory no job should be set to processing, or set to pending but not in the queue. but if
    /// your job server crashes these states are possible, so we also provide options to re-queue
    /// these jobs so they are run again.
    ///
    /// The job queue needs to be running when you call cleanup. You can call `cleanup` with
    /// `failedJobs`` set to whatever you like at any point to re-queue failed jobs. Moving processing
    /// or pending jobs should only be done if you are certain there is nothing else processing
    /// the job queue.
    ///
    /// - Parameters:
    ///   - failedJobs: What to do with jobs tagged as failed
    ///   - processingJobs: What to do with jobs tagged as processing
    ///   - pendingJobs: What to do with jobs tagged as pending
    /// - Throws:
    public func cleanup(
        failedJobs: JobCleanup = .doNothing,
        processingJobs: JobCleanup = .doNothing,
        pendingJobs: JobCleanup = .doNothing
    ) async throws {
        do {
            /// wait for migrations to complete before running job queue cleanup
            try await self.migrations.waitUntilCompleted()
            _ = try await self.client.withConnection { connection in
                self.logger.info("Update Jobs")
                try await self.updateJobsOnInit(withStatus: .pending, onInit: pendingJobs, connection: connection)
                try await self.updateJobsOnInit(
                    withStatus: .processing,
                    onInit: processingJobs,
                    connection: connection
                )
                try await self.updateJobsOnInit(withStatus: .failed, onInit: failedJobs, connection: connection)
            }
        } catch let error as PSQLError {
            logger.error(
                "JobQueue initialization failed",
                metadata: [
                    "error": "\(String(reflecting: error))"
                ]
            )
            throw error
        }
    }

    ///  Register job
    /// - Parameters:
    ///   - job: Job Definition
    public func registerJob<Parameters: Codable & Sendable>(_ job: JobDefinition<Parameters>) {
        self.jobRegistry.registerJob(job)
    }

    /// Push Job onto queue
    /// - Returns: Identifier of queued job
    @discardableResult public func push<Parameters>(_ jobRequest: JobRequest<Parameters>, options: JobOptions) async throws -> JobID {
        let buffer = try self.jobRegistry.encode(jobRequest: jobRequest)
        let jobID = JobID()
        try await self.client.withTransaction(logger: self.logger) { connection in
            try await self.add(jobID: jobID, jobBuffer: buffer, connection: connection)
            try await self.addToQueue(jobID: jobID, connection: connection, delayUntil: options.delayUntil)
        }
        return jobID
    }

    /// Retry a job
    /// - Returns: Bool
    public func retry<Parameters>(_ id: JobID, jobRequest: JobRequest<Parameters>, options: JobOptions) async throws {
        let buffer = try self.jobRegistry.encode(jobRequest: jobRequest)
        try await self.client.withTransaction(logger: self.logger) { connection in
            try await self.updateJob(id: id, buffer: buffer, connection: connection)
            try await self.addToQueue(jobID: id, connection: connection, delayUntil: options.delayUntil)
        }
    }

    /// This is called to say job has finished processing and it can be deleted
    public func finished(jobID: JobID) async throws {
        try await self.delete(jobID: jobID)
    }

    /// This is called to say job has failed to run and should be put aside
    public func failed(jobID: JobID, error: Error) async throws {
        try await self.setStatus(jobID: jobID, status: .failed)
    }

    /// stop serving jobs
    public func stop() async {
        self.isStopped.withLockedValue { $0 = true }
    }

    /// shutdown queue once all active jobs have been processed
    public func shutdownGracefully() async {}

    public func getMetadata(_ key: String) async throws -> ByteBuffer? {
        let stream = try await self.client.query(
            "SELECT value FROM _hb_pg_job_queue_metadata WHERE key = \(key)",
            logger: self.logger
        )
        for try await value in stream.decode(ByteBuffer.self) {
            return value
        }
        return nil
    }

    public func setMetadata(key: String, value: ByteBuffer) async throws {
        try await self.client.query(
            """
            INSERT INTO _hb_pg_job_queue_metadata (key, value) VALUES (\(key), \(value))
            ON CONFLICT (key)
            DO UPDATE SET value = \(value)
            """,
            logger: self.logger
        )
    }

    func popFirst() async throws -> JobQueueResult<JobID>? {
        do {
            // The withTransaction closure returns a Result<(ByteBuffer, JobID)?, Error> because
            // we want to be able to exit the closure without cancelling the transaction
            let result = try await self.client.withTransaction(logger: self.logger) { connection -> Result<(ByteBuffer, JobID)?, Error> in
                try Task.checkCancellation()

                let stream = try await connection.query(
                    """
                    WITH next_job AS (
                        SELECT
                            job_id
                        FROM _hb_pg_job_queue
                        WHERE delayed_until <= NOW()
                        ORDER BY createdAt, delayed_until ASC
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    )
                    DELETE FROM
                        _hb_pg_job_queue
                    WHERE job_id = (SELECT job_id FROM next_job)
                    RETURNING job_id
                    """,
                    logger: self.logger
                )
                // return nil if nothing in queue
                guard let jobID = try await stream.decode(UUID.self, context: .default).first(where: { _ in true }) else {
                    return Result.success(nil)
                }
                // set job status to processing
                try await self.setStatus(jobID: jobID, status: .processing, connection: connection)

                // select job from job table
                let stream2 = try await connection.query(
                    "SELECT job FROM _hb_pg_jobs WHERE id = \(jobID)",
                    logger: self.logger
                )
                guard let buffer = try await stream2.decode(ByteBuffer.self, context: .default).first(where: { _ in true }) else {
                    logger.error(
                        "Failed to find job with id",
                        metadata: [
                            "JobID": "\(jobID)"
                        ]
                    )
                    // if failed to find the job in the job table return nil
                    return .success(nil)
                }
                return .success((buffer, jobID))

            }
            guard let (buffer, jobID) = try result.get() else { return nil }
            do {
                let jobInstance = try self.jobRegistry.decode(buffer)
                return JobQueueResult(id: jobID, result: .success(jobInstance))
            } catch let error as JobQueueError {
                return JobQueueResult(id: jobID, result: .failure(error))
            }
        } catch let error as PSQLError {
            logger.error(
                "Failed to get job from queue",
                metadata: [
                    "error": "\(String(reflecting: error))"
                ]
            )
            throw error
        } catch let error as JobQueueError {
            logger.error(
                "Job failed",
                metadata: [
                    "error": "\(String(reflecting: error))"
                ]
            )
            throw error
        }
    }

    func add(jobID: JobID, jobBuffer: ByteBuffer, connection: PostgresConnection) async throws {
        try await connection.query(
            """
            INSERT INTO _hb_pg_jobs (id, job, status)
            VALUES (\(jobID), \(jobBuffer), \(Status.pending))
            """,
            logger: self.logger
        )
    }
    // TODO: maybe add a new column colum for attempt so far after PR https://github.com/hummingbird-project/swift-jobs/pull/63 is merged?
    func updateJob(id: JobID, buffer: ByteBuffer, connection: PostgresConnection) async throws {
        try await connection.query(
            """
            UPDATE _hb_pg_jobs
            SET job = \(buffer),
                lastModified = \(Date.now),
                status = \(Status.failed)
            WHERE id = \(id)
            """,
            logger: self.logger
        )
    }

    func delete(jobID: JobID) async throws {
        try await self.client.query(
            "DELETE FROM _hb_pg_jobs WHERE id = \(jobID)",
            logger: self.logger
        )
    }

    func addToQueue(jobID: JobID, connection: PostgresConnection, delayUntil: Date) async throws {
        // TODO: assign Date.now in swift-jobs options?
        try await connection.query(
            """
            INSERT INTO _hb_pg_job_queue (job_id, createdAt, delayed_until)
            VALUES (\(jobID), \(Date.now), \(delayUntil))
            -- We have found an existing job with the same id, SKIP this INSERT 
            ON CONFLICT (job_id) DO NOTHING
            """,
            logger: self.logger
        )
    }

    func setStatus(jobID: JobID, status: Status, connection: PostgresConnection) async throws {
        try await connection.query(
            "UPDATE _hb_pg_jobs SET status = \(status), lastModified = \(Date.now) WHERE id = \(jobID)",
            logger: self.logger
        )
    }

    func setStatus(jobID: JobID, status: Status) async throws {
        try await self.client.query(
            "UPDATE _hb_pg_jobs SET status = \(status), lastModified = \(Date.now) WHERE id = \(jobID)",
            logger: self.logger
        )
    }

    func getJobs(withStatus status: Status) async throws -> [JobID] {
        let stream = try await self.client.query(
            "SELECT id FROM _hb_pg_jobs WHERE status = \(status) FOR UPDATE SKIP LOCKED",
            logger: self.logger
        )
        var jobs: [JobID] = []
        for try await id in stream.decode(JobID.self, context: .default) {
            jobs.append(id)
        }
        return jobs
    }

    func updateJobsOnInit(withStatus status: Status, onInit: JobCleanup, connection: PostgresConnection) async throws {
        switch onInit {
        case .remove:
            try await connection.query(
                "DELETE FROM _hb_pg_jobs WHERE status = \(status) ",
                logger: self.logger
            )

        case .rerun:
            let jobs = try await getJobs(withStatus: status)
            self.logger.info("Moving \(jobs.count) jobs with status: \(status) to job queue")
            for jobID in jobs {
                try await self.addToQueue(jobID: jobID, connection: connection, delayUntil: Date.now)
            }

        case .doNothing:
            break
        }
    }

    let jobRegistry: JobRegistry
}

/// extend PostgresJobQueue to conform to AsyncSequence
extension PostgresJobQueue {
    public struct AsyncIterator: AsyncIteratorProtocol {
        public typealias Element = JobQueueResult<JobID>

        let queue: PostgresJobQueue

        public func next() async throws -> Element? {
            while true {
                if self.queue.isStopped.withLockedValue({ $0 }) {
                    return nil
                }

                if let job = try await queue.popFirst() {
                    return job
                }
                // we only sleep if we didn't receive a job
                try await Task.sleep(for: self.queue.configuration.pollTime)
            }
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        .init(queue: self)
    }
}

extension JobQueueDriver where Self == PostgresJobQueue {
    /// Return Postgres driver for Job Queue
    /// - Parameters:
    ///   - client: Postgres client
    ///   - migrations: Database migration collection to add postgres job queue migrations to
    ///   - configuration: Queue configuration
    ///   - logger: Logger used by queue
    public static func postgres(
        client: PostgresClient,
        migrations: DatabaseMigrations,
        configuration: PostgresJobQueue.Configuration = .init(),
        logger: Logger
    ) async -> Self {
        await Self(client: client, migrations: migrations, configuration: configuration, logger: logger)
    }
}
