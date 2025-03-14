//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2024-2025 the Hummingbird authors
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
public final class PostgresJobQueue: JobQueueDriver, CancellableJobQueue, ResumableJobQueue {

    public typealias JobID = UUID
    /// what to do with failed/processing jobs from last time queue was handled
    public enum JobCleanup: Sendable {
        case doNothing
        case rerun
        case remove
    }

    /// Job priority from lowest to highest
    public struct JobPriority: Equatable, Sendable {
        let rawValue: Priority

        // Job priority
        enum Priority: Int16, Sendable, PostgresCodable {
            case lowest = 0
            case lower = 1
            case normal = 2
            case higher = 3
            case highest = 4
        }
        /// Lowest priority
        public static func lowest() -> JobPriority {
            JobPriority(rawValue: .lowest)
        }
        /// Lower priority
        public static func lower() -> JobPriority {
            JobPriority(rawValue: .lower)
        }
        /// Normal is the default priority
        public static func normal() -> JobPriority {
            JobPriority(rawValue: .normal)
        }
        /// Higher priority
        public static func higher() -> JobPriority {
            JobPriority(rawValue: .higher)
        }
        /// Higgest priority
        public static func highest() -> JobPriority {
            JobPriority(rawValue: .highest)
        }
    }

    /// Options for job pushed to queue
    public struct JobOptions: JobOptionsProtocol {
        /// Delay running job until
        public var delayUntil: Date
        /// Priority for this job
        public var priority: JobPriority

        /// Default initializer for JobOptions
        public init() {
            self.delayUntil = .now
            self.priority = .normal()
        }

        ///  Initializer for JobOptions
        /// - Parameter delayUntil: Whether job execution should be delayed until a later date
        public init(delayUntil: Date?) {
            self.delayUntil = delayUntil ?? .now
            self.priority = .normal()
        }

        ///  Initializer for JobOptions
        /// - Parameter delayUntil: Whether job execution should be delayed until a later date
        /// - Parameter priority: The priority for a job
        public init(delayUntil: Date = .now, priority: JobPriority = .normal()) {
            self.delayUntil = delayUntil
            self.priority = priority
        }
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
        case cancelled = 3
        case paused = 4
    }

    /// Queue configuration
    public struct Configuration: Sendable {
        /// Queue poll time to wait if queue empties
        let pollTime: Duration
        /// Which Queue to push jobs into
        let queueName: String

        ///  Initialize configuration
        /// - Parameters
        ///   - pollTime: Queue poll time to wait if queue empties
        ///   - queueName: Name of queue we are handing
        public init(
            pollTime: Duration = .milliseconds(100),
            queueName: String = "default"
        ) {
            self.pollTime = pollTime
            self.queueName = queueName
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
        await migrations.add(CreateSwiftJobsMigrations(), skipDuplicates: true)
    }

    public func onInit() async throws {
        self.logger.info("Waiting for JobQueue migrations")
        /// Need migrations to have completed before job queue processing can start
        try await self.migrations.waitUntilCompleted()
    }

    ///  Cancel job
    ///
    /// This function is used to cancel a job. Job cancellation is not gaurenteed howerever.
    /// Cancellable jobs are jobs with a delayed greather than when the cancellation request was made
    ///
    /// - Parameters:
    ///   - jobID: an existing job
    /// - Throws:
    public func cancel(jobID: JobID) async throws {
        try await self.client.withTransaction(logger: logger) { connection in
            try await deleteFromQueue(jobID: jobID, connection: connection)
            try await delete(jobID: jobID)
        }
    }

    ///  Pause job
    ///
    /// This function is used to pause a job. Job paus is not gaurenteed howerever.
    /// Pausable jobs are jobs with a delayed greather than when the pause request was made
    ///
    /// - Parameters:
    ///   - jobID: an existing job
    /// - Throws:
    public func pause(jobID: UUID) async throws {
        try await self.client.withTransaction(logger: logger) { connection in
            try await deleteFromQueue(jobID: jobID, connection: connection)
            try await setStatus(jobID: jobID, status: .paused, connection: connection)
        }
    }

    ///  Resume job
    ///
    /// This function is used to resume jobs. Job  is not gaurenteed howerever.
    /// Cancellable jobs are jobs with a delayed greather than when the cancellation request was made
    ///
    /// - Parameters:
    ///   - jobID: an existing job
    /// - Throws:
    public func resume(jobID: JobID) async throws {
        try await self.client.withTransaction(logger: logger) { connection in
            try await setStatus(jobID: jobID, status: .pending, connection: connection)
            try await addToQueue(
                jobID: jobID,
                queueName: configuration.queueName,
                options: .init(),
                connection: connection
            )
        }
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
                    "Error": "\(String(reflecting: error))"
                ]
            )
            throw error
        }
    }

    ///  Register job
    /// - Parameters:
    ///   - job: Job Definition
    public func registerJob<Parameters: JobParameters>(_ job: JobDefinition<Parameters>) {
        self.jobRegistry.registerJob(job)
    }

    /// Push Job onto queue
    /// - Returns: Identifier of queued job
    @discardableResult public func push<Parameters: JobParameters>(_ jobRequest: JobRequest<Parameters>, options: JobOptions) async throws -> JobID {
        let jobID = JobID()
        try await self.client.withTransaction(logger: self.logger) { connection in
            try await self.add(jobID: jobID, jobRequest: jobRequest, queueName: configuration.queueName, connection: connection)
            try await self.addToQueue(jobID: jobID, queueName: configuration.queueName, options: options, connection: connection)
        }
        return jobID
    }

    /// Retry an existing Job
    /// - Parameters
    ///   - id: Job instance ID
    ///   - jobRequest: Job Request
    ///   - options: Job retry options
    public func retry<Parameters: JobParameters>(_ id: JobID, jobRequest: JobRequest<Parameters>, options: JobRetryOptions) async throws {
        let buffer = try self.jobRegistry.encode(jobRequest: jobRequest)
        try await self.client.withTransaction(logger: self.logger) { connection in
            try await self.updateJob(id: id, buffer: buffer, connection: connection)
            try await self.addToQueue(
                jobID: id,
                queueName: configuration.queueName,
                options: .init(delayUntil: options.delayUntil),
                connection: connection
            )
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
            "SELECT value FROM swift_jobs.queues_metadata WHERE key = \(key) AND queue_name = \(configuration.queueName)",
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
            INSERT INTO swift_jobs.queues_metadata (key, value, queue_name)
            VALUES (\(key), \(value), \(configuration.queueName))
            ON CONFLICT (key)
            DO UPDATE SET value = \(value)
            """,
            logger: self.logger
        )
    }

    func popFirst() async throws -> JobQueueResult<JobID>? {
        enum PopFirstResult {
            case nothing
            case result(Result<PostgresRow, Error>, jobID: JobID)
        }
        do {
            // The withTransaction closure returns a Result<(ByteBuffer, JobID)?, Error> because
            // we want to be able to exit the closure without cancelling the transaction
            let popFirstResult = try await self.client.withTransaction(logger: self.logger) {
                connection -> PopFirstResult in
                try Task.checkCancellation()

                let stream = try await connection.query(
                    """
                    WITH next_job AS (
                        SELECT
                            job_id
                        FROM swift_jobs.queues
                        WHERE delayed_until <= NOW()
                        AND queue_name = \(configuration.queueName)
                        ORDER BY priority DESC, delayed_until ASC, created_at ASC 
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    )
                    DELETE FROM
                        swift_jobs.queues
                    WHERE job_id = (SELECT job_id FROM next_job)
                    RETURNING job_id
                    """,
                    logger: self.logger
                )
                // return nil if nothing in queue
                guard let jobID = try await stream.decode(UUID.self, context: .default).first(where: { _ in true }) else {
                    return .nothing
                }
                // set job status to processing
                try await self.setStatus(jobID: jobID, status: .processing, connection: connection)

                // select job from job table
                let stream2 = try await connection.query(
                    """
                    SELECT
                        job
                    FROM swift_jobs.jobs
                    WHERE id = \(jobID) AND queue_name = \(configuration.queueName)
                    """,
                    logger: self.logger
                )
                guard let row = try await stream2.first(where: { _ in true }) else {
                    logger.info(
                        "Failed to find job with id",
                        metadata: [
                            "JobID": "\(jobID)",
                            "Queue": "\(configuration.queueName)",
                        ]
                    )
                    // if failed to find the job in the job table return error
                    return .result(.failure(JobQueueError(code: .unrecognisedJobId, jobName: nil)), jobID: jobID)
                }
                return .result(.success(row), jobID: jobID)
            }

            switch popFirstResult {
            case .nothing:
                return nil
            case .result(let result, let jobID):
                do {
                    let row = try result.get()
                    let jobInstance = try row.decode(AnyDecodableJob.self, context: .withJobRegistry(self.jobRegistry)).job
                    return JobQueueResult(id: jobID, result: .success(jobInstance))
                } catch let error as JobQueueError {
                    return JobQueueResult(id: jobID, result: .failure(error))
                }
            }
        } catch let error as PSQLError {
            logger.info(
                "Failed to get job from queue",
                metadata: [
                    "Error": "\(String(reflecting: error))",
                    "Queue": "\(configuration.queueName)",
                ]
            )
            throw error
        } catch let error as JobQueueError {
            logger.info(
                "Job failed",
                metadata: [
                    "Error": "\(String(reflecting: error))",
                    "Queue": "\(configuration.queueName)",
                ]
            )
            throw error
        }
    }

    func add<Parameters>(jobID: JobID, jobRequest: JobRequest<Parameters>, queueName: String, connection: PostgresConnection) async throws {
        try await connection.query(
            """
            INSERT INTO swift_jobs.jobs (id, job, status, queue_name)
            VALUES (\(jobID), \(jobRequest), \(Status.pending), \(queueName))
            """,
            logger: self.logger
        )
    }
    // TODO: maybe add a new column colum for attempt so far after PR https://github.com/hummingbird-project/swift-jobs/pull/63 is merged?
    func updateJob(id: JobID, buffer: ByteBuffer, connection: PostgresConnection) async throws {
        try await connection.query(
            """
            UPDATE swift_jobs.jobs
            SET job = \(buffer),
                last_modified = \(Date.now),
                status = \(Status.failed)
            WHERE id = \(id) AND queue_name = \(configuration.queueName)
            """,
            logger: self.logger
        )
    }

    func delete(jobID: JobID) async throws {
        try await self.client.query(
            """
            DELETE FROM swift_jobs.jobs
            WHERE id = \(jobID) AND queue_name = \(configuration.queueName)
            """,
            logger: self.logger
        )
    }

    func deleteFromQueue(jobID: JobID, connection: PostgresConnection) async throws {
        try await connection.query(
            """
            DELETE FROM swift_jobs.queues
            WHERE job_id = \(jobID) AND queue_name = \(configuration.queueName)
            """,
            logger: self.logger
        )
    }

    func addToQueue(jobID: JobID, queueName: String, options: JobOptions, connection: PostgresConnection) async throws {
        try await connection.query(
            """
            INSERT INTO swift_jobs.queues (job_id, created_at, delayed_until, queue_name, priority)
            VALUES (\(jobID), \(Date.now), \(options.delayUntil), \(queueName), \(options.priority.rawValue))
            -- We have found an existing job with the same id, SKIP this INSERT 
            ON CONFLICT (job_id) DO NOTHING
            """,
            logger: self.logger
        )
    }

    func setStatus(jobID: JobID, status: Status, connection: PostgresConnection) async throws {
        try await connection.query(
            """
            UPDATE swift_jobs.jobs
            SET status = \(status),
                last_modified = \(Date.now)
            WHERE id = \(jobID) AND queue_name = \(configuration.queueName)
            """,
            logger: self.logger
        )
    }

    func setStatus(jobID: JobID, status: Status) async throws {
        try await self.client.query(
            """
            UPDATE swift_jobs.jobs
            SET status = \(status),
                last_modified = \(Date.now)
            WHERE id = \(jobID) AND queue_name = \(configuration.queueName)
            """,
            logger: self.logger
        )
    }

    func getJobs(withStatus status: Status) async throws -> [JobID] {
        let stream = try await self.client.query(
            """
            SELECT
                id
            FROM swift_jobs.jobs
            WHERE status = \(status) AND queue_name = \(configuration.queueName)
            """,
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
                """
                DELETE FROM swift_jobs.jobs
                WHERE status = \(status) AND queue_name = \(configuration.queueName)
                """,
                logger: self.logger
            )

        case .rerun:
            let jobs = try await getJobs(withStatus: status)
            self.logger.info("Moving \(jobs.count) jobs with status: \(status) to job queue")
            for jobID in jobs {
                try await self.addToQueue(jobID: jobID, queueName: configuration.queueName, options: .init(), connection: connection)
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

extension PostgresDecodingContext where JSONDecoder == Foundation.JSONDecoder {
    /// A ``PostgresDecodingContext`` that uses a Foundation `JSONDecoder` with job registry attached as userInfo.
    public static func withJobRegistry(_ jobRegistry: JobRegistry) -> PostgresDecodingContext {
        let jsonDecoder = JSONDecoder()
        jsonDecoder.userInfo[._jobConfiguration] = jobRegistry
        return PostgresDecodingContext(jsonDecoder: jsonDecoder)
    }
}
