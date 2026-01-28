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

import ExtrasBase64
import Foundation
import Jobs
import Logging
import NIOCore
import PostgresMigrations
import PostgresNIO

/// Parameters for Cleanup job
public struct PostgresJobCleanupParameters: Sendable & Codable {
    let completedJobs: PostgresJobQueue.JobCleanup
    let failedJobs: PostgresJobQueue.JobCleanup
    let cancelledJobs: PostgresJobQueue.JobCleanup
    let pausedJobs: PostgresJobQueue.JobCleanup

    ///  Initialize PostgresJobCleanupParameters
    /// - Parameters:
    ///   - completedJobs: What to do with completed jobs
    ///   - failedJobs: What to do with failed jobs
    ///   - cancelledJobs: What to do with cancelled jobs
    ///   - pausedJobs: What to do with paused jobs
    public init(
        completedJobs: PostgresJobQueue.JobCleanup = .doNothing,
        failedJobs: PostgresJobQueue.JobCleanup = .doNothing,
        cancelledJobs: PostgresJobQueue.JobCleanup = .doNothing,
        pausedJobs: PostgresJobQueue.JobCleanup = .doNothing
    ) {
        self.failedJobs = failedJobs
        self.completedJobs = completedJobs
        self.cancelledJobs = cancelledJobs
        self.pausedJobs = pausedJobs
    }
}

/// Parameters for cleanup of jobs stuck in processing
public struct PostgresProcessingJobCleanupParameters: Sendable, Codable {
    let maxJobsToProcess: Int

    ///  Initialize ValkeyProcessingJobCleanupParameters
    /// - Parameters:
    ///   - maxJobsToProcess: Maximum number of jobs to process in one go
    public init(maxJobsToProcess: Int) {
        self.maxJobsToProcess = maxJobsToProcess
    }
}

extension PostgresJobQueue {
    /// what to do with failed/processing jobs from last time queue was handled
    public struct JobCleanup: Sendable, Codable {
        enum RawValue: Codable {
            case doNothing
            case rerun
            case remove(maxAge: Duration?)
        }
        let rawValue: RawValue

        /// Do nothing to jobs
        public static var doNothing: Self { .init(rawValue: .doNothing) }
        /// Re-queue jobs for running again
        public static var rerun: Self { .init(rawValue: .rerun) }
        /// Delete jobs
        public static var remove: Self { .init(rawValue: .remove(maxAge: nil)) }
        /// Delete jobs that are older than maxAge
        public static func remove(maxAge: Duration) -> Self { .init(rawValue: .remove(maxAge: maxAge)) }
    }

    /// clean up job name.
    ///
    /// Use this with the ``/Jobs/JobSchedule`` to schedule a cleanup of
    /// failed, cancelled or completed jobs
    public var cleanupJob: JobName<PostgresJobCleanupParameters> {
        .init("_Jobs_PostgresCleanup_\(self.configuration.queueName)")
    }

    /// clean of hung processing jobs job name.
    ///
    /// Use this with the ``/Jobs/JobSchedule`` to schedule a cleanup hung processing jobs
    public var cleanupProcessingJob: JobName<PostgresProcessingJobCleanupParameters> {
        .init("_Jobs_ValkeyProcessingCleanup_\(self.configuration.queueName)")
    }

    /// register clean up job on queue
    func registerCleanupJobs() {
        self.registerJob(
            JobDefinition(name: cleanupJob, parameters: PostgresJobCleanupParameters.self, retryStrategy: .dontRetry) { parameters, context in
                try await self.cleanup(
                    pendingJobs: .doNothing,
                    processingJobs: .doNothing,
                    completedJobs: parameters.completedJobs,
                    failedJobs: parameters.failedJobs,
                    cancelledJobs: parameters.cancelledJobs,
                    pausedJobs: parameters.pausedJobs,
                    logger: self.logger
                )
            }
        )
        self.registerJob(
            JobDefinition(name: cleanupProcessingJob, retryStrategy: .dontRetry) { parameters, context in
                try await self.cleanupProcessingJobs(maxJobsToProcess: parameters.maxJobsToProcess)
            }
        )

    }

    ///  Cleanup job queues
    ///
    /// This function is used to re-run or delete jobs in a certain state. Failed, completed,
    /// cancelled and paused jobs can be pushed back into the pending queue to be re-run or removed.
    /// When called at startup in theory no job should be set to processing, or set to pending but
    /// not in the queue. but if your job server crashes these states are possible, so we also provide
    /// options to re-queue these jobs so they are run again.
    ///
    /// You can call `cleanup` with `failedJobs`, `completedJobs`, `cancelledJobs` or `pausedJobs` set
    /// to whatever you like at any point to re-queue failed jobs. Moving processing or pending jobs
    /// should only be done if you are certain there is nothing processing the job queue.
    ///
    /// - Parameters:
    ///   - pendingJobs: What to do with jobs tagged as pending
    ///   - processingJobs: What to do with jobs tagged as processing
    ///   - completedJobs: What to do with jobs tagged as completed
    ///   - failedJobs: What to do with jobs tagged as failed
    ///   - cancelledJobs: What to do with jobs tagged as cancelled
    ///   - pausedJobs: What to do with jobs tagged as paused
    ///   - logger: Optional logger to use when performing cleanup
    /// - Throws:
    public func cleanup(
        pendingJobs: JobCleanup = .doNothing,
        processingJobs: JobCleanup = .doNothing,
        completedJobs: JobCleanup = .doNothing,
        failedJobs: JobCleanup = .doNothing,
        cancelledJobs: JobCleanup = .doNothing,
        pausedJobs: JobCleanup = .doNothing,
        logger: Logger? = nil
    ) async throws {
        let logger = logger ?? self.logger
        do {
            /// wait for migrations to complete before running job queue cleanup
            try await self.migrations.waitUntilCompleted()
            _ = try await self.client.withTransaction(logger: logger) { connection in
                self.logger.info("Cleanup Jobs")
                try await self.updateJobsOnInit(withStatus: .pending, onInit: pendingJobs, connection: connection)
                try await self.updateJobsOnInit(withStatus: .processing, onInit: processingJobs, connection: connection)
                try await self.updateJobsOnInit(withStatus: .failed, onInit: failedJobs, connection: connection)
                try await self.updateJobsOnInit(withStatus: .completed, onInit: completedJobs, connection: connection)
                try await self.updateJobsOnInit(withStatus: .cancelled, onInit: cancelledJobs, connection: connection)
                try await self.updateJobsOnInit(withStatus: .paused, onInit: pausedJobs, connection: connection)
            }
        } catch let error as PSQLError {
            logger.error(
                "JobQueue cleanup failed",
                metadata: [
                    "Error": "\(String(reflecting: error))"
                ]
            )
            throw error
        }
    }

    /// Clean up jobs stuck in processing because their worker is no longer active
    public func cleanupProcessingJobs(maxJobsToProcess: Int) async throws {
        do {
            let bytes: [UInt8] = (0..<16).map { _ in UInt8.random(in: 0...255) }
            let lockID = ByteBuffer(string: Base64.encodeToString(bytes: bytes))
            var workersInactive: Set<String> = .init()

            let stream = try await self.client.query(
                """
                SELECT
                    id, worker_id
                FROM swift_jobs.jobs
                WHERE status = \(Status.processing) AND queue_name = \(configuration.queueName)
                FETCH FIRST \(maxJobsToProcess) ROWS ONLY
                """,
                logger: self.logger
            )
            for try await (id, workerID) in stream.decode((JobID, String?).self, context: .default) {
                guard let workerID else { continue }
                var inactive = workersInactive.contains(workerID)
                if !inactive {
                    inactive = try await self.acquireLock(key: .jobWorkerActiveLock(workerID: workerID), id: lockID, expiresIn: 10)
                }
                if inactive {
                    // we acquired the lock so the worker must have gone down, reschedule the job
                    workersInactive.insert(workerID)
                    self.logger.debug("Re-scheduling Job", metadata: ["JobID": .stringConvertible(id)])
                    try await self.client.withTransaction(logger: logger) { connection in
                        try await setStatus(jobID: id, status: .pending, connection: connection)
                        try await addToQueue(
                            jobID: id,
                            queueName: configuration.queueName,
                            options: .init(),
                            connection: connection
                        )
                    }
                }
            }
        } catch {
            self.logger.info("Cleanup of hung processing jobs failed: \(error)")
            throw error
        }
    }

    func updateJobsOnInit(withStatus status: Status, onInit: JobCleanup, connection: PostgresConnection) async throws {
        switch onInit.rawValue {
        case .remove(let olderThan):
            let date: Date =
                if let olderThan {
                    .now - Double(olderThan.components.seconds)
                } else {
                    .distantFuture
                }
            try await connection.query(
                """
                DELETE FROM swift_jobs.jobs
                WHERE status = \(status) AND queue_name = \(configuration.queueName)
                AND last_modified < \(date)
                """,
                logger: self.logger
            )

        case .rerun:
            let jobs = try await getJobs(withStatus: status, connection: connection)
            self.logger.info("Moving \(jobs.count) jobs with status: \(status) to job queue")
            for jobID in jobs {
                try await self.addToQueue(jobID: jobID, queueName: configuration.queueName, options: .init(), connection: connection)
            }

        case .doNothing:
            break
        }
    }
}
