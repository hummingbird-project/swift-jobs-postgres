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

extension PostgresJobQueue {
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
    ///   - completedJobs: What to do with jobs tagged as completed
    ///   - cancelledJobs: What to do with jobs tagged as cancelled
    ///   - logger: Optional logger to use when performing cleanup
    /// - Throws:
    public func cleanup(
        failedJobs: JobCleanup = .doNothing,
        processingJobs: JobCleanup = .doNothing,
        pendingJobs: JobCleanup = .doNothing,
        completedJobs: JobCleanup = .doNothing,
        cancelledJobs: JobCleanup = .doNothing,
        logger: Logger? = nil
    ) async throws {
        let logger = logger ?? self.logger
        do {
            /// wait for migrations to complete before running job queue cleanup
            try await self.migrations.waitUntilCompleted()
            _ = try await self.client.withTransaction(logger: logger) { connection in
                self.logger.info("Update Jobs")
                try await self.updateJobsOnInit(withStatus: .pending, onInit: pendingJobs, connection: connection)
                try await self.updateJobsOnInit(withStatus: .processing, onInit: processingJobs, connection: connection)
                try await self.updateJobsOnInit(withStatus: .failed, onInit: failedJobs, connection: connection)
                try await self.updateJobsOnInit(withStatus: .completed, onInit: completedJobs, connection: connection)
                try await self.updateJobsOnInit(withStatus: .cancelled, onInit: cancelledJobs, connection: connection)
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

    func updateJobsOnInit(withStatus status: Status, onInit: JobCleanup, connection: PostgresConnection) async throws {
        switch onInit.rawValue {
        case .remove(let olderThan):
            let date: Date =
                if let olderThan {
                    .now - Double(olderThan.components.seconds)
                } else {
                    .distantPast
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
            let jobs = try await getJobs(withStatus: status)
            self.logger.info("Moving \(jobs.count) jobs with status: \(status) to job queue")
            for jobID in jobs {
                try await self.addToQueue(jobID: jobID, queueName: configuration.queueName, options: .init(), connection: connection)
            }

        case .doNothing:
            break
        }
    }
}
