//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2025 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Jobs

public struct JobCleanupParameters: JobParameters {
    static public var jobName: String { "_JobCleanup_" }

    let jobQueueName: String
    let failedJobs: PostgresJobQueue.JobCleanup
    let completedJobs: PostgresJobQueue.JobCleanup
    let cancelledJobs: PostgresJobQueue.JobCleanup

    public init(
        jobQueueName: String,
        failedJobs: PostgresJobQueue.JobCleanup = .doNothing,
        completedJobs: PostgresJobQueue.JobCleanup = .doNothing,
        cancelledJobs: PostgresJobQueue.JobCleanup = .doNothing
    ) {
        self.jobQueueName = jobQueueName
        self.failedJobs = failedJobs
        self.completedJobs = completedJobs
        self.cancelledJobs = cancelledJobs
    }
}
