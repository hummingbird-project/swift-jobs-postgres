//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2021-2021 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Atomics
import Jobs
import NIOConcurrencyHelpers
import PostgresMigrations
import PostgresNIO
import ServiceLifecycle
import XCTest

@testable import JobsPostgres

func getPostgresConfiguration() async throws -> PostgresClient.Configuration {
    .init(
        host: ProcessInfo.processInfo.environment["POSTGRES_HOSTNAME"] ?? "localhost",
        port: 5432,
        username: ProcessInfo.processInfo.environment["POSTGRES_USER"] ?? "test_user",
        password: ProcessInfo.processInfo.environment["POSTGRES_PASSWORD"] ?? "test_password",
        database: ProcessInfo.processInfo.environment["POSTGRES_DB"] ?? "test_db",
        tls: .disable
    )
}

extension XCTestExpectation {
    convenience init(description: String, expectedFulfillmentCount: Int) {
        self.init(description: description)
        self.expectedFulfillmentCount = expectedFulfillmentCount
    }
}

final class JobsTests: XCTestCase {
    func createJobQueue(
        configuration: PostgresJobQueue.Configuration = .init(),
        function: String = #function
    ) async throws -> JobQueue<PostgresJobQueue> {
        let logger = {
            var logger = Logger(label: function)
            logger.logLevel = .debug
            return logger
        }()
        let postgresClient = try await PostgresClient(
            configuration: getPostgresConfiguration(),
            backgroundLogger: logger
        )
        let postgresMigrations = DatabaseMigrations()
        return await JobQueue(
            .postgres(
                client: postgresClient,
                migrations: postgresMigrations,
                configuration: configuration,
                logger: logger
            ),
            logger: logger,
            options: .init(defaultRetryStrategy: .exponentialJitter(maxBackoff: .milliseconds(10)))
        )
    }

    /// Helper function for test a server
    ///
    /// Creates test client, runs test function abd ensures everything is
    /// shutdown correctly
    @discardableResult public func testJobQueue<T>(
        jobQueue: JobQueue<PostgresJobQueue>,
        jobProcessorOptions: JobQueueProcessorOptions = .init(),
        failedJobsInitialization: PostgresJobQueue.JobCleanup = .remove,
        processingJobsInitialization: PostgresJobQueue.JobCleanup = .remove,
        pendingJobsInitialization: PostgresJobQueue.JobCleanup = .remove,
        revertMigrations: Bool = false,
        test: (JobQueue<PostgresJobQueue>) async throws -> T
    ) async throws -> T {
        do {
            return try await withThrowingTaskGroup(of: Void.self) { group in
                let serviceGroup = ServiceGroup(
                    configuration: .init(
                        services: [jobQueue.queue.client, jobQueue.processor(options: jobProcessorOptions)],
                        gracefulShutdownSignals: [.sigterm, .sigint],
                        logger: jobQueue.queue.logger
                    )
                )
                group.addTask {
                    try await serviceGroup.run()
                }
                do {
                    let migrations = jobQueue.queue.migrations
                    let client = jobQueue.queue.client
                    let logger = jobQueue.queue.logger
                    if revertMigrations {
                        try await migrations.revert(client: client, groups: [.jobQueue], logger: logger, dryRun: false)
                    }
                    try await migrations.apply(client: client, groups: [.jobQueue], logger: logger, dryRun: false)
                    try await jobQueue.queue.cleanup(failedJobs: failedJobsInitialization, processingJobs: processingJobsInitialization)
                    let value = try await test(jobQueue)
                    await serviceGroup.triggerGracefulShutdown()
                    return value
                } catch let error as PSQLError {
                    XCTFail("\(String(reflecting: error))")
                    await serviceGroup.triggerGracefulShutdown()
                    throw error
                } catch {
                    await serviceGroup.triggerGracefulShutdown()
                    throw error
                }
            }
        } catch let error as PSQLError {
            XCTFail("\(String(reflecting: error))")
            throw error
        }
    }

    /// Helper for testing job priority
    @discardableResult public func testPriorityJobQueue<T>(
        jobQueue: JobQueue<PostgresJobQueue>,
        failedJobsInitialization: PostgresJobQueue.JobCleanup = .remove,
        processingJobsInitialization: PostgresJobQueue.JobCleanup = .remove,
        pendingJobsInitialization: PostgresJobQueue.JobCleanup = .remove,
        revertMigrations: Bool = false,
        test: (JobQueue<PostgresJobQueue>) async throws -> T
    ) async throws -> T {
        do {
            return try await withThrowingTaskGroup(of: Void.self) { group in
                let serviceGroup = ServiceGroup(
                    configuration: .init(
                        services: [jobQueue.queue.client],
                        gracefulShutdownSignals: [.sigterm, .sigint],
                        logger: jobQueue.queue.logger
                    )
                )
                group.addTask {
                    try await serviceGroup.run()
                }
                do {
                    let migrations = jobQueue.queue.migrations
                    let client = jobQueue.queue.client
                    let logger = jobQueue.queue.logger
                    if revertMigrations {
                        try await migrations.revert(client: client, groups: [.jobQueue], logger: logger, dryRun: false)
                    }
                    try await migrations.apply(client: client, groups: [.jobQueue], logger: logger, dryRun: false)
                    try await jobQueue.queue.cleanup(failedJobs: failedJobsInitialization, processingJobs: processingJobsInitialization)
                    let value = try await test(jobQueue)
                    await serviceGroup.triggerGracefulShutdown()
                    return value
                } catch let error as PSQLError {
                    XCTFail("\(String(reflecting: error))")
                    await serviceGroup.triggerGracefulShutdown()
                    throw error
                } catch {
                    await serviceGroup.triggerGracefulShutdown()
                    throw error
                }
            }
        } catch let error as PSQLError {
            XCTFail("\(String(reflecting: error))")
            throw error
        }
    }

    /// Helper function for test a server
    ///
    /// Creates test client, runs test function abd ensures everything is
    /// shutdown correctly
    @discardableResult public func testJobQueue<T>(
        numWorkers: Int,
        failedJobsInitialization: PostgresJobQueue.JobCleanup = .remove,
        processingJobsInitialization: PostgresJobQueue.JobCleanup = .remove,
        pendingJobsInitialization: PostgresJobQueue.JobCleanup = .remove,
        revertMigrations: Bool = true,
        configuration: PostgresJobQueue.Configuration = .init(),
        function: String = #function,
        test: (JobQueue<PostgresJobQueue>) async throws -> T
    ) async throws -> T {
        let jobQueue = try await self.createJobQueue(
            configuration: configuration,
            function: function
        )
        return try await self.testJobQueue(
            jobQueue: jobQueue,
            jobProcessorOptions: .init(numWorkers: numWorkers),
            failedJobsInitialization: failedJobsInitialization,
            processingJobsInitialization: processingJobsInitialization,
            pendingJobsInitialization: pendingJobsInitialization,
            revertMigrations: revertMigrations,
            test: test
        )
    }

    func testBasic() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testBasic"
            let value: Int
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 10)
        try await self.testJobQueue(numWorkers: 1) { jobQueue in
            jobQueue.registerJob(parameters: TestParameters.self) { parameters, context in
                context.logger.info("Parameters=\(parameters.value)")
                try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
                expectation.fulfill()
            }
            try await jobQueue.push(TestParameters(value: 1))
            try await jobQueue.push(TestParameters(value: 2))
            try await jobQueue.push(TestParameters(value: 3))
            try await jobQueue.push(TestParameters(value: 4))
            try await jobQueue.push(TestParameters(value: 5))
            try await jobQueue.push(TestParameters(value: 6))
            try await jobQueue.push(TestParameters(value: 7))
            try await jobQueue.push(TestParameters(value: 8))
            try await jobQueue.push(TestParameters(value: 9))
            try await jobQueue.push(TestParameters(value: 10))

            await fulfillment(of: [expectation], timeout: 5)
        }
    }

    func testDelayedJobs() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testDelayedJobs"
            let value: Int
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 2)
        let jobExecutionSequence: NIOLockedValueBox<[Int]> = .init([])

        try await self.testJobQueue(numWorkers: 1) { jobQueue in
            jobQueue.registerJob(parameters: TestParameters.self) { parameters, context in
                context.logger.info("Parameters=\(parameters.value)")
                jobExecutionSequence.withLockedValue {
                    $0.append(parameters.value)
                }
                try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
                expectation.fulfill()
            }
            try await jobQueue.push(
                TestParameters(value: 1),
                options: .init(
                    delayUntil: Date.now.addingTimeInterval(1)
                )
            )
            try await jobQueue.push(TestParameters(value: 5))

            let processingJobs = try await jobQueue.queue.getJobs(withStatus: .pending)
            XCTAssertEqual(processingJobs.count, 2)

            await fulfillment(of: [expectation], timeout: 10)

            let pendingJobs = try await jobQueue.queue.getJobs(withStatus: .pending)
            XCTAssertEqual(pendingJobs.count, 0)
        }
        XCTAssertEqual(jobExecutionSequence.withLockedValue { $0 }, [5, 1])
    }

    func testJobPriorities() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testPriorityJobs"
            let value: Int
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 2)
        let jobExecutionSequence: NIOLockedValueBox<[Int]> = .init([])

        let jobQueue = try await self.createJobQueue(configuration: .init(), function: #function)

        try await testPriorityJobQueue(jobQueue: jobQueue) { queue in
            queue.registerJob(parameters: TestParameters.self) { parameters, context in
                context.logger.info("Parameters=\(parameters.value)")
                jobExecutionSequence.withLockedValue {
                    $0.append(parameters.value)
                }
                try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
                expectation.fulfill()
            }

            try await queue.push(
                TestParameters(value: 20),
                options: .init(
                    priority: .lowest
                )
            )

            try await queue.push(
                TestParameters(value: 2025),
                options: .init(
                    priority: .highest
                )
            )

            try await withThrowingTaskGroup(of: Void.self) { group in
                let serviceGroup = ServiceGroup(services: [queue.processor()], logger: queue.logger)

                let processingJobs = try await jobQueue.queue.getJobs(withStatus: .pending)
                XCTAssertEqual(processingJobs.count, 2)

                group.addTask {
                    try await serviceGroup.run()
                }

                await fulfillment(of: [expectation], timeout: 10)

                let pendingJobs = try await jobQueue.queue.getJobs(withStatus: .pending)
                XCTAssertEqual(pendingJobs.count, 0)
                await serviceGroup.triggerGracefulShutdown()
            }
        }
        XCTAssertEqual(jobExecutionSequence.withLockedValue { $0 }, [2025, 20])
    }

    func testJobPrioritiesWithDelay() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testPriorityJobsWithDelay"
            let value: Int
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 2)
        let jobExecutionSequence: NIOLockedValueBox<[Int]> = .init([])

        let jobQueue = try await self.createJobQueue(configuration: .init(), function: #function)

        try await testPriorityJobQueue(jobQueue: jobQueue) { queue in
            queue.registerJob(parameters: TestParameters.self) { parameters, context in
                context.logger.info("Parameters=\(parameters.value)")
                jobExecutionSequence.withLockedValue {
                    $0.append(parameters.value)
                }
                try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
                expectation.fulfill()
            }

            try await queue.push(
                TestParameters(value: 20),
                options: .init(
                    priority: .lower
                )
            )

            try await queue.push(
                TestParameters(value: 2025),
                options: .init(
                    delayUntil: Date.now.addingTimeInterval(1),
                    priority: .higher
                )
            )

            try await withThrowingTaskGroup(of: Void.self) { group in
                let serviceGroup = ServiceGroup(services: [queue.processor()], logger: queue.logger)

                let processingJobs = try await jobQueue.queue.getJobs(withStatus: .pending)
                XCTAssertEqual(processingJobs.count, 2)

                group.addTask {
                    try await serviceGroup.run()
                }

                await fulfillment(of: [expectation], timeout: 10)

                let pendingJobs = try await jobQueue.queue.getJobs(withStatus: .pending)
                XCTAssertEqual(pendingJobs.count, 0)
                await serviceGroup.triggerGracefulShutdown()
            }
        }
        XCTAssertEqual(jobExecutionSequence.withLockedValue { $0 }, [20, 2025])
    }

    func testMultipleWorkers() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testMultipleWorkers"
            let value: Int
        }
        let runningJobCounter = ManagedAtomic(0)
        let maxRunningJobCounter = ManagedAtomic(0)
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 10)

        try await self.testJobQueue(numWorkers: 4) { jobQueue in
            jobQueue.registerJob(parameters: TestParameters.self) { parameters, context in
                let runningJobs = runningJobCounter.wrappingIncrementThenLoad(by: 1, ordering: .relaxed)
                if runningJobs > maxRunningJobCounter.load(ordering: .relaxed) {
                    maxRunningJobCounter.store(runningJobs, ordering: .relaxed)
                }
                try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
                context.logger.info("Parameters=\(parameters)")
                expectation.fulfill()
                runningJobCounter.wrappingDecrement(by: 1, ordering: .relaxed)
            }

            try await jobQueue.push(TestParameters(value: 1))
            try await jobQueue.push(TestParameters(value: 2))
            try await jobQueue.push(TestParameters(value: 3))
            try await jobQueue.push(TestParameters(value: 4))
            try await jobQueue.push(TestParameters(value: 5))
            try await jobQueue.push(TestParameters(value: 6))
            try await jobQueue.push(TestParameters(value: 7))
            try await jobQueue.push(TestParameters(value: 8))
            try await jobQueue.push(TestParameters(value: 9))
            try await jobQueue.push(TestParameters(value: 10))

            await fulfillment(of: [expectation], timeout: 5)

            XCTAssertGreaterThan(maxRunningJobCounter.load(ordering: .relaxed), 1)
            XCTAssertLessThanOrEqual(maxRunningJobCounter.load(ordering: .relaxed), 4)
        }
    }

    func testErrorRetryCount() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testErrorRetryCount"
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 3)
        struct FailedError: Error {}
        try await self.testJobQueue(numWorkers: 1) { jobQueue in
            jobQueue.registerJob(
                parameters: TestParameters.self,
                retryStrategy: .exponentialJitter(maxAttempts: 3, maxBackoff: .milliseconds(10))
            ) { _, _ in
                expectation.fulfill()
                throw FailedError()
            }
            try await jobQueue.push(TestParameters())

            await fulfillment(of: [expectation], timeout: 5)
            try await Task.sleep(for: .milliseconds(200))

            let failedJobs = try await jobQueue.queue.getJobs(withStatus: .failed)
            XCTAssertEqual(failedJobs.count, 1)
            let pendingJobs = try await jobQueue.queue.getJobs(withStatus: .pending)
            XCTAssertEqual(pendingJobs.count, 0)
        }
    }

    func testErrorRetryAndThenSucceed() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testErrorRetryAndThenSucceed"
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 2)
        let currentJobTryCount: NIOLockedValueBox<Int> = .init(0)
        struct FailedError: Error {}
        try await self.testJobQueue(numWorkers: 1) { jobQueue in
            jobQueue.registerJob(
                parameters: TestParameters.self,
                retryStrategy: .exponentialJitter(maxAttempts: 3, maxBackoff: .milliseconds(10))
            ) { _, _ in
                defer {
                    currentJobTryCount.withLockedValue {
                        $0 += 1
                    }
                }
                expectation.fulfill()
                if (currentJobTryCount.withLockedValue { $0 }) == 0 {
                    throw FailedError()
                }
            }
            try await jobQueue.push(TestParameters())

            await fulfillment(of: [expectation], timeout: 5)
            try await Task.sleep(for: .milliseconds(200))

            let failedJobs = try await jobQueue.queue.getJobs(withStatus: .failed)
            XCTAssertEqual(failedJobs.count, 0)
            let pendingJobs = try await jobQueue.queue.getJobs(withStatus: .pending)
            XCTAssertEqual(pendingJobs.count, 0)
        }
        XCTAssertEqual(currentJobTryCount.withLockedValue { $0 }, 2)
    }

    func testJobSerialization() async throws {
        struct TestJobParameters: JobParameters {
            static let jobName = "testJobSerialization"
            let id: Int
            let message: String
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called")
        try await self.testJobQueue(numWorkers: 1) { jobQueue in
            jobQueue.registerJob(parameters: TestJobParameters.self) { parameters, _ in
                XCTAssertEqual(parameters.id, 23)
                XCTAssertEqual(parameters.message, "Hello!")
                expectation.fulfill()
            }
            try await jobQueue.push(TestJobParameters(id: 23, message: "Hello!"))

            await fulfillment(of: [expectation], timeout: 5)
        }
    }

    /// Test job is cancelled on shutdown
    func testShutdownJob() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testShutdownJob"
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 1)

        try await self.testJobQueue(
            numWorkers: 4,
            configuration: .init(
                retentionPolicy: .init(
                    cancelled: .doNotRetain,
                    completed: .doNotRetain,
                    failed: .doNotRetain
                )
            )
        ) { jobQueue in
            jobQueue.registerJob(parameters: TestParameters.self) { _, _ in
                expectation.fulfill()
                try await Task.sleep(for: .milliseconds(1000))
            }
            try await jobQueue.push(TestParameters())
            await fulfillment(of: [expectation], timeout: 5)

            let processingJobs = try await jobQueue.queue.getJobs(withStatus: .processing)
            XCTAssertEqual(processingJobs.count, 1)
            let pendingJobs = try await jobQueue.queue.getJobs(withStatus: .pending)
            XCTAssertEqual(pendingJobs.count, 0)
            return jobQueue
        }
    }

    /// test job fails to decode but queue continues to process
    func testFailToDecode() async throws {
        struct TestIntParameter: JobParameters {
            static let jobName = "testFailToDecode"
            let value: Int
        }
        struct TestStringParameter: JobParameters {
            static let jobName = "testFailToDecode"
            let value: String
        }
        let string: NIOLockedValueBox<String> = .init("")
        let expectation = XCTestExpectation(description: "job was called", expectedFulfillmentCount: 1)

        try await self.testJobQueue(numWorkers: 4) { jobQueue in
            jobQueue.registerJob(parameters: TestStringParameter.self) { parameters, _ in
                string.withLockedValue { $0 = parameters.value }
                expectation.fulfill()
            }
            try await jobQueue.push(TestIntParameter(value: 2))
            try await jobQueue.push(TestStringParameter(value: "test"))
            await fulfillment(of: [expectation], timeout: 5)
        }
        string.withLockedValue {
            XCTAssertEqual($0, "test")
        }
    }

    /// creates job that errors on first attempt, and is left on processing queue and
    /// is then rerun on startup of new server
    func testRerunAtStartup() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testRerunAtStartup"
        }
        struct RetryError: Error {}
        let firstTime = ManagedAtomic(true)
        let finished = ManagedAtomic(false)
        let failedExpectation = XCTestExpectation(description: "TestJob failed", expectedFulfillmentCount: 1)
        let succeededExpectation = XCTestExpectation(description: "TestJob2 succeeded", expectedFulfillmentCount: 1)
        let job = JobDefinition(parameters: TestParameters.self) { _, _ in
            if firstTime.compareExchange(expected: true, desired: false, ordering: .relaxed).original {
                failedExpectation.fulfill()
                throw RetryError()
            }
            succeededExpectation.fulfill()
            finished.store(true, ordering: .relaxed)
        }
        let jobQueue = try await createJobQueue()
        jobQueue.registerJob(job)
        try await self.testJobQueue(
            jobQueue: jobQueue,
            revertMigrations: true
        ) { jobQueue in
            try await jobQueue.push(TestParameters())

            await fulfillment(of: [failedExpectation], timeout: 10)

            XCTAssertFalse(firstTime.load(ordering: .relaxed))
            XCTAssertFalse(finished.load(ordering: .relaxed))
        }

        let jobQueue2 = try await createJobQueue()
        jobQueue2.registerJob(job)
        try await self.testJobQueue(jobQueue: jobQueue2, failedJobsInitialization: .rerun) { _ in
            await fulfillment(of: [succeededExpectation], timeout: 10)
            XCTAssertTrue(finished.load(ordering: .relaxed))
        }
    }

    func testMultipleJobQueueHandlers() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testMultipleJobQueueHandlers"
            let value: Int
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 200)
        let logger = {
            var logger = Logger(label: "testMultipleJobQueueHandlers")
            logger.logLevel = .debug
            return logger
        }()
        let job = JobDefinition(parameters: TestParameters.self) { parameters, context in
            context.logger.info("Parameters=\(parameters.value)")
            try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
            expectation.fulfill()
        }
        let postgresClient = try await PostgresClient(
            configuration: getPostgresConfiguration(),
            backgroundLogger: logger
        )
        let postgresMigrations = DatabaseMigrations()
        let jobQueue = await JobQueue(
            .postgres(
                client: postgresClient,
                migrations: postgresMigrations,
                logger: logger
            ),
            logger: logger
        )
        let postgresMigrations2 = DatabaseMigrations()
        let jobQueue2 = await JobQueue(
            .postgres(
                client: postgresClient,
                migrations: postgresMigrations2,
                configuration: .init(
                    queueName: "job_queue_2"
                ),
                logger: logger
            ),
            logger: logger
        )
        jobQueue.registerJob(job)
        jobQueue2.registerJob(job)

        try await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = ServiceGroup(
                configuration: .init(
                    services: [
                        postgresClient,
                        jobQueue.processor(options: .init(numWorkers: 2)),
                        jobQueue2.processor(options: .init(numWorkers: 2)),
                    ],
                    gracefulShutdownSignals: [.sigterm, .sigint],
                    logger: logger
                )
            )
            group.addTask {
                try await serviceGroup.run()
            }
            try await postgresMigrations.apply(client: postgresClient, groups: [.jobQueue], logger: logger, dryRun: false)
            try await postgresMigrations2.apply(client: postgresClient, groups: [.jobQueue], logger: logger, dryRun: false)
            try await jobQueue.queue.cleanup(failedJobs: .remove, processingJobs: .remove)
            try await jobQueue2.queue.cleanup(failedJobs: .remove, processingJobs: .remove)
            do {
                for i in 0..<200 {
                    try await jobQueue.push(TestParameters(value: i))
                    try await jobQueue2.push(TestParameters(value: i))
                }
                await fulfillment(of: [expectation], timeout: 5)
                await serviceGroup.triggerGracefulShutdown()
            } catch {
                XCTFail("\(String(reflecting: error))")
                await serviceGroup.triggerGracefulShutdown()
                throw error
            }
        }
    }

    func testMetadata() async throws {
        let logger = Logger(label: "testMetadata")
        try await withThrowingTaskGroup(of: Void.self) { group in
            let postgresClient = try await PostgresClient(
                configuration: getPostgresConfiguration(),
                backgroundLogger: logger
            )
            group.addTask {
                await postgresClient.run()
            }
            let postgresMigrations = DatabaseMigrations()
            let jobQueue = await PostgresJobQueue(
                client: postgresClient,
                migrations: postgresMigrations,
                logger: logger
            )
            try await postgresMigrations.apply(client: postgresClient, groups: [.jobQueue], logger: logger, dryRun: false)

            let value = ByteBuffer(string: "Testing metadata")
            try await jobQueue.setMetadata(key: "test", value: value)
            let metadata = try await jobQueue.getMetadata("test")
            XCTAssertEqual(metadata, value)
            let value2 = ByteBuffer(string: "Testing metadata again")
            try await jobQueue.setMetadata(key: "test", value: value2)
            let metadata2 = try await jobQueue.getMetadata("test")
            XCTAssertEqual(metadata2, value2)

            // cancel postgres client task
            group.cancelAll()
        }
    }

    func testResumableAndPausableJobs() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "TestJob"
        }
        struct ResumableJob: JobParameters {
            static let jobName = "ResumanableJob"
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 2)
        let didResumableJobRun: NIOLockedValueBox<Bool> = .init(false)
        let didTestJobRun: NIOLockedValueBox<Bool> = .init(false)

        let jobQueue = try await self.createJobQueue(configuration: .init(), function: #function)

        try await testPriorityJobQueue(jobQueue: jobQueue) { queue in
            queue.registerJob(parameters: TestParameters.self) { parameters, _ in
                didTestJobRun.withLockedValue {
                    $0 = true
                }
                try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
                expectation.fulfill()
            }

            queue.registerJob(parameters: ResumableJob.self) { parameters, _ in
                didResumableJobRun.withLockedValue {
                    $0 = true
                }
                try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
                expectation.fulfill()
            }

            let resumableJob = try await queue.push(
                ResumableJob(),
                options: .init(
                    priority: .lowest
                )
            )

            try await queue.push(
                TestParameters(),
                options: .init(
                    priority: .normal
                )
            )

            try await jobQueue.pauseJob(jobID: resumableJob)

            try await withThrowingTaskGroup(of: Void.self) { group in
                let serviceGroup = ServiceGroup(services: [queue.processor()], logger: queue.logger)

                let processingJobs = try await jobQueue.queue.getJobs(withStatus: .pending)
                XCTAssertEqual(processingJobs.count, 1)

                group.addTask {
                    try await serviceGroup.run()
                }

                let processingJobCount = try await jobQueue.queue.getJobs(withStatus: .processing)
                XCTAssertEqual(processingJobCount.count, 0)

                let pausedJobs = try await jobQueue.queue.getJobs(withStatus: .paused)
                XCTAssertEqual(pausedJobs.count, 1)

                try await jobQueue.resumeJob(jobID: resumableJob)

                await fulfillment(of: [expectation], timeout: 10)
                await serviceGroup.triggerGracefulShutdown()
            }
        }
        XCTAssertEqual(didTestJobRun.withLockedValue { $0 }, true)
        XCTAssertEqual(didResumableJobRun.withLockedValue { $0 }, true)
    }

    func testCancellableJob() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testCancellableJob"
            let value: Int
        }
        struct NoneCancelledJobParameters: JobParameters {
            static let jobName = "NoneCancelledJob"
            let value: Int
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 1)
        let didRunCancelledJob: NIOLockedValueBox<Bool> = .init(false)
        let didRunNoneCancelledJob: NIOLockedValueBox<Bool> = .init(false)

        let jobQueue = try await self.createJobQueue(
            configuration: .init(
                retentionPolicy: .init(
                    cancelled: .doNotRetain,
                    completed: .doNotRetain,
                    failed: .doNotRetain
                )
            ),
            function: #function
        )

        try await testPriorityJobQueue(jobQueue: jobQueue) { queue in
            queue.registerJob(parameters: TestParameters.self) { parameters, context in
                context.logger.info("Parameters=\(parameters.value)")
                didRunCancelledJob.withLockedValue {
                    $0 = true
                }
                try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
                expectation.fulfill()
            }

            queue.registerJob(parameters: NoneCancelledJobParameters.self) { parameters, context in
                context.logger.info("Parameters=\(parameters.value)")
                didRunNoneCancelledJob.withLockedValue {
                    $0 = true
                }
                try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
                expectation.fulfill()
            }

            let cancellableJob = try await queue.push(
                TestParameters(value: 42),
                options: .init(
                    priority: .lower
                )
            )

            try await queue.push(
                NoneCancelledJobParameters(value: 2025),
                options: .init(
                    priority: .highest
                )
            )

            try await jobQueue.cancelJob(jobID: cancellableJob)

            try await withThrowingTaskGroup(of: Void.self) { group in
                let serviceGroup = ServiceGroup(services: [queue.processor()], logger: queue.logger)

                let processingJobs = try await jobQueue.queue.getJobs(withStatus: .pending)
                XCTAssertEqual(processingJobs.count, 1)

                group.addTask {
                    try await serviceGroup.run()
                }

                await fulfillment(of: [expectation], timeout: 10)
                // Jobs has been removed
                let cancelledJobs = try await jobQueue.queue.getJobs(withStatus: .cancelled)
                XCTAssertEqual(cancelledJobs.count, 0)

                await serviceGroup.triggerGracefulShutdown()
            }
        }
        XCTAssertEqual(didRunCancelledJob.withLockedValue { $0 }, false)
        XCTAssertEqual(didRunNoneCancelledJob.withLockedValue { $0 }, true)
    }

    func testCompletedJobRetention() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testCompletedJobRetention"
            let value: Int
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 3)
        try await self.testJobQueue(
            numWorkers: 1,
            configuration: .init(retentionPolicy: .init(completed: .retain))
        ) { jobQueue in
            jobQueue.registerJob(parameters: TestParameters.self) { parameters, context in
                context.logger.info("Parameters=\(parameters.value)")
                expectation.fulfill()
            }
            try await jobQueue.push(TestParameters(value: 1))
            try await jobQueue.push(TestParameters(value: 2))
            try await jobQueue.push(TestParameters(value: 3))

            await fulfillment(of: [expectation], timeout: 10)
            try await Task.sleep(for: .milliseconds(200))

            let completedJobs = try await jobQueue.queue.getJobs(withStatus: .completed)
            XCTAssertEqual(completedJobs.count, 3)
            try await jobQueue.queue.cleanup(completedJobs: .remove(maxAge: .seconds(10)))
            XCTAssertEqual(completedJobs.count, 3)
            try await jobQueue.queue.cleanup(completedJobs: .remove(maxAge: .seconds(0)))
            let zeroJobs = try await jobQueue.queue.getJobs(withStatus: .completed)
            XCTAssertEqual(zeroJobs.count, 0)
        }
    }

    func testCancelledJobRetention() async throws {
        let jobQueue = try await self.createJobQueue(
            configuration: .init(retentionPolicy: .init(cancelled: .retain))
        )
        let jobName = JobName<Int>("testCancelledJobRetention")
        jobQueue.registerJob(name: jobName) { _, _ in }

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                // run postgres client
                await jobQueue.queue.client.run()
            }
            try await jobQueue.queue.migrations.apply(client: jobQueue.queue.client, logger: jobQueue.logger, dryRun: false)

            let jobId = try await jobQueue.push(jobName, parameters: 1)
            let jobId2 = try await jobQueue.push(jobName, parameters: 2)

            try await jobQueue.cancelJob(jobID: jobId)
            try await jobQueue.cancelJob(jobID: jobId2)

            var cancelledJobs = try await jobQueue.queue.getJobs(withStatus: .cancelled)
            XCTAssertEqual(cancelledJobs.count, 2)
            try await jobQueue.queue.cleanup(cancelledJobs: .remove(maxAge: .seconds(0)))
            cancelledJobs = try await jobQueue.queue.getJobs(withStatus: .cancelled)
            XCTAssertEqual(cancelledJobs.count, 0)

            group.cancelAll()
        }
    }

    func testCleanupProcessingJobs() async throws {
        let jobQueue = try await self.createJobQueue()
        let jobName = JobName<Int>("testCancelledJobRetention")
        jobQueue.registerJob(name: jobName) { _, _ in }

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                // run postgres client
                await jobQueue.queue.client.run()
            }
            try await jobQueue.queue.migrations.apply(client: jobQueue.queue.client, logger: jobQueue.logger, dryRun: false)

            let jobID = try await jobQueue.push(jobName, parameters: 1)
            let job = try await jobQueue.queue.popFirst()
            XCTAssertEqual(jobID, job?.id)
            _ = try await jobQueue.push(jobName, parameters: 1)
            _ = try await jobQueue.queue.popFirst()

            var processingJobs = try await jobQueue.queue.getJobs(withStatus: .processing)
            XCTAssertEqual(processingJobs.count, 2)

            try await jobQueue.queue.cleanup(processingJobs: .remove)

            processingJobs = try await jobQueue.queue.getJobs(withStatus: .processing)
            XCTAssertEqual(processingJobs.count, 0)

            group.cancelAll()
        }
    }

    func testCleanupJob() async throws {
        try await self.testJobQueue(
            numWorkers: 1,
            configuration: .init(retentionPolicy: .init(failed: .retain))
        ) { jobQueue in
            try await self.testJobQueue(
                numWorkers: 1,
                configuration: .init(
                    queueName: "SecondQueue",
                    retentionPolicy: .init(failed: .retain)
                )
            ) { jobQueue2 in
                let (stream, cont) = AsyncStream.makeStream(of: Void.self)
                var iterator = stream.makeAsyncIterator()
                struct TempJob: Sendable & Codable {}
                let barrierJobName = JobName<TempJob>("barrier")
                jobQueue.registerJob(name: "testCleanupJob", parameters: String.self) { parameters, context in
                    throw CancellationError()
                }
                jobQueue.registerJob(name: barrierJobName, parameters: TempJob.self) { parameters, context in
                    cont.yield()
                }
                jobQueue2.registerJob(name: "testCleanupJob", parameters: String.self) { parameters, context in
                    throw CancellationError()
                }
                jobQueue2.registerJob(name: barrierJobName, parameters: TempJob.self) { parameters, context in
                    cont.yield()
                }
                try await jobQueue.push("testCleanupJob", parameters: "1")
                try await jobQueue.push("testCleanupJob", parameters: "2")
                try await jobQueue.push("testCleanupJob", parameters: "3")
                try await jobQueue.push(barrierJobName, parameters: .init())
                try await jobQueue2.push("testCleanupJob", parameters: "1")
                try await jobQueue2.push(barrierJobName, parameters: .init())

                await iterator.next()
                await iterator.next()

                let failedJob = try await jobQueue.queue.getJobs(withStatus: .failed)
                XCTAssertEqual(failedJob.count, 3)
                try await jobQueue.push(jobQueue.queue.cleanupJob, parameters: .init(failedJobs: .remove))
                try await jobQueue.push(barrierJobName, parameters: .init())

                await iterator.next()

                let zeroJobs = try await jobQueue.queue.getJobs(withStatus: .failed)
                XCTAssertEqual(zeroJobs.count, 0)
                let jobCount2 = try await jobQueue2.queue.getJobs(withStatus: .failed)
                XCTAssertEqual(jobCount2.count, 1)
            }
        }
    }
}
