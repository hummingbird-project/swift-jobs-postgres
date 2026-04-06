import Foundation
import Jobs
import JobsPostgres
import Logging
import PostgresMigrations
import PostgresNIO
import ServiceLifecycle

func getPostgresConfiguration() -> PostgresClient.Configuration {
    .init(
        host: ProcessInfo.processInfo.environment["POSTGRES_HOSTNAME"] ?? "localhost",
        port: 5432,
        username: ProcessInfo.processInfo.environment["POSTGRES_USER"] ?? "test_user",
        password: ProcessInfo.processInfo.environment["POSTGRES_PASSWORD"] ?? "test_password",
        database: ProcessInfo.processInfo.environment["POSTGRES_DB"] ?? "test_db",
        tls: .disable
    )
}

struct TestParameters: JobParameters {
    static let jobName = "testMultipleJobQueueHandlers"
    let value: Int
}
let logger = {
    var logger = Logger(label: "testMultipleJobQueueHandlers")
    logger.logLevel = .info
    return logger
}()
let (stream, cont) = AsyncStream.makeStream(of: Void.self)
let job = JobDefinition(parameters: TestParameters.self) { parameters, context in
    //try await Task.sleep(for: .milliseconds(parameters.value))
    cont.yield()
}
let postgresClient = PostgresClient(
    configuration: getPostgresConfiguration(),
    backgroundLogger: logger
)
let postgresMigrations = DatabaseMigrations()
let jobQueue = await JobQueue(
    .postgres(
        client: postgresClient,
        migrations: postgresMigrations,
        configuration: .init(
            queueName: "testMultipleJobQueueHandlers"
        ),
        logger: logger
    ),
    logger: logger
)
jobQueue.registerJob(job)

try await withThrowingTaskGroup(of: Void.self) { group in
    let serviceGroup = ServiceGroup(
        configuration: .init(
            services: [
                postgresClient,
                jobQueue.processor(options: .init(numWorkers: 4)),
            ],
            gracefulShutdownSignals: [.sigterm, .sigint],
            logger: logger
        )
    )
    group.addTask {
        try await serviceGroup.run()
    }
    try await postgresMigrations.apply(client: postgresClient, groups: [.jobQueue], logger: logger, dryRun: false)
    try await jobQueue.queue.cleanup(processingJobs: .remove, failedJobs: .remove)
    do {
        let time = Date.now
        for _ in 0..<1000 {
            try await jobQueue.push(TestParameters(value: Int.random(in: 1..<5)))
        }
        var iterator = stream.makeAsyncIterator()
        for _ in 0..<1000 {
            await iterator.next()
        }
        print("Time: \(-time.timeIntervalSinceNow)")
        await serviceGroup.triggerGracefulShutdown()
    } catch {
        //Issue.record("\(String(reflecting: error))")
        await serviceGroup.triggerGracefulShutdown()
        throw error
    }
}
