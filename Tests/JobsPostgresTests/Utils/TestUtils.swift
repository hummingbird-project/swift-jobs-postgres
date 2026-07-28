//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import Foundation
import Jobs
import JobsPostgres
import PostgresMigrations
import PostgresNIO

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

func createPostgresJobDriver(
    configuration: PostgresJobQueue.Configuration = .init(),
    function: String = #function
) async -> PostgresJobQueue {
    let logger = {
        var logger = Logger(label: function)
        logger.logLevel = .debug
        return logger
    }()
    var configuration = configuration
    if configuration.queueName == "default" {
        configuration.queueName = function
    }
    let postgresClient = PostgresClient(
        configuration: getPostgresConfiguration(),
        backgroundLogger: logger
    )
    let postgresMigrations = DatabaseMigrations()
    return await .postgres(
        client: postgresClient,
        migrations: postgresMigrations,
        configuration: configuration,
        logger: logger
    )
}

func createJobQueue(
    configuration: PostgresJobQueue.Configuration = .init(),
    function: String = #function
) async throws -> JobQueue<PostgresJobQueue> {
    let postgresDriver = await createPostgresJobDriver(configuration: configuration, function: function)
    return JobQueue(
        postgresDriver,
        logger: postgresDriver.logger,
        options: .init(defaultRetryStrategy: .exponentialJitter(maxBackoff: .milliseconds(10)))
    )
}
