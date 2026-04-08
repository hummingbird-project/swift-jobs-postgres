//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import JobsPostgres
import Logging
import PostgresMigrations
import PostgresNIO
import Testing

final class PostgresMigrations: SuiteTrait, TestScoping {
    #if compiler(>=6.2)
    typealias ScopeFunction = @Sendable @concurrent () async throws -> Void
    #else
    typealias ScopeFunction = @Sendable () async throws -> Void
    #endif
    let postgresConfiguration: PostgresClient.Configuration

    init(postgresConfiguration: PostgresClient.Configuration) {
        self.postgresConfiguration = postgresConfiguration
    }

    func provideScope(for test: Test, testCase: Test.Case?, performing function: ScopeFunction) async throws {
        var logger = Logger(label: "PostgresMigrations")
        logger.logLevel = .debug
        let postgresClient = PostgresClient(
            configuration: self.postgresConfiguration,
            backgroundLogger: logger
        )
        let postgresMigrations = DatabaseMigrations()
        await PostgresJobQueue.addMigrations(to: postgresMigrations)
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                await postgresClient.run()
            }
            try await postgresMigrations.revert(client: postgresClient, groups: [.jobQueue], logger: logger, dryRun: false)
            try await postgresMigrations.apply(client: postgresClient, groups: [.jobQueue], logger: logger, dryRun: false)
            group.cancelAll()
        }
        try await function()
    }
}
extension SuiteTrait where Self == PostgresMigrations {
    static func postgresMigrations(configuration: PostgresClient.Configuration) -> Self { .init(postgresConfiguration: configuration) }
}
