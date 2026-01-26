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

import Logging
import PostgresMigrations
import PostgresNIO

struct CreateWorkerIDColumnMigration: DatabaseMigration {
    func apply(connection: PostgresNIO.PostgresConnection, logger: Logging.Logger) async throws {
        try await connection.query(
            """
            ALTER TABLE swift_jobs.jobs
            ADD COLUMN worker_id TEXT
            """,
            logger: logger
        )
    }

    func revert(connection: PostgresNIO.PostgresConnection, logger: Logging.Logger) async throws {
        try await connection.query(
            """
            ALTER TABLE swift_jobs.jobs
            DROP COLUMN worker_id
            """,
            logger: logger
        )
    }

    var description: String { "__JobMetadataMigration__" }
    var group: DatabaseMigrationGroup { .jobQueue }
}
