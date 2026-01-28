//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

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
