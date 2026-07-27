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

struct AddCreateAtColumnJobsMigration: DatabaseMigration {
    func apply(connection: PostgresNIO.PostgresConnection, logger: Logging.Logger) async throws {
        try await connection.query(
            """
            ALTER TABLE swift_jobs.jobs
            ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            """,
            logger: logger
        )
    }

    func revert(connection: PostgresNIO.PostgresConnection, logger: Logging.Logger) async throws {
        try await connection.query(
            """
            ALTER TABLE swift_jobs.jobs
            DROP COLUMN created_at
            """,
            logger: logger
        )
    }

    var name: String { "_AddCreateAtColumnJobsMigration_" }
    var group: DatabaseMigrationGroup { .jobQueue }
}
