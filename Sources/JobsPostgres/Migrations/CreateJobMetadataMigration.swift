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

struct CreateJobMetadataMigration: DatabaseMigration {

    func apply(connection: PostgresNIO.PostgresConnection, logger: Logging.Logger) async throws {
        try await connection.query(
            """
            CREATE TABLE IF NOT EXISTS swift_jobs.metadata(
                key TEXT NOT NULL,
                value BYTEA NOT NULL,
                expires TIMESTAMPTZ NOT NULL DEFAULT 'infinity',
                queue_name TEXT NOT NULL DEFAULT 'default',
                CONSTRAINT key_queue_name PRIMARY KEY (key, queue_name)
            )
            """,
            logger: logger
        )
    }

    func revert(connection: PostgresNIO.PostgresConnection, logger: Logging.Logger) async throws {
        try await connection.query(
            """
            DROP TABLE swift_jobs.metadata
            """,
            logger: logger
        )
    }

    var description: String { "__JobMetadataMigration__" }
    var group: DatabaseMigrationGroup { .jobQueue }
}
