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
                expires TIMESTAMPTZ NOT NULL DEFAULT '+infinity',
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
