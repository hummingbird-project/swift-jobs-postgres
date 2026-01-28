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

struct CreateSwiftJobsMigrations: DatabaseMigration {

    func apply(connection: PostgresNIO.PostgresConnection, logger: Logging.Logger) async throws {

        try await connection.query("CREATE SCHEMA IF NOT EXISTS swift_jobs;", logger: logger)

        try await connection.query(
            """
            CREATE TABLE IF NOT EXISTS swift_jobs.jobs (
                id UUID PRIMARY KEY,
                job BYTEA NOT NULL,
                last_modified TIMESTAMPTZ NOT NULL DEFAULT now(),
                queue_name TEXT NOT NULL DEFAULT 'default',
                status SMALLINT NOT NULL
            );
            """,
            logger: logger
        )

        try await connection.query(
            """
            CREATE TABLE IF NOT EXISTS swift_jobs.queues(
                job_id UUID PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL,
                delayed_until TIMESTAMPTZ NOT NULL DEFAULT now(),
                queue_name TEXT NOT NULL DEFAULT 'default',
                priority SMALLINT NOT NULL DEFAULT 0
            );
            """,
            logger: logger
        )

        try await connection.query(
            """
            CREATE INDEX IF NOT EXISTS queues_delayed_until_priority_queue_name_idx 
            ON swift_jobs.queues(priority DESC, delayed_until ASC, queue_name ASC)
            """,
            logger: logger
        )
    }

    func revert(connection: PostgresNIO.PostgresConnection, logger: Logging.Logger) async throws {
        try await connection.query(
            """
            DROP SCHEMA swift_jobs CASCADE;
            """,
            logger: logger
        )
    }

    var description: String { "__CreateSwiftJobsMigrations__" }
    var group: DatabaseMigrationGroup { .jobQueue }
}
