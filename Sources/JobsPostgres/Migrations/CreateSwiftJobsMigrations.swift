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
                status SMALLINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                unique_key TEXT NOT NULL UNIQUE
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

        try await connection.query(
            """
            CREATE TABLE IF NOT EXISTS swift_jobs.queues_metadata(
                key TEXT PRIMARY KEY,
                value BYTEA NOT NULL,
                queue_name TEXT NOT NULL DEFAULT 'default'
            )
            """,
            logger: logger
        )

        try await connection.query(
            """
            CREATE INDEX IF NOT EXISTS queues_metadata_key_queue_name_idx
            ON swift_jobs.queues_metadata(key, queue_name)
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
