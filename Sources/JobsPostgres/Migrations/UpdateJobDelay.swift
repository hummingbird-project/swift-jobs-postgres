//
//  UpdateJobDelay.swift
//  swift-jobs-postgres
//
//  Created by Stevenson Michel on 2/17/25.
//

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

struct UpdateJobDelay: DatabaseMigration {
    func apply(connection: PostgresConnection, logger: Logger) async throws {
        try await connection.query(
            """
            ALTER TABLE _hb_pg_job_queue ALTER COLUMN delayed_until TYPE TIMESTAMPTZ USING COALESCE(delayed_until, NOW()),
                ALTER COLUMN delayed_until SET DEFAULT NOW(),
                ALTER COLUMN delayed_until SET NOT NULL
                
            """,
            logger: logger
        )
    }

    func revert(connection: PostgresConnection, logger: Logger) async throws {
        try await connection.query(
            "ALTER TABLE _hb_pg_job_queue ALTER COLUMN delayed_until DROP NOT NULL",
            logger: logger
        )
    }

    var name: String { "_Update_JobQueueDelay_Table_" }
    var group: DatabaseMigrationGroup { .jobQueue }
}
