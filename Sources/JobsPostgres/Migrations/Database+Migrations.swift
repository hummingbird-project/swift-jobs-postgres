//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import PostgresMigrations

extension DatabaseMigrationGroup {
    /// JobQueue migration group
    public static var jobQueue: Self { .init("_hb_jobqueue") }
}
