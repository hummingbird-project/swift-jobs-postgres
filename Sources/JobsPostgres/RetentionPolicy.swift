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

import Foundation

/// Data rentension policy
public struct RetentionPolicy: Sendable {
    /// Data retention policy
    public struct RetainData: Equatable, Sendable {
        enum Policy {
            case retain
            case doNotRetain
        }

        let rawValue: Policy
        /// Retain task
        public static var retain: RetainData { RetainData(rawValue: .retain) }
        /// Never retain any data
        public static var never: RetainData { RetainData(rawValue: .doNotRetain) }
    }

    /// Jobs with status cancelled
    /// default retention is set for 7 days
    public var cancelled: RetainData
    /// Jobs with status completed
    /// default retention is set to 7 days
    public var completed: RetainData
    /// Jobs with status failed
    /// default retention is set to 30 days
    public var failed: RetainData

    public init(
        cancelled: RetainData = .retain,
        completed: RetainData = .retain,
        failed: RetainData = .retain
    ) {
        self.cancelled = cancelled
        self.completed = completed
        self.failed = failed
    }
}
