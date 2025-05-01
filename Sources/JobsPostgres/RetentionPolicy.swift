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
            case retain(for: TimeInterval)
            case doNotRetain
        }

        let rawValue: Policy
        /// Retain policy
        /// default to 7 days
        public static func retain(for timeAmout: TimeInterval = 60 * 60 * 24 * 7) -> RetainData {
            RetainData(rawValue: .retain(for: timeAmout))
        }
        /// Never retain any data
        public static let never: RetainData = RetainData(rawValue: .doNotRetain)

        public static func == (lhs: RetentionPolicy.RetainData, rhs: RetentionPolicy.RetainData) -> Bool {
            switch (lhs.rawValue, rhs.rawValue) {
            case (.retain(let lhsTimeAmout), .retain(let rhsTimeAmout)):
                return lhsTimeAmout == rhsTimeAmout
            case (.doNotRetain, .doNotRetain):
                return true
            default:
                return false
            }
        }
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
        cancelled: RetainData = .retain(),
        completed: RetainData = .retain(),
        failed: RetainData = .retain(for: 60 * 60 * 24 * 30)
    ) {
        self.cancelled = cancelled
        self.completed = completed
        self.failed = failed
    }
}
