//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import Foundation
import Testing

@testable import JobsPostgres

struct UUIDv7Tests {
    @Test func testVersion() {
        for _ in 0..<16 {
            let uuid = UUID._version7()
            #expect(uuid.uuid.6 & 0xf0 == 0x70)
        }
    }

    @Test func testIncreasing() async throws {
        var uuids: [UUID] = []
        for _ in 0..<16 {
            uuids.append(UUID._version7())
            try await Task.sleep(for: .microseconds(100))
        }
        #expect(uuids == uuids.sorted())
    }
}
