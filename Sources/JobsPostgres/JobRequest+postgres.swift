//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2024-2025 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Jobs
import PostgresNIO

extension JobRequest {
    public static var psqlType: PostgresDataType {
        .bytea
    }
    public static var psqlFormat: PostgresFormat {
        .binary
    }

    @inlinable
    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) throws {
        try context.jsonEncoder.encode(self, into: &byteBuffer)
    }
}

#if hasAttribute(retroactive)
extension JobRequest: @retroactive PostgresEncodable where Parameters: Encodable {}
#else
extension JobRequest: PostgresEncodable where Parameters: Encodable {}
#endif
