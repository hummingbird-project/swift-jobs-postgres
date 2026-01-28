//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

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
