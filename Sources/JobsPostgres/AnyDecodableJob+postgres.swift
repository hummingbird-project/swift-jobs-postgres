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

extension AnyDecodableJob {
    public static var psqlType: PostgresDataType {
        .bytea
    }
    public static var psqlFormat: PostgresFormat {
        .binary
    }

    public init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        switch (format, type) {
        case (.binary, .bytea):
            self = try context.jsonDecoder.decode(Self.self, from: buffer)
        default:
            throw PostgresDecodingError.Code.typeMismatch
        }
    }
}

#if hasAttribute(retroactive)
extension AnyDecodableJob: @retroactive PostgresDecodable {}
#else
extension AnyDecodableJob: PostgresDecodable {}
#endif
