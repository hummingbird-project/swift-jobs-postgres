//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

#if compiler(>=6.2.3)

@_spi(JobsAPI) public import Jobs
import NIOCore
import PostgresNIO

#if canImport(FoundationEssentials)
public import FoundationEssentials
#else
public import Foundation
#endif

/// AnyJob details used by JobsAPI
private struct JobsAPIAnyJob: Decodable, PostgresDecodable {
    let name: String

    public init(from decoder: any Decoder) throws {
        // Job JSON is structured as follows
        //  {
        //      "JobName": { job data... }
        //  }
        let container = try decoder.container(keyedBy: _JobCodingKey.self)
        guard let key = container.allKeys.first else {
            throw DecodingError.dataCorrupted(.init(codingPath: decoder.codingPath, debugDescription: "No keys found."))
        }
        self.name = key.stringValue
    }

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

@_spi(JobsAPI) extension PostgresJobQueue: JobsAPI {
    public func getJobs(maxNumber: Int, paginationToken: String?) async throws -> GetJobsResponse {
        let index = paginationToken.flatMap { Int($0) } ?? 0
        let stream = try await self.client.query(
            """
            SELECT
                id, job, last_modified, status, created_at
            FROM swift_jobs.jobs
            WHERE queue_name = \(configuration.queueName)
            ORDER BY created_at DESC
            LIMIT \(maxNumber)
            OFFSET \(index)
            """,
            logger: self.logger
        )
        var jobs: [JobAPIMetadata] = []
        for try await (id, job, lastModified, status, createdAt) in stream.decode((UUID, JobsAPIAnyJob, Date, Status, Date).self, context: .default) {
            jobs.append(
                .init(
                    id: id,
                    name: job.name,
                    createdAt: createdAt,
                    completedAt: status == .completed ? lastModified : nil,
                    status: .init(status)
                )
            )
        }
        return GetJobsResponse(paginationToken: jobs.count < maxNumber ? nil : String(index + maxNumber), jobs: jobs)
    }

    public func getJob(id: UUID) async throws -> GetJobResponse? {
        let stream = try await self.client.query(
            """
            SELECT
                job, last_modified, status, created_at
            FROM swift_jobs.jobs
            WHERE id = \(id)
            """,
            logger: self.logger
        )
        var iterator = stream.decode((ByteBuffer, Date, Status, Date).self, context: .default).makeAsyncIterator()
        guard let (job, lastModified, status, createdAt) = try await iterator.next() else { return nil }
        // has to decode job to get name, should we add a name column to table
        let anyJob = try JSONDecoder().decode(JobsAPIAnyJob.self, from: job)
        return GetJobResponse(
            jobMetadata: .init(
                id: id,
                name: anyJob.name,
                createdAt: createdAt,
                completedAt: status == .completed ? lastModified : nil,
                status: .init(status)
            ),
            jobParameters: job
        )
    }
}

extension JobAPIMetadata.Status {
    init(_ status: PostgresJobQueue.Status) {
        self =
            switch status {
            case .cancelled: .cancelled
            case .completed: .completed
            case .failed: .failed
            case .paused: .paused
            case .pending: .pending
            case .processing: .processing
            }
    }
}

internal struct _JobCodingKey: CodingKey {
    var stringValue: String
    var intValue: Int?

    init?(stringValue: String) {
        self.stringValue = stringValue
        self.intValue = nil
    }

    init?(intValue: Int) {
        self.stringValue = "\(intValue)"
        self.intValue = intValue
    }

    init(stringValue: String, intValue: Int?) {
        self.stringValue = stringValue
        self.intValue = intValue
    }

    internal init(index: Int) {
        self.stringValue = "Index \(index)"
        self.intValue = index
    }
}

#endif
