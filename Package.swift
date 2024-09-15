// swift-tools-version: 5.9

import PackageDescription

let package = Package(
    name: "swift-jobs-postgres",
    platforms: [.macOS(.v14), .iOS(.v17), .tvOS(.v17)],
    products: [
        .library(name: "JobsPostgres2", targets: ["JobsPostgres2"]),
    ],
    dependencies: [
        .package(url: "https://github.com/hummingbird-project/swift-jobs.git", branch: "main"),
        .package(url: "https://github.com/hummingbird-project/hummingbird-postgres.git", branch: "split-libraru"),
        .package(url: "https://github.com/vapor/postgres-nio.git", from: "1.21.0"),
    ],
    targets: [
        .target(
            name: "JobsPostgres2",
            dependencies: [
                .product(name: "PostgresMigrations", package: "hummingbird-postgres"),
                .product(name: "Jobs", package: "swift-jobs"),
                .product(name: "PostgresNIO", package: "postgres-nio"),
            ],
            path: "Sources/JobsPostgres"
        ),
        .testTarget(
            name: "JobsPostgresTests",
            dependencies: [
                "JobsPostgres2"
            ]
        ),
    ]
)
