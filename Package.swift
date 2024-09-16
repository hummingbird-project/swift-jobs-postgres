// swift-tools-version: 5.9

import PackageDescription

let package = Package(
    name: "swift-jobs-postgres",
    platforms: [.macOS(.v14), .iOS(.v17), .tvOS(.v17)],
    products: [
        .library(name: "JobsPostgres", targets: ["JobsPostgres"]),
    ],
    dependencies: [
        .package(url: "https://github.com/hummingbird-project/swift-jobs.git", branch: "main"),
        .package(url: "https://github.com/hummingbird-project/hummingbird-postgres.git", branch: "delete-jobs-postgres"),
        .package(url: "https://github.com/vapor/postgres-nio.git", from: "1.21.0"),
    ],
    targets: [
        .target(
            name: "JobsPostgres",
            dependencies: [
                .product(name: "PostgresMigrations", package: "hummingbird-postgres"),
                .product(name: "Jobs", package: "swift-jobs"),
                .product(name: "PostgresNIO", package: "postgres-nio"),
            ]
        ),
        .testTarget(
            name: "JobsPostgresTests",
            dependencies: [
                "JobsPostgres",
            ]
        ),
    ]
)