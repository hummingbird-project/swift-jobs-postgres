// swift-tools-version: 5.10

import PackageDescription

let package = Package(
    name: "swift-jobs-postgres",
    platforms: [.macOS(.v14), .iOS(.v17), .tvOS(.v17)],
    products: [
        .library(name: "JobsPostgres", targets: ["JobsPostgres"])
    ],
    dependencies: [
        // TODO: use a released version of swift-jobs
        .package(url: "https://github.com/hummingbird-project/swift-jobs.git", from: "1.0.0-beta.8"),
        .package(url: "https://github.com/hummingbird-project/postgres-migrations.git", from: "0.1.0"),
        .package(url: "https://github.com/vapor/postgres-nio.git", from: "1.25.0"),
    ],
    targets: [
        .target(
            name: "JobsPostgres",
            dependencies: [
                .product(name: "PostgresMigrations", package: "postgres-migrations"),
                .product(name: "Jobs", package: "swift-jobs"),
                .product(name: "PostgresNIO", package: "postgres-nio"),
            ]
        ),
        .testTarget(
            name: "JobsPostgresTests",
            dependencies: [
                "JobsPostgres"
            ]
        ),
    ]
)
