// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "swift-jobs-postgres",
    platforms: [.macOS(.v15), .iOS(.v18), .tvOS(.v18)],
    products: [
        .library(name: "JobsPostgres", targets: ["JobsPostgres"])
    ],
    dependencies: [
        .package(url: "https://github.com/hummingbird-project/swift-jobs.git", from: "1.0.0"),
        .package(url: "https://github.com/hummingbird-project/postgres-migrations.git", from: "1.0.0"),
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
