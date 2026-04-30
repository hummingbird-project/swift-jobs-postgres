// swift-tools-version: 6.1
import PackageDescription

var defaultSwiftSettings: [SwiftSetting] = [
    // https://github.com/apple/swift-evolution/blob/main/proposals/0335-existential-any.md
    .enableUpcomingFeature("ExistentialAny"),

    // https://github.com/swiftlang/swift-evolution/blob/main/proposals/0444-member-import-visibility.md
    .enableUpcomingFeature("MemberImportVisibility"),

    // https://github.com/swiftlang/swift-evolution/blob/main/proposals/0409-access-level-on-imports.md
    .enableUpcomingFeature("InternalImportsByDefault"),
]

let package = Package(
    name: "swift-jobs-postgres",
    platforms: [.macOS(.v15), .iOS(.v18), .tvOS(.v18)],
    products: [
        .library(name: "JobsPostgres", targets: ["JobsPostgres"])
    ],
    dependencies: [
        // MARK: update to a released version before publishing
        .package(url: "https://github.com/hummingbird-project/swift-jobs.git", from: "1.3.0"),
        .package(url: "https://github.com/hummingbird-project/postgres-migrations.git", from: "1.0.0"),
        .package(url: "https://github.com/vapor/postgres-nio.git", from: "1.30.1"),
    ],
    targets: [
        .target(
            name: "JobsPostgres",
            dependencies: [
                .product(name: "PostgresMigrations", package: "postgres-migrations"),
                .product(name: "Jobs", package: "swift-jobs"),
                .product(name: "PostgresNIO", package: "postgres-nio"),
            ],
            swiftSettings: defaultSwiftSettings
        ),
        .testTarget(
            name: "JobsPostgresTests",
            dependencies: [
                "JobsPostgres"
            ],
            swiftSettings: defaultSwiftSettings
        ),
    ]
)
