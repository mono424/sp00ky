// swift-tools-version: 5.9

import PackageDescription

let package = Package(
    name: "spooky_push",
    platforms: [
        .iOS("13.0")
    ],
    products: [
        .library(name: "spooky-push", targets: ["spooky_push"])
    ],
    dependencies: [
        .package(name: "FlutterFramework", path: "../FlutterFramework")
    ],
    targets: [
        .target(
            name: "spooky_push",
            dependencies: [
                .product(name: "FlutterFramework", package: "FlutterFramework")
            ]
        )
    ]
)
