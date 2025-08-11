load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "key",
    srcs = [
        "key.cc",
    ],
    hdrs = [
        "key.h",
    ],
    include_prefix = "compaction",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes",
        "//src/v/model",
    ],
)
