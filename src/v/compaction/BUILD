load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "compaction",
    hdrs = [
        "fwd.h",
    ],
    include_prefix = "compaction",
    visibility = ["//visibility:public"],
)

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

redpanda_cc_library(
    name = "key_offset_map",
    srcs = [
        "key_offset_map.cc",
    ],
    hdrs = [
        "key_offset_map.h",
    ],
    include_prefix = "compaction",
    visibility = ["//visibility:public"],
    deps = [
        ":key",
        "//src/v/container:chunked_vector",
        "//src/v/hashing:secure",
        "//src/v/model",
        "//src/v/utils:tracking_allocator",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)
