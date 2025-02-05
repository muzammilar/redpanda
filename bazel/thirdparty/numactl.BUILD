load("@bazel_skylib//rules:write_file.bzl", "write_file")

# This build is a translation from the official autotools-based build contained
# in the numactl source tree.

# The only preprocessor macros we need in the configuration file (and are used in the source)
# is TLS, the others are unused.
write_file(
    name = "config_h",
    out = "config.h",
    content = [
        "#define TLS __thread",
    ],
)

# !!! NOTE !!!
# This must be a shared library due to LGPL licensing

# This is a trick in Bazel to force a shared library to be built.
# Technically in recent versions this is supposed to be replaced
# with cc_shared_library, but that seems to be too early/has bugs
# namely when building this into our redpanda binary we get strange
# issues where all our rust dependencies are dropped from the linker
# input. So for now we'll stick to the old school way of forcing
# shared library usage through cc_binary(linkshared = True)
cc_binary(
    name = "libnuma.so",
    srcs = [
        "affinity.c",
        "affinity.h",
        "distance.c",
        "libnuma.c",
        "numaint.h",
        "rtnetlink.c",
        "rtnetlink.h",
        "syscall.c",
        "sysfs.c",
        "sysfs.h",
        "util.h",
        ":config_h",
        "numa.h",
        "numacompat1.h",
        "numaif.h",
    ],
    linkshared = True,
    copts = ["-Wno-unused-but-set-variable"],
    includes = ["."],
    additional_linker_inputs = [
        ":versions.ldscript",
    ],
    linkopts = [
        "-Wl,-init,numa_init",
        "-Wl,-fini,numa_fini",
        "-Wl,--version-script,$(location :versions.ldscript)",
    ],
)

# Wrap our shared library with the appropriate headers exposed.
# Generally cc_import is recommended over cc_library with .so srcs,
# but I failed to get that to work correctly in our setup, so we
# fallback to the old school ways of doing things again.
cc_library(
    name = "numactl",
    srcs = [":libnuma.so"],
    hdrs = [
        "numa.h",
        "numacompat1.h",
        "numaif.h",
    ],
    visibility = ["@seastar//:__pkg__"],
    includes = ["."],
)
