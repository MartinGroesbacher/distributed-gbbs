cc_library(
    name = "algorithm",
    srcs = ["algorithm.cpp"],
    hdrs = ["algorithm.h"],
    deps = [
        "//framework/message_passing:runtime",
        "//gbbs",
    ],
    visibility = ["//visibility:public"],
)

cc_binary(
    name = "mis",
    srcs = ["main.cc"],
    deps = [
        ":algorithm",
        "//framework/common:graph_adapter",
        "//framework/message_passing:runtime",
        "//gbbs",
    ],
)
