SRC = 'src/main/java/org/onosproject/**/'
TEST = 'src/test/java/org/onosproject/**/'

CURRENT_NAME = 'onos-app-segmentrouting'
CURRENT_TARGET = ':' + CURRENT_NAME

COMPILE_DEPS = [
    '//lib:CORE_DEPS',
    '//lib:org.apache.karaf.shell.console',
    '//lib:javax.ws.rs-api',
    '//cli:onos-cli',
    '//incubator/api:onos-incubator-api',
    '//apps/routing-api:onos-apps-routing-api',
    '//utils/rest:onlab-rest',

]

TEST_DEPS = [
    '//lib:TEST_ADAPTERS',
]

java_library(
    name = CURRENT_NAME,
    srcs = glob([SRC + '/*.java']),
    deps = COMPILE_DEPS,
    visibility = ['PUBLIC'],
    resources_root = 'src/main/resources',
    resources = glob(['src/main/resources/**']),
)

java_test(
    name = 'tests',
    srcs = glob([TEST + '/*.java']),
    deps = COMPILE_DEPS +
           TEST_DEPS +
           [CURRENT_TARGET],
    source_under_test = [CURRENT_TARGET],
    resources_root = 'src/test/resources',
    resources = glob(['src/test/resources/**']),
)
