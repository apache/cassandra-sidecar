<!--
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
-->

# Testing Guide for Apache Cassandra Sidecar

This document provides a comprehensive guide to running tests for the Apache Cassandra Sidecar project.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Test Types](#test-types)
- [Running Tests](#running-tests)
- [Test Configuration](#test-configuration)
- [Development Testing](#development-testing)
- [Continuous Integration](#continuous-integration)
- [Troubleshooting](#troubleshooting)

## Prerequisites

### Required Software

1. **Java 11 or higher**
2. **Docker** for running integration tests that leverage the S3MockContainer to test the S3 client. Unless your test requires the S3 Mock Container, please create a unit or integration test. 
3. **Git** for cloning repositories

### Build Prerequisites

Before running tests, you must build the required Cassandra dtest jars:

```bash
./scripts/build-dtest-jars.sh
```

This script builds dtest jars for the supported Cassandra versions.
You can customize which versions to build:

- **REPO**: Cassandra git repository (default: `https://github.com/apache/cassandra.git`)
- **BRANCHES**: Space-delimited list of branches (default: `"cassandra-4.0 cassandra-4.1 cassandra-5.0 trunk"`)

Example with custom branches:
```bash
BRANCHES="cassandra-4.1 trunk" ./scripts/build-dtest-jars.sh
```

### Network Setup for Multi-Node Tests

For multi-node in-jvm dtests, network aliases must be configured.
Tests assume each node's IP address is `127.0.0.x`, where `x` is the node ID.

#### macOS Network Aliases

Create temporary aliases for nodes 2-20:
```bash
for i in {2..20}; do sudo ifconfig lo0 alias "127.0.0.${i}"; done
```

#### Linux Network Aliases

Create temporary aliases for nodes 2-20:
```bash
for i in {2..20}; do sudo ip addr add "127.0.0.${i}/8" dev lo; done
```

#### Hostname Configuration

In addition to network aliases, you must add hostname entries to `/etc/hosts` for localhost1 through localhost20.

Add the following entries to `/etc/hosts`:
```
127.0.0.1    localhost1
127.0.0.2    localhost2
127.0.0.3    localhost3
127.0.0.4    localhost4
127.0.0.5    localhost5
127.0.0.6    localhost6
127.0.0.7    localhost7
127.0.0.8    localhost8
127.0.0.9    localhost9
127.0.0.10   localhost10
127.0.0.11   localhost11
127.0.0.12   localhost12
127.0.0.13   localhost13
127.0.0.14   localhost14
127.0.0.15   localhost15
127.0.0.16   localhost16
127.0.0.17   localhost17
127.0.0.18   localhost18
127.0.0.19   localhost19
127.0.0.20   localhost20
```

**Note**: Editing `/etc/hosts` requires sudo privileges:
```bash
sudo vi /etc/hosts
```

## Test Types

### Unit Tests

Unit tests verify individual components in isolation. They are fast-running
and don't require external dependencies.

**Location**: `src/test/java/` in each module

**Running unit tests**:
```bash
# Run all unit tests
./gradlew test

# Run unit tests for a specific module
./gradlew :client:test
./gradlew :server:test
```

### Integration Tests

Integration tests verify component interactions and require running
Cassandra instances. They are divided into two categories:

#### Lightweight Integration Tests
- Basic functionality tests
- Faster execution
- **Tag**: None (default)

#### Heavyweight Integration Tests  
- Complex scenarios
- Multi-node cluster tests
- Longer execution times
- **Tag**: `@Tag("heavy")`

**Location**: `src/integrationTest/` in test modules

**Running integration tests**:
```bash
# Run all integration tests
./gradlew integrationTest

# Run only lightweight integration tests
./gradlew integrationTestLightWeight

# Run only heavyweight integration tests
./gradlew integrationTestHeavyWeight

# Run integration tests for specific module
./gradlew :integration-tests:integrationTest

# Run integration tests for specific module
./gradlew :server:integrationTest
```

### Container Tests

Container-based tests run in Docker environments for additional isolation. For example,
tests for the Restore from S3 feature leverage the testcontainer-based S3 service
(`com.adobe.testing.s3mock.testcontainers.S3MockContainer`). Only tests that require
testcontainer-like functionality should live under `src/containerTest/`; otherwise,
a unit or in JVM dtest integration test is preferred.

**Location**: `src/containerTest/` in relevant modules

### Test Fixtures

Test fixtures provide shared test utilities and data across modules.

**Location**: `src/testFixtures/` in modules with `java-test-fixtures` plugin

## Running Tests

### Quick Test Commands

```bash
# Run all tests (unit + integration + container)
./gradlew check

# Run only unit tests
./gradlew test

# Run only integration tests
./gradlew integrationTest

# Skip integration tests
./gradlew check -x integrationTest

# Skip container tests
./gradlew check -x containerTest

# Run with specific Cassandra versions
./gradlew test -Dcassandra.sidecar.versions_to_test=4.1,5.1
```

### Test Execution Configuration

Integration tests support several environment variables:

| Variable                         | Default | Description                                                |
|----------------------------------|---------|------------------------------------------------------------|
| `INTEGRATION_MAX_HEAP_SIZE`      | `3000M` | Maximum heap size for integration tests, per parallel fork |
| `INTEGRATION_MAX_PARALLEL_FORKS` | `4`     | Number of parallel test forks                              |
| `INTEGRATION_MTLS_ENABLED`       | `true`  | Enable mTLS for integration tests                          |

Example:
```bash
INTEGRATION_MAX_HEAP_SIZE=4000M INTEGRATION_MAX_PARALLEL_FORKS=2 ./gradlew integrationTest
```

### Gradle Properties

You can also skip integration tests using Gradle properties:

```bash
# Via command line
./gradlew check -x integrationTest

# Via environment variable
export skipIntegrationTest=true
./gradlew check
```

## Test Configuration

### Cassandra Version Testing

The test framework supports multiple Cassandra versions simultaneously. Configure versions via:

**System Property**:
```bash
-Dcassandra.sidecar.versions_to_test=4.0,4.1,5.1
```

**Default versions**: `4.1,5.1` (as defined in `TestVersionSupplier.java`)

### Test Logging

Integration tests use a dedicated logback configuration:
- **File**: `server/src/test/resources/logback-in-jvm-dtest.xml`
- **Level**: Configurable per test class

### JVM Options

Tests running on Java 11+ automatically receive optimized JVM arguments defined in `gradle/common/java11Options.gradle`.

## Development Testing

### Running Tests During Development

For faster development cycles:

```bash
# Run the Sidecar server with code checks disabled. This is useful for local manual testing of endpoints.
./gradlew run

# Run specific test class
./gradlew test --tests "org.apache.cassandra.sidecar.HealthServiceTest"

# Run specific integration test class
./gradlew :integration-tests:integrationTestLightWeight --tests "org.apache.cassandra.sidecar.routes.RoutesIntegrationTest"

# Or run a heavy-weight test class
./gradlew :server:integrationTestHeavyWeight --tests "org.apache.cassandra.sidecar.routes.tokenrange.LeavingTest"

# Debug test execution
./gradlew test --debug-jvm

# Run tests continuously
./gradlew test --continuous
```

### Test Reports

Test reports are generated in:
- **Unit tests**: `build/test-results/`
- **Integration tests**: `build/test-results/integration/`
- **HTML reports**: Available in respective `reports/` directories

### Code Coverage

Jacoco coverage reports are automatically generated after tests:

```bash
# Generate coverage report
./gradlew jacocoTestReport

# View coverage report
open build/reports/jacoco/test/html/index.html
```

## Continuous Integration

### CircleCI Integration

The project uses CircleCI for automated testing. To set up CircleCI on your fork:

1. Use CircleCI's "Add Projects" function
2. Choose manual configuration (don't replace the existing config)
3. CircleCI will automatically use the in-project configuration

### Code Quality Checks

Before tests run, the following quality checks execute:
- **Checkstyle**: Code style verification
- **SpotBugs**: Static analysis for bugs
- **RAT**: License header verification

```bash
# Run code quality checks only
./gradlew codeCheckTasks

# Skip code quality checks
./gradlew test -x codeCheckTasks
```

## Troubleshooting

### Common Issues

#### "DTest jar not found"
**Solution**: Run `./scripts/build-dtest-jars.sh` to build required dependencies.

#### "Network interface not available"
**Solution**: Set up network aliases as described in [Network Setup](#network-setup-for-multi-node-tests).

#### "OutOfMemoryError during integration tests"
**Solution**: Increase heap size:
```bash
INTEGRATION_MAX_HEAP_SIZE=4000M ./gradlew integrationTest
```

#### "Tests are hanging or slow"
**Solution**: Reduce parallel forks:
```bash
INTEGRATION_MAX_PARALLEL_FORKS=1 ./gradlew integrationTest
```

### Test Isolation Issues

Integration tests use `forkEvery = 1` to ensure test isolation. If you encounter test pollution:

1. Verify each test properly cleans up resources
2. Check for static state leakage within the failing test class.
3. Review test execution order.

If the test state pollution is unavoidable within the test class, it is reasonable to split the tests into multiple, independent test classes.

### Enabling debug logging for Integration Tests

Enable debug logging by modifying `server/src/test/resources/logback-in-jvm-dtest.xml`:

```xml
<logger name="org.apache.cassandra.sidecar" level="DEBUG"/>
```

### Troubleshooting Long-Running Tests

For resource-intensive tests:

1. Use `@Tag("heavy")` to categorize expensive tests
2. Monitor test execution times (logged automatically for tests ≥ 60 seconds)
3. Consider test parallelization settings

### Environment-Specific Issues

#### Docker Issues
- Ensure Docker daemon is running
- Verify Docker has sufficient resources allocated
- Check for port conflicts

#### Java Version Issues
- Verify Java 11+ compatibility
- Check JVM arguments in CI environments
- Review JVM options defined in [gradle/common/java11Options.gradle](gradle/common/java11Options.gradle)

## Additional Resources

- [Project README](README.md) - General project information
- [Contributing Guide](CONTRIBUTING.md) - Development contribution guidelines
- [CircleCI Configuration](.circleci/config.yml) - CI/CD pipeline details
- [Gradle Build Scripts](build.gradle) - Build configuration details

For questions or issues not covered in this guide, please:
1. Check existing [JIRA issues](https://issues.apache.org/jira/projects/CASSANDRASC/issues/)
2. Join the discussion on [ASF Slack](https://s.apache.org/slack-invite) in #cassandra
3. Subscribe to the [mailing list](mailto:dev-subscribe@cassandra.apache.org)
