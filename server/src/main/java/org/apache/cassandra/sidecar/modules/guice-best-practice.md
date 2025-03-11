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

# Guice Best Practices in Sidecar

First of all, refer to [Guice Wiki](https://github.com/google/guice/wiki/BestPractices) for the best practices in general.

There are certain project-specific best practices we advocate for Apache Cassandra Sidecar. 
This article captures those items in the below. 

## Multi-bindings for Plugin Extension

[Multi-bindings](https://github.com/google/guice/wiki/Multibindings) is a Guice feature to support the plugin-type architecture,
where multiple modules contribute components to plug into the system.

The applications of multi-bindings in Sidecar include,
1. Routes definition
2. Periodic tasks deployment
3. Sidecar schemas registration

Taking the "Routes definition" for instance, multiple modules under the package `org.apache.cassandra.sidecar.modules` define
the `VertxRoute`s. Each of them are provided into the `MapBinder`, keyed by the unique class key. Once Guice is fully initialized,
it internally has a complete bindings map of `Map<Class<? extends ClassKey>, VertxRoute>`, merged from all modules. Finally, all the
routes in the map are mounted to the vertx `Router` and Sidecar service is started. Below is an example of providing the Sidecar
health check `VertxRoute` into the `MapBinder`, using the annotations, `@ProvidesIntoMap` in combination with `@KeyClassMapKey`.

```java
@ProvidesIntoMap
@KeyClassMapKey(VertxRouteMapKeys.SidecarHealthRouteKey.class)
VertxRoute sidecarHealthRoute(RouteBuilder.Factory factory)
{
  return factory.builderForUnprotectedRoute()
                .handler(context -> context.json(OK_STATUS))
                .build();
}
```

Other `VertxRoute`s can be provided and plugged in from other Guice models. Thus, the architecture is open to extension. The routes
mounting process can be found at [ApiModule](ApiModule.java).

The same applies to periodic tasks and sidecar internal schemas. You may check out
[SchedulingModule](SchedulingModule.java) and [SchedulingModule](SchedulingModule.java).

### Using binding DSL

Beside the binding annotation (preferred), e.g. `@ProvidesIntoMap`, the binding DSL is another way to define the bindings. The following snippet
illustrates the usage. The bindings are configured in the `configure()` method of a module. 

```java
    @Override
    protected void configure()
    {
        // Leverage TypeLiteral to preserve the lower bound type Key at the runtime
        TypeLiteral<Class<? extends ClassKey>> keyType = new TypeLiteral<>() {};
        TypeLiteral<PeriodicTask> valueType = new TypeLiteral<>() {};
        MapBinder<Class<? extends ClassKey>, PeriodicTask> periodicTaskMapBinder = MapBinder.newMapBinder(binder(), keyType, valueType);
        periodicTaskMapBinder.addBinding(PeriodicTaskMapKeys.RestoreJobDiscovererKey.class).to(RestoreJobDiscoverer.class);
        periodicTaskMapBinder.addBinding(PeriodicTaskMapKeys.RestoreProcessorKey.class).to(RestoreProcessor.class);
        periodicTaskMapBinder.addBinding(PeriodicTaskMapKeys.RingTopologyRefresherKey.class).to(RingTopologyRefresher.class);
    }
```

Using the DSL permits more succinct code. In the example above, each binding is a single line of code, whereas the annotation approach requires 
a provider method and generally more verbose. However, having succinct code should not be the only reason of using DSL. In most of the cases,
prefer the annotation approach, as it is often more flexible and provide better readability. 

The DSL approach should be used if the mapBinder target is also _directly_ referenced in other components. For example, `RestoreJobDiscoverer`
is annotated as `@Singleton`, and it is provided into the mapBinder and `RestoreJobConsistencyChecker`. Using the DSL, Guice ensures that the instance 
in the mapBinder is the same instance for `RestoreJobConsistencyChecker`. In other words, `RestoreJobDiscoverer` is a true singleton. However, if
using `@ProvidesIntoMap`, a new instance is created, violating the singleton scope. 

### Multi-bindings type resolution

Type resolution is needed when there is a requirement to substitute the bound types when integrating Apache Sidecar with your existing 
internal Cassandra Sidecar project. For example, disable route "X" and use an API-compatible route "Y".

[MultiBindingTypeResolverModule](multibindings/MultiBindingTypeResolverModule.java) and 
[MultiBindingTypeResolver](multibindings/MultiBindingTypeResolver.java) is the answer to the requirement. You can implement
your own `MultiBindingTypeResolver` that updates the `MapBinder` and override `MultiBindingTypeResolverModule`. 
Apache Sidecar employs the `IdentityMultiBindingTypeResolver` that makes no modification.

## Group bindings by feature

The bindings in Sidecar are grouped by their features. For instance, [ApiModule](ApiModule.java) provides the capability of defining API
routes. Another example is [CassandraOperationsModule](CassandraOperationsModule.java). It contains the bindings regarding triggering 
operations in and retrieving information from Cassandra. 

Grouping by feature allows newcomers to the project to quickly learn the features and the relevant classes.

It is also describe [here](https://github.com/google/guice/wiki/OrganizeModulesByFeature).

## Prefer binding to interface

When a different implementation can be provided in the future, prefer using an interface and a default implementation
that can be later switched transparently without affecting the classes that depend on it.

## Prefer binding utility as singleton, instead of static methods

Prefer creating concrete classes over utility methods. The concrete classes can be managed as a
[Singleton](https://en.wikipedia.org/wiki/Singleton_pattern) if they do not have external dependencies that might
change their behavior based on the configuration.
