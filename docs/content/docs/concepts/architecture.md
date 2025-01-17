---
title: "Architecture"
weight: 2
type: docs
aliases:
- /concepts/architecture.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Architecture
Flink Kubernetes Operator (Operator) acts as a control plane to manage the complete deployment lifecycle of Apache Flink applications. The Operator can be installed on a Kubernetes cluster using [Helm](https://helm.sh). In most production environments it is typically deployed in a designated namespace and controls Flink deployments in one or more managed namespaces. The custom resource descriptor (CRD) that describes the schema of a `FlinkDeployment` is a cluster wide resource. For a CRD, the declaration must be registered before any resources of that CRDs kind(s) can be used, and the registration process sometimes takes a few seconds.  

{{< img src="/img/concepts/architecture.svg" alt="Flink Kubernetes Operator Architecture" >}}
> Note: There is no support at this time for [upgrading or deleting CRDs using Helm](https://helm.sh/docs/chart_best_practices/custom_resource_definitions/).

## Control Loop
The Operator follow the Kubernetes principles, notably the [control loop](https://kubernetes.io/docs/concepts/architecture/controller/):

{{< img src="/img/concepts/control_loop.svg" alt="Control Loop" >}}

Users can interact with the operator using the Kubernetes command-line tool, [kubectl](https://kubernetes.io/docs/tasks/tools/). The Operator continuously tracks cluster events relating to the `FlinkDeployment` custom resource. When the operator receives a new event, it will take action to adjust the Kubernetes cluster to the desired state as part of its reconciliation loop. The initial loop consists of the following high-level steps:
1. User submits a `FlinkDeployment` custom resource(CR) using `kubectl`
2. The operator launches the Flink cluster deployment and creates an ingress rule for UI access
3. The `JobManager` creates `TaskManager` pods
4. The `JobManager` submits the job

The CR can be (re)applied on the cluster any time. The Operator makes continuous adjustments to imitate the desired state until the current state becomes the desired state. All lifecycle management operations are realized using this very simple principle in the Operator.

The Operator is built with the [Java Operator SDK](https://github.com/java-operator-sdk/java-operator-sdk) and uses the [Native Kubernetes Integration](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/native_kubernetes/) for launching Flink deployments and submitting jobs under the hood. The Java Operator SDK is a higher level framework and related tooling to support writing Kubernetes Operators in Java. Both the Java Operator SDK and Flink's native kubernetes integration itself is using the [Fabric8 Kubernetes Client](https://github.com/fabric8io/kubernetes-client) to interact with the Kubernetes API Server.





 







