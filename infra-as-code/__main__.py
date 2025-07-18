import logging

import pulumi
import pulumi_digitalocean as do

region = "sgp1"
project_name = "first-project"
cluster_name = "k8s-1-33-1-do-1-sgp1-1752378431833"
version = "1.33.1-do.1"
pool_name = "pool-l7g14a0wb"
tags = ["k8s", "testing", project_name]
node_size = "s-2vcpu-4gb"
num_nodes = 3


def create_k8s_cluster(
    name, region, version, tags, pool_name, node_size, num_nodes, project_name
):
    logging.info(
        f"creating k8s cluster with name: {name}, region: {region}, project_name:{project_name}"  # noqa: E501
    )
    cluster = do.KubernetesCluster(
        resource_name=name,
        name=name,
        region=region,
        version=version,
        tags=tags,
        node_pool=do.KubernetesClusterNodePoolArgs(
            name=pool_name,
            size=node_size,
            node_count=num_nodes,
        ),
    )
    return cluster.id, cluster.cluster_urn


# existing_cluster = do.get_kubernetes_cluster(name=cluster_name)

cluster_id, cluster_urn = create_k8s_cluster(
    cluster_name, region, version, tags, pool_name, node_size, num_nodes, project_name
)
pulumi.export("k8s_cluster_id", cluster_id)
pulumi.export("k8s_cluster_urn", cluster_urn)

pulumi.export(
    "waiting",
    pulumi.runtime.invoke(
        "pulumi:providers:kubernetes",
        {
            # Dummy invocation to force a wait
        },
    ),
)
