import logging
import os

import pulumi
import pulumi_digitalocean as do
import traceback
#from pulumi import InvokeError


region = "sgp1"
project_name = "first-project"
cluster_name = "k8s-1-33-1-do-1-sgp1-1752378431833"
version = "1.33.1-do.1"
pool_name = "pool-l7g14a0wb"
tags = ["k8s", "testing", project_name]
node_size= "s-2vcpu-4gb"
num_nodes=3


def create_k8s_cluster(cluster_name, region,version,tags, pool_name, node_size, num_nodes, project_name):
    logging.info(f"creating k8s cluster with name: {cluster_name}, region: {region}, project_name:{project_name}")
    cluster = do.KubernetesCluster(
        resource_name=cluster_name,
        name=cluster_name,
        region=region,
        version=version,
        tags=tags,
        node_pool=do.KubernetesClusterNodePoolArgs(
            name=pool_name,
            size=node_size,
            node_count=num_nodes,
        )
    )
    pulumi.export("k8s_cluster_id", cluster.id)
    pulumi.export("k8s_cluster_urn", cluster.cluster_urn)


try:
    existing_cluster = do.get_kubernetes_cluster(name=cluster_name)
    resource_name=cluster_name
    #existing_cluster = do.get(resource_name, os.environ.get('K8S_CLUSTER_ID', ''))
    create_k8s_cluster(cluster_name, region, version, tags, pool_name, node_size, num_nodes, project_name)
    #pulumi.export("k8s_cluster_id", existing_cluster.id)
except Exception as e:
    if "Unable to find cluster with name" in str(e):
        pulumi.log.warn(f"**** Cluster '{cluster_name}' does not exist — will create it.")
        create_k8s_cluster(cluster_name, region, version, tags, pool_name, node_size, num_nodes, project_name)
    else:
        traceback.print_exc()
#
# try:
#     cluster = do.KubernetesCluster.get("existing-cluster", cluster_name, opts=pulumi.ResourceOptions())
#     pulumi.export("k8s_cluster_id", cluster.id)
# except Exception as e:
#     pulumi.log.warn(f"Cluster '{cluster_name}' details could not be fetched")
#     traceback.print_exc()
#     #create_k8s_cluster()
