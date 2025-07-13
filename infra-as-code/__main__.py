import pulumi
import pulumi_digitalocean as do

region = "sgp1"
project_name = "first-project"
cluster_name = "k8s-1-33-1-do-1-sgp1-1752378431833"
version = "1.33.1-do.1"
pool_name = "pool-l7g14a0wb"
tags = ["k8s", "testing"]
node_size= "s-2vcpu-4gb"
num_nodes=3

cluster = do.KubernetesCluster.get("existing-cluster", cluster_name, opts=pulumi.ResourceOptions())

# Only create if not found
if cluster is None:
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
        ),
        project_id=project_name,  # Optional; project_id is not project name. You might need to fetch via API.
    )
