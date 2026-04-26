output "k8s_cluster_internal_fqdn" {
  value = yandex_kubernetes_cluster.k8s_cluster.master[0].internal_v4_endpoint
}

output "registry_id" {
  value = yandex_container_registry.registry.id
}