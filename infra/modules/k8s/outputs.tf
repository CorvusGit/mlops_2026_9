output "k8s_cluster_internal_fqdn" {
  value = yandex_kubernetes_cluster.kub_cluster.master[0].internal_v4_endpoint
}

output "k8_cluster_id" {
  value = yandex_kubernetes_cluster.kub_cluster.id
}

output "registry_id" {
  value = yandex_container_registry.registry.id
}
