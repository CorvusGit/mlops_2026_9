# modules/k8s/main.tf

resource "yandex_kubernetes_cluster" "k8s_cluster" {
  name       = var.instance_name
  network_id = var.network_id

  master {
    version = "1.28"
    zonal {
      zone      = var.provider_config.zone
      subnet_id = var.subnet_id
    }

    maintenance_policy {
      auto_upgrade = true
    }

    # ЗАКРЫВАЕМ API ОТ ИНТЕРНЕТА
    public_ip = false 
  }

  
  service_account_id      = var.service_account_id
  node_service_account_id = var.service_account_id
}

resource "yandex_kubernetes_node_group" "k8s_nodes" {
  cluster_id = yandex_kubernetes_cluster.k8s_cluster.id
  name       = "minimal-node-group"

  instance_template {
    platform_id = "standard-v1"

    resources {
      memory = 4  # GB
      cores  = 2
    }

    boot_disk {
      type = "network-ssd"
      size = 20 # GB
    }

    network_interface {
      subnet_ids = [var.subnet_id]
      nat        = false
    }

    scheduling_policy {
      preemptible = true  # дешевле
    }
  }

  scale_policy {
    fixed_scale {
      size = 3
    }
  }

  allocation_policy {
    location {
      zone = var.provider_config.zone
    }
  }
}

# Реестр для образов
resource "yandex_container_registry" "registry" {
  name      = var.registry_name
  folder_id = var.provider_config.folder_id
}