resource "google_pubsub_topic" "sample-jobs" {
  project = var.project
  name = "sample-jobs"

  message_storage_policy {
    allowed_persistence_regions = [
      "europe-west2",
    ]
  }
}

resource "google_pubsub_subscription" "sample-workers" {
  project = var.project
  name  = "sample-workers"
  topic = google_pubsub_topic.sample-jobs.name

  labels = {
    foo = "sample-service-workers"
  }
}
