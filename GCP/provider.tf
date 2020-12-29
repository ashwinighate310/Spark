
variable "path" {default = "/home/udemy/terraform/credentials"}

provider "google" {
    project = "festive-zoo-239708"
    region = "europe-west2-a"
    credentials = "${file("${var.path}/secrets.json")}"
}