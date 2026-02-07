terraform {
  backend "local" {
    path = "terraform.tfstate"
  }
}

### Use this if you want to configure S3 as your backend,
### you will have to create your bucket first and replace the values accordingly.

/*
terraform {
  backend "s3" {
    bucket = "terraform-state-bucket"
    key    = "tf_state/terraform.tfstate"
    region = "us-east-1"
  }
}
*/

provider "aws" {
    region = "us-east-1"
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}


locals {
  bucket_inputs = {
        bucket_name = "my-unique-bucket-name-12345"
        tags = {
        Environment = "sandbox"
        Owner       = "John Doe"
        repo       = "db-performance-testing"
        }
    }
  }

module "s3_bucket" {
  source = "bucket/"
  config = local.bucket_inputs
}
