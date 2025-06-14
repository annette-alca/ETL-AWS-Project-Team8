terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket = "bucket-to-hold-tf-state-for-terraform" #sample, user to change
    key    = "de-project-terrific-totes/terraform.tfstate" #sample, user to change
    region = "eu-west-2"
}
}

# Configure the AWS Provider
provider "aws" {
  region = "eu-west-2"
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}


  