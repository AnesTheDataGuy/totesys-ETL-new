terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket = "testing-backend-bucket-nc" # testing 
    key    = "de-s3-file-reader/terraform.tfstate"
    region = "eu-west-2"
  }
}

provider "aws" {
  region = "eu-west-2"
  default_tags {
    tags = {
      ProjectName  = "ToteSys-Najdorf"
      DeployedFrom = "Terraform"
      Repository   = "NEED TO CHANGE"
    }
  }
}

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}
