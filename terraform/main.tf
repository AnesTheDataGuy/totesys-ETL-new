terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket = "testing-backend-bucket-nc"
    key    = "de-s3-file-reader/terraform.tfstate"
    region = "eu-west-2"
    # access = var.access_key != "" ? var.access_key : env("AWS_ACCESS_KEY_ID")
    # secret_key = var.secret_key != "" ? var.secret_key : env("AWS_SECRET_ACCESS_KEY")
    # cohort_id = env("COHORT_ID")
    # user = env("USER")
    # password = env("PASSWORD")
    # host = env("HOST")
    # database = env("DATABASE")
    # port = env("PORT")
  }
}

provider "aws" {
  region = "eu-west-2"
  default_tags {
    tags = {
      ProjectName  = "ToteSys-Najdorf"
      DeployedFrom = "Terraform"
      Repository   = "https://github.com/AnesTheDataGuy/totesys-ETL"
    }
  }
}

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}
