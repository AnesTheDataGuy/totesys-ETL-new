variable "lambda_bucket" {
    type = string
    default = "ToteSys-Lambda-"
}

variable "raw_data" {
    type = string
    default = "ToteSys-Raw-Data-"
}

variable "processed_data" {
    type = string
    default = "ToteSys-Raw-Data-"
}

variable "athena_queries" {
    type = string
    default = "ToteSys-Raw-Data-"
}

variable "extract_lambda" {
  type    = string
  default = "extract"
}

variable "transform_lambda" {
  type    = string
  default = "transform"
}

variable "load_lambda" {
  type    = string
  default = "load"
}

variable "python_runtime" {
  type    = string
  default = "3.12" 
}