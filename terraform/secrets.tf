resource "aws_secretsmanager_secret" "totesys_secret" {
  name = "totesys_database_credentials_test"
  description = "secret for database"
  
}

variable "credentials" {
  default = {
    COHORT_ID="de_2024_06_03"
    USER="project_team_6"
    PASSWORD="3x0ekEA99wHddWx"
    HOST="nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com"
    DATABASE="totesys"
    PORT=5432
  }
  type = map(string)
}

resource "aws_secretsmanager_secret_version" "totesys_secret_manager" {
  secret_id     = aws_secretsmanager_secret.totesys_secret.id
  secret_string = jsonencode(var.credentials)
}
