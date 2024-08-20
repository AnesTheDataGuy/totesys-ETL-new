resource "aws_secretsmanager_secret" "db_credentials" {
  name = "db-credentials-secret"
}

resource "aws_secretsmanager_secret_version" "db_credentials" {
  secret_id     = aws_secretsmanager_secret.db_credentials.id
  secret_string = jsonencode({
    DB_USERNAME = var.UN
    DB_PASSWORD = var.PW
    DB_NAME     = var.DB
    DB_HOST     = var.HT
    DB_PORT     = var.PT
  })

  depends_on = [aws_secretsmanager_secret.db_credentials]
}