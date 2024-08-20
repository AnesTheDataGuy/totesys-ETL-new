resource "aws_secretsmanager_secret" "env_vars" {
  name = "env_variables_secret"
}

resource "aws_secretsmanager_secret_version" "env_vars" {
  secret_id     = aws_secretsmanager_secret.env_vars.id
  secret_string = jsonencode({
    DB_USERNAME = var.UN
    DB_PASSWORD = var.PW
    DB_NAME     = var.DB
    DB_HOST     = var.HT
    DB_PORT     = var.PT
  })

  depends_on = [aws_secretsmanager_secret.env_vars]
}