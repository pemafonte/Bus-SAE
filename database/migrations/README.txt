Migrações incrementais (opcional)

- O ficheiro ../schema.sql é idempotente (IF NOT EXISTS) e pode ser reaplicado em
  produção sem apagar motoristas, escalas nem histórico de serviços.

- Para alterações futuras grandes, pode adicionar aqui ficheiros numerados, por
  exemplo 001_add_column_x.sql, e documentar a ordem de execução no README
  principal do repositório.

- Nunca use DROP TABLE / TRUNCATE em dados de negócio em produção sem backup.
