# Plataforma para Motoristas de Autocarros

MVP com:
- Login individual por motorista
- Login por username (todos os perfis)
- Iniciar e finalizar viagem
- Registo de pontos GPS durante a viagem
- Desenho de percurso em mapa
- Calculo de kms da viagem
- Historico de servicos
- Dashboard de supervisor com filtros
- Exportacao CSV (Excel)
- Login de motorista por numero mecanografico
- Servicos previstos e escala diaria
- Handover de servico (troca motorista/viatura)

## Estrutura

- `backend`: API Node.js + Express + PostgreSQL
- `database/schema.sql`: modelo da base de dados
- `frontend`: pagina web simples (HTML + JS + Leaflet)
- `frontend/supervisor.html`: dashboard supervisor

## 1) Base de dados

O ficheiro `database/schema.sql` é **idempotente** (só `CREATE TABLE IF NOT EXISTS` e `ALTER ... ADD COLUMN IF NOT EXISTS`). Pode ser reaplicado **sem apagar** motoristas, escalas nem histórico de serviços.

### Desenvolvimento com Docker (recomendado — dados persistentes)

Na raiz do projecto:

```bash
docker compose up -d
```

Isto cria o PostgreSQL com um **volume Docker** (`bus_platform_pgdata`). Os dados mantêm-se entre `git pull`, alterações ao código e reinícios do contentor. **Evite** `docker compose down -v` (o `-v` apaga o volume e perde tudo).

Configure `backend/.env` com:

```env
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/bus_platform
```

Aplicar o schema (primeira vez ou após novas colunas/tabelas no repositório):

```bash
cd backend
npm run db:apply
```

### Sem Docker (PostgreSQL já instalado)

1. Criar a base `bus_platform`
2. `cd backend && npm run db:apply` **ou** em psql: `\i database/schema.sql`

### Produção

- Use uma instância PostgreSQL **dedicada** (servidor ou serviço gerido). O `DATABASE_URL` no `backend/.env` do servidor aponta para essa instância; **não** recrie a base em cada deploy.
- O deploy por SSH **não** copia o `.env` — a ligação à BD mantém-se.
- Para backups: `pg_dump` periódico (ex.: diário) para ficheiro fora do servidor de aplicações. No Windows, com `pg_dump` no PATH: `.\scripts\backup-database.ps1`.
- Nunca execute `DROP DATABASE`, `TRUNCATE` em tabelas de negócio ou `docker compose down -v` contra a BD de produção.

## 2) Backend

```bash
cd backend
cp .env.example .env
npm install
npm run dev
```

API em `http://localhost:4000`.

## 3) Frontend

Como e um frontend estatico, pode abrir `frontend/index.html` no browser
ou servir por um servidor simples.

## Arranque rapido (Windows)

Com tudo instalado, podes arrancar backend + frontend com:

```powershell
cd c:\Projetos\Cursor_projetos
.\start-all.ps1
```

## Endpoints principais

- `POST /auth/register`
- `POST /auth/login`
- `GET /services` (auth)
- `POST /services/start` (auth)
- `POST /services/:serviceId/points` (auth)
- `POST /services/:serviceId/end` (auth)
- `GET /services/:serviceId` (auth)
- `GET /services/today-planned` (auth driver)
- `GET /services/pending-handover` (auth driver)
- `GET /services/history-detailed` (auth driver)
- `POST /services/:serviceId/handover` (auth driver em servico)
- `POST /services/:serviceId/resume` (auth driver destino)
- `GET /supervisor/overview` (supervisor/admin)
- `GET /supervisor/services` (supervisor/admin + filtros)
- `GET /supervisor/services/export.csv` (supervisor/admin + filtros)
- `GET /supervisor/drivers` (supervisor/admin)
- `POST /supervisor/drivers` (supervisor/admin)
- `PATCH /supervisor/drivers/:driverId` (supervisor/admin, ativar/desativar)
- `POST /supervisor/drivers/import` (supervisor/admin)
- `GET /supervisor/drivers/import-template.csv` (supervisor/admin)
- `GET /supervisor/drivers/import-template.xlsx` (supervisor/admin)
- `GET /supervisor/drivers/export.csv?company=` (supervisor/admin)
- `GET /supervisor/drivers/export.xlsx?company=` (supervisor/admin)

## Fluxo de utilizacao

1. Registar motorista
2. Fazer login
3. Preencher cabecalho do servico
4. Clicar "Iniciar viagem"
5. Permitir geolocalizacao no browser
6. Clicar "Finalizar viagem"
7. Consultar resumo e historico com kms e percurso

## Fase 1 (escala diaria)

- Motorista pode autenticar com `numero mecanografico + password`
- App mostra `servicos previstos de hoje`
- Motorista seleciona o servico e inicia viagem sem preencher manualmente

## Fase 2.1 (continuidade em avaria/acidente)

- Transferencia de servico em curso sem perder kms acumulados
- Criacao de segmentos por servico (`service_segments`)
- Novo motorista pode assumir servico pendente de handover
- Troca de viatura e/ou motorista com motivo e notas

## Perfis de utilizador

- `driver`: perfil padrao para operacao da viagem
- `supervisor`: acesso ao dashboard operacional
- `admin`: acesso total ao dashboard

Para criar um supervisor/admin no registo:

```json
{
  "name": "Supervisor 1",
  "email": "supervisor@empresa.pt",
  "password": "123456",
  "role": "supervisor"
}
```

## Dashboard supervisor (fase 2)

- Abrir `frontend/supervisor.html`
- Login com utilizador de perfil `supervisor` ou `admin`
- Aplicar filtros por `driverId`, `lineCode`, `status`, `fromDate`, `toDate`
- Exportar CSV compatível com Excel

## GitHub (CI e deploy)

- **CI**: em cada push ou PR para `main`/`master`, o workflow instala dependências do `backend` (`npm ci`) e valida sintaxe de `src/server.js`.
- **Deploy por SSH**: workflow `Deploy (SSH)` — corre manualmente em *Actions* (*Run workflow*) ou em cada push para `main` (pode desactivar o `push` em `.github/workflows/deploy-ssh.yml` se preferir só deploy manual).

Configure no GitHub **Settings → Secrets and variables → Actions**:

| Secret | Descrição |
|--------|-----------|
| `DEPLOY_HOST` | Hostname ou IP do servidor |
| `DEPLOY_USER` | Utilizador SSH |
| `DEPLOY_SSH_KEY` | Chave privada PEM com acesso ao servidor |
| `DEPLOY_REMOTE_PATH` | Caminho absoluto no servidor (ex.: `/var/www/bus-platform`) |
| `DEPLOY_POST_COMMAND` | *(opcional)* Uma linha a correr na raiz do projecto após `npm ci`, ex.: `pm2 restart bus-api` ou `sudo systemctl restart nome-do-servico` |

No servidor, crie uma vez o ficheiro `backend/.env` de produção (não vai no Git). O rsync **não** sobrescreve `.env`.

Após cada deploy que inclua alterações em `database/schema.sql`, pode correr no servidor (na pasta do projecto) `cd backend && npm run db:apply` para aplicar novas tabelas/colunas **sem** apagar dados existentes.

Repositório local:

```bash
git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin https://github.com/SEU_USER/SEU_REPO.git
git push -u origin main
```

## Proximos passos recomendados

- Criar app mobile (Android/iOS) para GPS mais confiavel em segundo plano
- Adicionar validacoes de negocio (turnos, tempos maximos, etc.)
- Criar dashboard de supervisao
- Exportacao PDF/Excel dos servicos
