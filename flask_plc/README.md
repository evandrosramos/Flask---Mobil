# 🏭 SAE Oftálmica 02 — Sistema de Monitoramento

Dashboard web em tempo real para monitoramento do CLP **Allen-Bradley CompactLogix 1769-L32E**
da máquina SAE Oftálmica 02, desenvolvido com **Flask + pylogix + PostgreSQL**.

---

## 📋 Descrição

O sistema coleta tags do CLP via rede Ethernet industrial (protocolo EtherNet/IP),
armazena os dados em banco PostgreSQL local e exibe um dashboard web com atualização
automática via APIs REST.

---

## 🗂️ Estrutura do Projeto

---

## 🚀 Tecnologias

| Camada            | Biblioteca / Tecnologia                |
| ----------------- | -------------------------------------- |
| Comunicação CLP   | `pylogix` — EtherNet/IP                |
| Banco de dados    | `PostgreSQL` via `pgserver` (embutido) |
| ORM               | `SQLAlchemy`                           |
| Driver PostgreSQL | `psycopg2`                             |
| Servidor web      | `Flask`                                |
| Frontend          | HTML5 + CSS3 + JavaScript + `Chart.js` |

---

## 🗄️ Banco de Dados

| Tabela                    | Descrição                                            |
| ------------------------- | ---------------------------------------------------- |
| `status_maquina`          | Status geral: modos, segurança, motions, módulos     |
| `contadores_producao`     | Contadores e estatísticas de produção                |
| `status_estacoes`         | Status das sequências por estação                    |
| `alarmes_snapshot`        | Snapshot das palavras de alarme                      |
| `alarmes_evento`          | Histórico de ativação/desativação de alarmes         |
| `alarmes_estado_anterior` | Estado anterior dos alarmes (detecção de borda)      |
| `tempos_ciclo`            | Tempo de ciclo por estação com início, fim e duração |

---

## 🔌 APIs REST

| Endpoint            | Intervalo | Descrição                                  |
| ------------------- | --------- | ------------------------------------------ |
| `GET /`             | —         | Dashboard principal (Jinja2)               |
| `GET /api/status`   | 3 s       | Status da máquina + contadores             |
| `GET /api/estacoes` | 2 s       | Status das estações + métricas de tempo    |
| `GET /api/alarmes`  | 10 s      | Histórico de eventos de alarme             |
| `GET /api/grafico`  | 30 s      | Série histórica para gráficos (últimas 8h) |

---

## ⚙️ Configuração

### Pré-requisitos

```bash pip install flask pylogix sqlalchemy psycopg2-binary pgserver

CLP_IP   = '192.168.100.1'
SLOT     = 0
CICLO_STATUS_S   = 3
CICLO_ESTACOES_S = 2

# Terminal 1 — Coletor de dados
python coletor.py

# Terminal 2 — Servidor web
python app.py

```
