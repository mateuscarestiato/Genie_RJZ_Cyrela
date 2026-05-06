# 🚀 Genie RJZ Cyrela — Portal do Desenvolvedor Databricks

Plataforma interativa para consulta de dados, análise de esquemas e otimização de queries SQL, conectada ao Databricks Genie.

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)](https://python.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.44+-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io)
[![Databricks](https://img.shields.io/badge/Databricks-Genie_API-FF3621?logo=databricks&logoColor=white)](https://www.databricks.com)
[![License](https://img.shields.io/badge/Licença-Privado-lightgrey)]()

---

## 📋 Índice

- [Visão Geral](#-visão-geral)
- [Funcionalidades](#-funcionalidades)
- [Arquitetura](#-arquitetura)
- [Pré-Requisitos](#-pré-requisitos)
- [Instalação](#-instalação)
- [Configuração](#-configuração)
- [Como Executar](#-como-executar)
- [Guia de Uso](#-guia-de-uso)
- [Troubleshooting](#-troubleshooting)
- [Dependências](#-dependências)

---

## 🎯 Visão Geral

O **Genie RJZ Cyrela** é um portal de desenvolvimento que centraliza ferramentas essenciais para o time de Engenharia de Dados. Ele se conecta diretamente ao **Databricks Genie** via API REST e oferece uma interface web moderna construída com **Streamlit**, permitindo:

- Conversar com os dados usando linguagem natural (Genie Chat).
- Explorar metadados e qualidade de tabelas automaticamente.
- Revisar e otimizar queries SQL com sugestões inteligentes.
- Comparar esquemas entre ambientes (Dev vs Prod).
- Visualizar linhagem de dados com grafos interativos.
- **Automatizar o deploy de modelos dbt com abertura automática de Pull Requests no Azure DevOps.**
- **Analisar o impacto de mudanças de query em dashboards de BI.**
- **Mapear colunas legadas para o ambiente atual com tabelas de confiança.**
- **Converter XMLs de Localização Avançada do CRM para SQL Databricks.**
- **Provisionar novos Genie Spaces programaticamente via API.**

---

## ✨ Funcionalidades

### 💬 Genie Chat

Converse com seus dados usando linguagem natural. O Genie interpreta sua pergunta, gera a query SQL, executa no Databricks e retorna os resultados em tabela e gráficos.

| Recurso | Descrição |
| --- | --- |
| **Modo Usuário** | Respostas simples, linguagem de negócio |
| **Modo Desenvolvedor** | SQL completo, métricas, rastreabilidade |
| **Gráficos automáticos** | Gerados pelo Genie com base nos dados |
| **Download** | Exportação em Excel (`.xlsx`) e CSV |
| **Follow-up** | Perguntas sugeridas clicáveis para manter o contexto |

---

### 📚 Dicionário e Perfil de Dados (Profiling)

Selecione qualquer tabela do schema `dev.iops_rj` e obtenha automaticamente:

- **Esquema completo** — nome, tipo e comentário de cada coluna.
- **Perfil estatístico** — contagem de nulos, valores distintos, mínimos e máximos por coluna.

> 💡 A lista de tabelas é carregada dinamicamente via `SHOW TABLES IN dev.iops_rj`.

---

### ⚡ Otimizador e Revisor SQL (Linter)

Cole qualquer query SQL e receba uma análise técnica completa via Genie:

1. **Resumo** do que a query faz.
2. **Sugestões de performance** — particionamento, Z-Order, hints de join, eliminação de cross joins.
3. **Boas práticas** — legibilidade, padrões de código, nomeação.
4. **Query refatorada** — versão otimizada pronta para uso.

---

### ⚖️ Comparador de Ambientes (Dev vs Prod)

Informe duas tabelas (ex: `dev.iops_rj.tabela` e `prd.iops_rj.tabela`) e obtenha um relatório coluna a coluna:

| Status | Significado |
| --- | --- |
| ✅ Iguais | Coluna existe em ambos com o mesmo tipo |
| ⚠️ Tipo Divergente | Coluna existe em ambos, mas com tipos diferentes |
| 🛑 Falta no DEV | Coluna existe somente na PROD |
| 🛑 Falta no PROD | Coluna existe somente no DEV |

---

### 🛡️ Analisador de Impacto em BI

Evite quebras em dashboards de Power BI antes de realizar o deploy. Esta ferramenta compara a query atual com a proposta e identifica:
- **Remoção de colunas** (Risco Crítico).
- **Renomeação de campos** (Risco Crítico).
- **Mudança de tipos de dados** (Risco de Alerta).
- **Adição de campos** (Risco Seguro).

Retorna um status visual (`CRÍTICO`, `ALERTA` ou `SEGURO`) para apoiar a decisão do Engenheiro de Dados.

---

### 🚀 DevOps & CI/CD Hub (Auto-PR)

Integração direta com o **Azure DevOps** para automatizar o ciclo de vida do dbt.
- **Git CLI Workflow**: Realiza um clone completo da branch `dev` para garantir integridade.
- **Auto-Commit**: Salva o arquivo `.sql` (Jinja) e `.yml` (Doc) nas pastas corretas (`dbt/models/` e `dbt/models/*/docs/`).
- **Auto-PR**: Abre automaticamente um Pull Request apontado para `dev` ou `main` com um clique.
- **Status em Tempo Real**: Visualize o último commit e autor da branch alvo antes de enviar.

---

### 🌠 Criar Novo Genie Space (API)

Ferramenta administrativa para criar novos espaços do Genie sem sair do portal.
- Navegação via Unity Catalog para seleção de tabelas.
- Geração automática do `serialized_space` JSON.
- Ordenação automática de identificadores de tabelas (Requisito da API Databricks).

---

### 🔍 Mapeador de Colunas (Legacy → Atual)

Compare uma query antiga ou uma lista de colunas legadas com o esquema atual do seu Genie Space para identificar as correspondências corretas.
- Sugestões de nomes atuais.
- Nível de confiança no mapeamento.
- Explicações detalhadas de de-para.

---

### 🏹 Conversor CRM XML → SQL

Converta um XML de 'Localização Avançada' do CRM em uma query SQL/dbt compatível com o ambiente atual.
- Mapeamento automático de entidades CRM para tabelas Unity Catalog.
- Tradução de filtros e joins complexos.

---

### 🔗 Linhagem de Dados

Visualize graficamente as relações entre tabelas usando grafos interativos (Plotly), com tamanho fixo de 1200×800px para leitura clara dos nomes e conexões.

---

## 🏗 Arquitetura

```text
📦 Genie_RJZ_Cyrela/
├── 📄 genie_web_app.py       # Interface Streamlit (Portal principal)
├── 📄 genie_chat.py           # Client da API REST do Databricks Genie
├── 📄 run_streamlit.py        # Script auxiliar de execução
├── 📄 start_streamlit.bat     # Atalho Windows para iniciar o app
├── 📄 requirements.txt        # Dependências Python
├── 📄 .env.example            # Template de variáveis de ambiente
├── 📄 .gitignore              # Arquivos ignorados pelo Git
├── 📁 assets/                 # Imagens (logos, avatares, capas)
└── 📁 .streamlit/             # Configuração do Streamlit (tema)
```

---

## 📌 Pré-Requisitos

- **Python** 3.10 ou superior
- **Acesso** ao workspace Databricks com permissão no Genie Space
- **Token** OAuth ou PAT válido

---

## 🔧 Instalação

```bash
# 1. Clone o repositório
git clone https://github.com/mateuscarestiato/Genie_RJZ_Cyrela.git
cd Genie_RJZ_Cyrela

# 2. Crie e ative um ambiente virtual
python -m venv .venv
```

**Windows (PowerShell):**

```powershell
.\.venv\Scripts\Activate.ps1
```

**Linux / macOS:**

```bash
source .venv/bin/activate
```

```bash
# 3. Instale as dependências
pip install -r requirements.txt

# 4. Crie o arquivo de configuração
cp .env.example .env
```

---

## ⚙️ Configuração

Edite o arquivo `.env` com suas credenciais:

```env
# ─── Obrigatórios (Databricks) ──────────────────
DATABRICKS_HOST=https://adb-xxxxxxxxxxxxxxxx.x.azuredatabricks.net
DATABRICKS_TOKEN=seu_token_aqui
GENIE_SPACE_ID=uuid-do-seu-genie-space

# ─── DevOps (Opcional) ──────────────────────────
ADO_PAT=seu_pat_do_devops_aqui
ADO_ORG=cyrela-data-analytics
ADO_PROJECT=Data Analytics
ADO_REPO=lakehouse

# ─── Configurações do App ───────────────────────
GENIE_POLL_SECONDS=2          # Intervalo de polling (default: 2s)
GENIE_TIMEOUT_SECONDS=600     # Timeout por pergunta (default: 600s)
```

### Como obter o Token

1. Acesse seu workspace Databricks.
2. Vá em **Settings** → **Developer** → **Access tokens**.
3. Clique em **Generate new token**, copie o valor e cole no `.env`.

### Como obter o Genie Space ID

1. Abra o **Genie Space** no Databricks.
2. O UUID aparece na URL: `https://adb-xxx.azuredatabricks.net/ml/genie/rooms/<GENIE_SPACE_ID>`.

---

## ▶️ Como Executar

### Interface Web (recomendado)

```bash
streamlit run genie_web_app.py
```

Ou via Python:

```bash
python -m streamlit run genie_web_app.py
```

> O app abrirá automaticamente no navegador em `http://localhost:8501`.

### Modo CLI (linha de comando)

```bash
# Chat interativo
python genie_chat.py

# Pergunta única
python genie_chat.py --question "Top 10 clientes por receita" --no-followup
```

**Argumentos opcionais do CLI:**

| Argumento | Padrão | Descrição |
| --- | --- | --- |
| `--poll-seconds` | `2` | Intervalo de polling |
| `--timeout-seconds` | `600` | Timeout por pergunta |
| `--max-rows` | `20` | Linhas exibidas no terminal |
| `--no-followup` | — | Sair após a primeira resposta |

---

## 📖 Guia de Uso

### Navegação

No modo **Desenvolvedor**, a barra lateral apresenta o menu **"Ferramentas do Dev"** com as seguintes ferramentas organizadas por fluxo:

1.  **🌠 Criar Novo Genie Space (API)** — Setup de novos espaços.
2.  **💬 Genie Chat** — Chat com dados via linguagem natural.
3.  **📚 Dicionário e Perfil de Dados** — Esquema + profiling.
4.  **🛠️ Gerador de Modelos dbt/Jinja** — Criação de modelos `.sql`.
5.  **📄 Gerador de Documentação (.yml)** — Criação de `schema.yml`.
6.  **⚡ Otimizador e Revisor SQL** — Sugestões de performance.
7.  **🔍 Mapeador de Colunas** — De-para de campos legados.
8.  **🏹 Conversor CRM XML → SQL** — Tradução de FetchXML.
9.  **⚖️ Comparador de Ambientes** — Diferenças entre Dev e Prod.
10. **🛡️ Analisador de Impacto em BI** — Riscos de quebra de dashboards.
11. **🚀 DevOps & CI/CD Hub** — Deploy e PRs automáticos.

### Dicas

- 🔄 Use **"Nova conversa"** ao trocar de assunto para resetar o contexto do Genie.
- 📊 Ative o **"Modo analítico avançado"** para respostas mais detalhadas.
- 🖼️ Personalize o avatar do agente salvando uma imagem em `assets/agent_photo.png`.

---

## 🛠 Troubleshooting

| Erro | Causa provável | Solução |
| --- | --- | --- |
| `401 Unauthorized` | Token inválido ou expirado | Gere um novo token no Databricks |
| `403 Forbidden` | Sem permissão no Genie Space | Solicite acesso ao administrador |
| `404 Not Found` | `GENIE_SPACE_ID` incorreto | Verifique o UUID na URL do Genie Space |
| Timeout | Query muito pesada | Aumente `GENIE_TIMEOUT_SECONDS` |
| `PERMISSION_DENIED` | Sem acesso ao catálogo remoto | Solicite permissão no catálogo (`prd`) |

---

## 🧰 Dependências

| Pacote | Versão mínima | Finalidade |
| --- | --- | --- |
| `requests` | 2.32.0 | Chamadas HTTP à API do Databricks |
| `python-dotenv` | 1.0.1 | Leitura de variáveis do `.env` |
| `streamlit` | 1.44.0 | Interface web interativa |
| `pandas` | 2.2.0 | Manipulação de DataFrames |
| `plotly` | 5.24.0 | Gráficos interativos e linhagem |
| `pillow` | 10.4.0 | Processamento de imagens (avatares) |
| `openpyxl` | 3.1.5 | Exportação para Excel |

## 📈 Ganhos e Impacto (KPIs)

Para medir o sucesso do projeto, acompanhamos os seguintes indicadores:

| Pilar | KPI | Impacto Esperado |
| --- | --- | --- |
| **Produtividade** | Time-to-Answer (TTA) | Redução de >80% no tempo para obter respostas de dados. |
| **Eficiência** | Custo de DBUs (Databricks) | Redução de custos via queries otimizadas pelo Linter. |
| **Autonomia** | % de Self-Service | Aumento no número de usuários de negócio consultando dados sem auxílio técnico. |
| **Qualidade** | Erros de Schema em Prod | Redução de falhas em produção via Comparador de Ambientes. |

---

**Desenvolvido pelo time de Engenharia de Dados — RJZ Cyrela**

