# Databricks Genie API Client (Python)

Projeto pronto para conversar com um Genie Space via API REST do Databricks.

## O que este projeto faz

- Inicia conversa no Genie.
- Faz polling ate status terminal (`COMPLETED`, `FAILED`, `CANCELLED`, `QUERY_RESULT_EXPIRED`).
- Mostra attachments retornados pelo Genie.
- Tenta buscar preview de resultado SQL por attachment.
- Permite follow-up mantendo contexto da conversa.
- Oferece interface web local para chat com tabela, graficos e insights automaticos.

## Requisitos

- Python 3.10+
- Acesso ao Databricks com permissao no Genie Space
- Token OAuth/PAT valido

## Setup rapido (PowerShell)

```powershell
cd C:\Users\mateus.daniel\genie-databricks-api-client
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item .env.example .env
```

Depois, edite o arquivo `.env` e preencha os campos obrigatorios.

## Parametros de conexao (.env)

- `DATABRICKS_HOST`: URL do workspace Databricks
  - Exemplo Azure: `https://adb-xxxxxxxxxxxxxxxx.x.azuredatabricks.net`
- `DATABRICKS_TOKEN`: bearer token (OAuth recomendado)
- `GENIE_SPACE_ID`: UUID do Genie Space
- `GENIE_POLL_SECONDS` (opcional): intervalo de polling (default `2`)
- `GENIE_TIMEOUT_SECONDS` (opcional): timeout por pergunta em segundos (default `600`)

## Como rodar

### Modo interativo

```powershell
cd C:\Users\mateus.daniel\genie-databricks-api-client
.\.venv\Scripts\Activate.ps1
python genie_chat.py
```

### Modo web visual (Genie Local Studio)

```powershell
cd C:\Users\mateus.daniel\genie-databricks-api-client
.\.venv\Scripts\Activate.ps1
streamlit run genie_web_app.py
```

Ou alternativa (use quando preferir executar via python -m):

```powershell
python -m streamlit run genie_web_app.py
```

Recursos da interface web:

- Chat continuo com contexto de conversa (similar ao Genie Space).
- Modo `Usuario`: exibe resposta e datasets retornados pelo Genie.
- Modo `Desenvolvedor`: exibe query SQL, tabela completa, graficos e insights gerados pelo proprio Genie.
- Tabela exibida sem limitacao artificial de linhas no app (mostra tudo que o Genie retornar na resposta).
- Download por dataset em Excel (`.xlsx`) e download consolidado de relatorio por resposta.
- Preview tabular com fallback HTML caso `pyarrow` falhe no ambiente local.
- Perguntas sugeridas clicaveis para follow-up rapido.

Avatar customizado do agente:

- Salve sua imagem em `assets/agent_photo.png`.
- O app recorta automaticamente o centro em formato quadrado e usa no chat.

### Uma pergunta e sair

```powershell
python genie_chat.py --question "Top 10 clientes por receita no ultimo mes" --no-followup
```

## Argumentos uteis

- `--poll-seconds 2`
- `--timeout-seconds 600`
- `--max-rows 20`

## Dicas para operacao analitica

- Se quiser respostas mais tecnicas no modo web, ative `Modo analitico avancado` na barra lateral.
- Se trocar de tema/pergunta de negocio, clique em `Nova conversa` para resetar contexto do Genie.
- Se o chat estiver com muito historico, use `Limpar chat` para reduzir a carga visual local.

## Troubleshooting rapido

- `401 Unauthorized`: token invalido/expirado.
- `403 Forbidden`: falta permissao no Genie Space ou nos dados.
- `404 Not Found`: `GENIE_SPACE_ID` errado ou workspace errado.
- Timeout: aumente `GENIE_TIMEOUT_SECONDS`.
