# ⚠️ Erro de Autenticação - Como Resolver

## O Problema
A aplicação retorna o erro:
```
Credential was not sent or was of an unsupported type for this API. [Status: 401]
```

Isso ocorre porque o **DATABRICKS_TOKEN** não foi configurado corretamente.

## A Solução

### Opção 1: Preencher via Interface (Mais Rápido)
1. Abra a aplicação em `http://127.0.0.1:8501`
2. Na **barra lateral esquerda**, localize o campo **DATABRICKS_TOKEN**
3. Cole seu token válido do Databricks
4. O campo salvará automaticamente

### Opção 2: Editar o Arquivo .env (Persistente)
1. Abra o arquivo `.env` na raiz do projeto
2. Substitua esta linha:
   ```
   DATABRICKS_TOKEN=
   ```
   Por:
   ```
   DATABRICKS_TOKEN=seu_token_aqui
   ```
3. Salve o arquivo (Ctrl+S)
4. A aplicação recarregará automaticamente

## Como Obter um Token do Databricks

1. **Acesse seu workspace Databricks**
   - URL: https://adb-3762468175228684.4.azuredatabricks.net

2. **Clique em seu perfil** (canto superior direito)

3. **Navegue para:**
   - Settings → Developer → Access tokens

4. **Clique em "Generate new token"**

5. **Copie o token gerado**
   - ⚠️ O token só será exibido uma vez! Guarde-o com segurança

6. **Cole na aplicação** (Opção 1 ou 2 acima)

## Configurações Verificadas ✅
- ✅ DATABRICKS_HOST: `https://adb-3762468175228684.4.azuredatabricks.net`
- ✅ GENIE_SPACE_ID: `01f138326bf618b1b49762b4aeab3212`
- ❌ DATABRICKS_TOKEN: **PRECISA SER PREENCHIDO**

## Após Configurar o Token
1. A aplicação carregará as tabelas do Genie Space
2. Você poderá iniciar conversas com o Genie
3. Será possível visualizar linhagem de dados

## Dúvidas?
- Verifique se o token não expirou
- Certifique-se que tem permissões no Genie Space
- Tente regenerar um novo token
