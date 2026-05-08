import streamlit as st
import base64
from pathlib import Path
from pathlib import Path
from auth import (
    user_exists, create_user, verify_login, update_user_tokens, EMAIL_SUFFIX
)

def get_image_base64(path_str):
    try:
        with open(path_str, "rb") as f:
            return base64.b64encode(f.read()).decode("ascii")
    except:
        return ""

def render_auth_ui():
    st.markdown("""
        <style>
        .auth-container {
            max-width: 400px;
            margin: 0 auto;
            padding: 2rem;
            border-radius: 12px;
            box-shadow: 0 4px 14px rgba(31, 42, 68, 0.07);
            background: white;
            border: 1px solid rgba(31, 42, 68, 0.12);
        }
        .auth-title {
            text-align: center;
            color: #1f2a44;
            margin-bottom: 0.5rem;
        }
        .auth-subtitle {
            text-align: center;
            color: #666;
            margin-bottom: 2rem;
            font-size: 0.9rem;
        }
        </style>
    """, unsafe_allow_html=True)
    
    if "auth_mode" not in st.session_state:
        st.session_state.auth_mode = "login" # login, signup, forgot, 2fa_login, 2fa_signup, 2fa_forgot
    if "temp_email" not in st.session_state:
        st.session_state.temp_email = ""
    if "temp_password" not in st.session_state:
        st.session_state.temp_password = ""

    logo_b64 = get_image_base64("assets/logo_rjzcyrela_branco.png")
    header_html = f"""
    <div style="padding: 10px 0 30px 0; text-align: center;">
        <div style="display: flex; align-items: center; justify-content: center; gap: 20px;">
            <h2 style="color: #f0783d; margin: 0; font-size: 42px; font-weight: 800; text-shadow: 0 2px 4px rgba(0,0,0,0.2);">Genie</h2>
            <img src="data:image/png;base64,{logo_b64}" style="height: 65px; filter: drop-shadow(0 2px 6px rgba(0,0,0,0.4));">
        </div>
    </div>
    """
    st.markdown(header_html, unsafe_allow_html=True)

    mode = st.session_state.auth_mode

    if mode == "login":
        _render_login()
    elif mode == "signup":
        _render_signup()
    elif mode == "forgot":
        _render_forgot()
    elif mode == "2fa_login":
        _render_2fa("login")
    elif mode == "2fa_signup":
        _render_2fa("signup")
    elif mode == "2fa_forgot":
        _render_2fa("forgot")
    elif mode == "reset_password":
        _render_reset_password()

def _render_login():
    st.subheader("Login")
    email_prefix = st.text_input("E-mail corporativo", placeholder="seu.nome")
    password = st.text_input("Senha", type="password", autocomplete="new-password")
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    if st.button("Entrar", type="primary", use_container_width=True):
        if not email_prefix or not password:
            st.error("Preencha todos os campos.")
            return
            
        if email_prefix.strip().lower() == "admin":
            full_email = "admin"
        else:
            prefix = email_prefix.strip().lower()
            full_email = prefix + EMAIL_SUFFIX if not prefix.endswith(EMAIL_SUFFIX) else prefix
        
        login_result = verify_login(full_email, password)
        if login_result["success"]:
            st.session_state.authenticated = True
            st.session_state.user_email = full_email
            st.session_state.is_admin = login_result["is_admin"]
            st.rerun()
        else:
            st.error("E-mail ou senha incorretos.")

    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Esqueceu a senha?", use_container_width=True):
            st.session_state.auth_mode = "forgot"
            st.rerun()
    with col2:
        if st.button("Cadastrar nova conta", use_container_width=True):
            st.session_state.auth_mode = "signup"
            st.rerun()

def _render_signup():
    st.subheader("Cadastro")
    email_prefix = st.text_input("E-mail corporativo", placeholder="seu.nome")
    password = st.text_input("Senha", type="password", autocomplete="new-password")
    password_confirm = st.text_input("Confirmar Senha", type="password", autocomplete="new-password")
    
    if st.button("Voltar para Login"):
        st.session_state.auth_mode = "login"
        st.rerun()

    if st.button("Cadastrar", type="primary", use_container_width=True):
        if not email_prefix or not password or not password_confirm:
            st.error("Preencha todos os campos.")
            return
        if password != password_confirm:
            st.error("As senhas não coincidem.")
            return
            
        prefix = email_prefix.strip().lower()
        full_email = prefix + EMAIL_SUFFIX if not prefix.endswith(EMAIL_SUFFIX) else prefix
        
        if user_exists(full_email):
            st.error("Este e-mail já está cadastrado.")
            return
            
        create_user(full_email, password)
        st.success("Cadastro realizado com sucesso!")
        st.session_state.authenticated = True
        st.session_state.user_email = full_email
        st.session_state.is_admin = False
        st.rerun()

def _render_forgot():
    st.subheader("Recuperar Senha")
    st.info("O envio automático de e-mails está desativado. Por favor, entre em contato com o administrador (admin) para redefinir a sua senha.")
    
    if st.button("Voltar para Login"):
        st.session_state.auth_mode = "login"
        st.rerun()

def render_token_setup_ui(email, current_host, current_token, current_space, current_ado_org, current_ado_proj, current_ado_repo, current_ado_pat):
    st.markdown("""
        <div style="background-color: #1f2a44; padding: 20px; text-align: center; border-radius: 12px 12px 0 0; margin-bottom: 20px;">
            <h2 style="color: white; margin: 0;">Configuração Inicial</h2>
            <p style="color: #ccc; margin: 0; font-size: 14px;">Defina suas credenciais individuais do Databricks e Azure DevOps</p>
        </div>
    """, unsafe_allow_html=True)
    
    st.info("Para usar o Genie e todas as integrações, você precisa configurar seus tokens pessoais. Essas informações ficarão salvas e você não precisará fazer isso novamente.")
    
    with st.expander("🤔 Onde encontro e qual o padrão dessas informações?", expanded=False):
        st.markdown("""
        ### 🔹 Databricks
        
        **1. Databricks Host**
        * **Padrão:** `https://adb-<numero-da-conta>.<id-regiao>.azuredatabricks.net`
        * **Como conseguir:** É a URL base que você acessa no seu navegador quando abre o workspace do Databricks da Cyrela.
        
        **2. Databricks Token (PAT)**
        * **Padrão:** Começa com `dapi...`
        * **Como conseguir:** 
          1. Abra o Databricks.
          2. Clique no seu perfil (canto superior direito) -> **Settings**.
          3. Vá em **Developer** -> **Access tokens**.
          4. Clique em **Generate new token**.
          5. No campo **Scope**, selecione **Other APIs**.
          6. No campo **API scope(s)**, selecione **all-apis**.
          7. Clique em **Generate**, copie o token e cole aqui.
        
        <hr>
        
        ### 🔹 Azure DevOps
        
        **4. ADO_ORG (Organização)**
        * **Padrão:** `cyrela-data-analytics`
        * **Como conseguir:** É o nome da organização que aparece na URL do Azure DevOps: `dev.azure.com/<SUA_ORGANIZACAO>`
        
        **5. ADO_PROJECT (Projeto)**
        * **Padrão:** `Data Analytics`
        * **Como conseguir:** É o nome do projeto onde o repositório está localizado.
        
        **6. ADO_REPO (Repositório)**
        * **Padrão:** `lakehouse`
        * **Como conseguir:** É o nome do repositório de código (git) específico que o Genie vai acessar.
        
        **7. ADO_PAT (Personal Access Token)**
        * **Padrão:** Um código longo criptografado.
        * **Como conseguir:** 
          1. Acesse o [Azure DevOps](https://dev.azure.com).
          2. Clique no ícone de "Configurações de Usuário" (engrenagem com uma pessoa) ao lado do seu avatar.
          3. Selecione **Personal Access Tokens**.
          4. Clique em **New Token**. Dê um nome, selecione as permissões (ex: Code: Read & Write) e crie.
        """, unsafe_allow_html=True)

    with st.form("token_setup_form", clear_on_submit=True):
        st.subheader("Integração Databricks")
        host = st.text_input("Databricks Host", value=current_host or "https://adb-3762468175228684.4.azuredatabricks.net")
        token = st.text_input("Databricks Token (dapi...)", value=current_token, type="password", autocomplete="new-password")
        
        st.markdown("<hr>", unsafe_allow_html=True)
        st.subheader("Integração Azure DevOps")
        ado_org = st.text_input("ADO_ORG (Organização)", value=current_ado_org or "cyrela-data-analytics")
        ado_proj = st.text_input("ADO_PROJECT (Projeto)", value=current_ado_proj or "Data Analytics")
        ado_repo = st.text_input("ADO_REPO (Repositório)", value=current_ado_repo or "lakehouse")
        ado_pat = st.text_input("ADO_PAT (Personal Access Token)", value=current_ado_pat, type="password", autocomplete="new-password")
        
        submit = st.form_submit_button("Salvar Configurações", type="primary", use_container_width=True)
        
        if submit:
            if not host or not token or not ado_org or not ado_proj or not ado_repo or not ado_pat:
                st.error("Por favor, preencha todos os campos obrigatórios.")
            else:
                # O space_id pode ser vazio na configuração inicial
                update_user_tokens(email, host, token, current_space or "", ado_org, ado_proj, ado_repo, ado_pat)
                
                # Atualizar a sessão para que o main() pegue esses valores
                st.session_state.config_host = host
                st.session_state.config_token = token
                st.session_state.config_space_id = current_space or ""
                
                st.session_state.config_devops_org = ado_org
                st.session_state.config_devops_proj = ado_proj
                st.session_state.config_devops_repo = ado_repo
                st.session_state.config_devops_pat = ado_pat
                
                st.session_state.needs_token_setup = False
                st.rerun()
