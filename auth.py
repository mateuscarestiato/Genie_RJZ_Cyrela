import os
import hashlib
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import random
from datetime import datetime, timedelta
import streamlit as st

# ---- NOVO: Supabase e Criptografia ----
from supabase import create_client, Client
from cryptography.fernet import Fernet

EMAIL_SUFFIX = "@rjzcyrela.com.br"

def get_supabase() -> Client:
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        st.error("ERRO: SUPABASE_URL ou SUPABASE_KEY não configurado nas variáveis de ambiente.")
    return create_client(url, key)

def get_cipher() -> Fernet:
    key = os.environ.get("ENCRYPTION_KEY")
    if not key:
        st.warning("ENCRYPTION_KEY não configurada. Usando chave temporária (tokens serão perdidos ao reiniciar).")
        key = Fernet.generate_key().decode('utf-8')
        os.environ["ENCRYPTION_KEY"] = key
    return Fernet(key.encode('utf-8'))

def encrypt_token(token: str) -> str:
    if not token:
        return ""
    cipher = get_cipher()
    return cipher.encrypt(token.encode('utf-8')).decode('utf-8')

def decrypt_token(encrypted_token: str) -> str:
    if not encrypted_token:
        return ""
    try:
        cipher = get_cipher()
        return cipher.decrypt(encrypted_token.encode('utf-8')).decode('utf-8')
    except Exception as e:
        print(f"Erro ao descriptografar token: {e}")
        return ""

def init_db():
    try:
        supabase = get_supabase()
        admin_email = "admin"
        admin_pass = os.getenv("ADMIN_PASSWORD", "admin123")
        
        response = supabase.table("users").select("email").eq("email", admin_email).execute()
        if not response.data:
            supabase.table("users").insert({
                "email": admin_email,
                "password_hash": hash_password(admin_pass),
                "is_admin": 1,
                "databricks_host": "",
                "databricks_token": encrypt_token(""),
                "genie_space_id": "",
                "ado_org": "cyrela-data-analytics",
                "ado_project": "Data Analytics",
                "ado_repo": "lakehouse",
                "ado_pat": encrypt_token("")
            }).execute()
    except Exception as e:
        print(f"Erro ao inicializar admin no Supabase: {e}")

def hash_password(password: str) -> str:
    salt = "cyrela_sec_"
    return hashlib.sha256((salt + password).encode()).hexdigest()

def check_password(password: str, hashed: str) -> bool:
    return hash_password(password) == hashed

def user_exists(email: str) -> bool:
    try:
        supabase = get_supabase()
        response = supabase.table("users").select("email").eq("email", email).execute()
        return len(response.data) > 0
    except Exception:
        return False

def create_user(email: str, password: str, is_admin: int = 0):
    supabase = get_supabase()
    supabase.table("users").insert({
        "email": email,
        "password_hash": hash_password(password),
        "is_admin": is_admin,
        "databricks_host": "",
        "databricks_token": encrypt_token(""),
        "genie_space_id": "",
        "ado_org": "cyrela-data-analytics",
        "ado_project": "Data Analytics",
        "ado_repo": "lakehouse",
        "ado_pat": encrypt_token("")
    }).execute()

def verify_login(email: str, password: str) -> dict:
    try:
        supabase = get_supabase()
        response = supabase.table("users").select("password_hash, is_admin").eq("email", email).execute()
        if response.data:
            user = response.data[0]
            if check_password(password, user["password_hash"]):
                return {"success": True, "is_admin": bool(user["is_admin"])}
    except Exception as e:
        print(f"Erro no login: {e}")
    return {"success": False}

def update_password(email: str, new_password: str):
    supabase = get_supabase()
    supabase.table("users").update({
        "password_hash": hash_password(new_password)
    }).eq("email", email).execute()

def get_user_tokens(email: str) -> dict:
    try:
        supabase = get_supabase()
        response = supabase.table("users").select(
            "databricks_host, databricks_token, genie_space_id, ado_org, ado_project, ado_repo, ado_pat"
        ).eq("email", email).execute()
        
        if response.data:
            res = response.data[0]
            return {
                "host": res.get("databricks_host") or "", 
                "token": decrypt_token(res.get("databricks_token") or ""), 
                "space_id": res.get("genie_space_id") or "",
                "ado_org": res.get("ado_org") or "cyrela-data-analytics", 
                "ado_project": res.get("ado_project") or "Data Analytics", 
                "ado_repo": res.get("ado_repo") or "lakehouse", 
                "ado_pat": decrypt_token(res.get("ado_pat") or "")
            }
    except Exception as e:
        print(f"Erro ao obter tokens: {e}")
        
    return {"host": "", "token": "", "space_id": "", "ado_org": "cyrela-data-analytics", "ado_project": "Data Analytics", "ado_repo": "lakehouse", "ado_pat": ""}

def update_user_tokens(email: str, host: str, token: str, space_id: str, ado_org: str, ado_proj: str, ado_repo: str, ado_pat: str):
    supabase = get_supabase()
    supabase.table("users").update({
        "databricks_host": host,
        "databricks_token": encrypt_token(token),
        "genie_space_id": space_id,
        "ado_org": ado_org,
        "ado_project": ado_proj,
        "ado_repo": ado_repo,
        "ado_pat": encrypt_token(ado_pat)
    }).eq("email", email).execute()

def generate_otp(email: str, otp_type: str = "login") -> str:
    code = str(random.randint(100000, 999999))
    expires = (datetime.now() + timedelta(minutes=10)).isoformat()
    
    supabase = get_supabase()
    supabase.table("otp_codes").delete().eq("email", email).eq("type", otp_type).execute()
    supabase.table("otp_codes").insert({
        "email": email,
        "code": code,
        "expires_at": expires,
        "type": otp_type
    }).execute()
    return code

def verify_otp(email: str, code: str, otp_type: str = "login") -> bool:
    try:
        supabase = get_supabase()
        response = supabase.table("otp_codes").select("expires_at").eq("email", email).eq("code", code).eq("type", otp_type).execute()
        
        if response.data:
            expires_at_str = response.data[0]["expires_at"]
            expires_at = datetime.fromisoformat(expires_at_str)
            if datetime.now() <= expires_at:
                supabase.table("otp_codes").delete().eq("email", email).eq("type", otp_type).execute()
                return True
    except Exception as e:
        print(f"Erro ao verificar OTP: {e}")
        
    return False

def send_email(to_email: str, subject: str, body: str):
    smtp_server = os.getenv("SMTP_SERVER")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASS")
    
    if not all([smtp_server, smtp_user, smtp_pass]):
        st.info(f"📧 **EMAIL SIMULADO (SMTP não configurado)**\n\n**Para:** {to_email}\n**Assunto:** {subject}\n\n{body}")
        print(f"--- EMAIL TO {to_email} ---\nSubject: {subject}\n{body}\n-------------------")
        return True
        
    try:
        msg = MIMEMultipart()
        msg['From'] = smtp_user
        msg['To'] = to_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)
        server.quit()
        return True
    except Exception as e:
        print(f"Erro ao enviar email: {e}")
        st.error("Erro ao enviar o e-mail de verificação. Verifique as configurações de SMTP.")
        return False
