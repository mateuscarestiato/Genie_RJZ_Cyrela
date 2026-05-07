import sqlite3
import hashlib
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import random
from datetime import datetime, timedelta
import streamlit as st

DB_FILE = "auth.db"
EMAIL_SUFFIX = "@rjzcyrela.com.br"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            email TEXT PRIMARY KEY,
            password_hash TEXT,
            is_admin INTEGER,
            databricks_host TEXT,
            databricks_token TEXT,
            genie_space_id TEXT,
            ado_org TEXT,
            ado_project TEXT,
            ado_repo TEXT,
            ado_pat TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS otp_codes (
            email TEXT,
            code TEXT,
            expires_at DATETIME,
            type TEXT
        )
    ''')
    
    # Create default admin if not exists
    admin_email = "admin"
    admin_pass = os.getenv("ADMIN_PASSWORD", "admin123")
    c.execute("SELECT email FROM users WHERE email=?", (admin_email,))
    if not c.fetchone():
        c.execute("INSERT INTO users (email, password_hash, is_admin, databricks_host, databricks_token, genie_space_id, ado_org, ado_project, ado_repo, ado_pat) VALUES (?, ?, ?, '', '', '', 'cyrela-data-analytics', 'Data Analytics', 'lakehouse', '')",
                  (admin_email, hash_password(admin_pass), 1))
        
    conn.commit()
    conn.close()

def hash_password(password: str) -> str:
    # Simples hash SHA-256 (idealmente usar bcrypt, mas mantendo sem novas dependências)
    salt = "cyrela_sec_"
    return hashlib.sha256((salt + password).encode()).hexdigest()

def check_password(password: str, hashed: str) -> bool:
    return hash_password(password) == hashed

def user_exists(email: str) -> bool:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT email FROM users WHERE email=?", (email,))
    res = c.fetchone()
    conn.close()
    return res is not None

def create_user(email: str, password: str, is_admin: int = 0):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT INTO users (email, password_hash, is_admin, databricks_host, databricks_token, genie_space_id, ado_org, ado_project, ado_repo, ado_pat) VALUES (?, ?, ?, '', '', '', 'cyrela-data-analytics', 'Data Analytics', 'lakehouse', '')",
              (email, hash_password(password), is_admin))
    conn.commit()
    conn.close()

def verify_login(email: str, password: str) -> dict:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT password_hash, is_admin FROM users WHERE email=?", (email,))
    res = c.fetchone()
    conn.close()
    if res and check_password(password, res[0]):
        return {"success": True, "is_admin": bool(res[1])}
    return {"success": False}

def update_password(email: str, new_password: str):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("UPDATE users SET password_hash=? WHERE email=?", (hash_password(new_password), email))
    conn.commit()
    conn.close()

def get_user_tokens(email: str) -> dict:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT databricks_host, databricks_token, genie_space_id, ado_org, ado_project, ado_repo, ado_pat FROM users WHERE email=?", (email,))
    res = c.fetchone()
    conn.close()
    if res:
        return {
            "host": res[0] or "", "token": res[1] or "", "space_id": res[2] or "",
            "ado_org": res[3] or "cyrela-data-analytics", 
            "ado_project": res[4] or "Data Analytics", 
            "ado_repo": res[5] or "lakehouse", 
            "ado_pat": res[6] or ""
        }
    return {"host": "", "token": "", "space_id": "", "ado_org": "cyrela-data-analytics", "ado_project": "Data Analytics", "ado_repo": "lakehouse", "ado_pat": ""}

def update_user_tokens(email: str, host: str, token: str, space_id: str, ado_org: str, ado_proj: str, ado_repo: str, ado_pat: str):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("UPDATE users SET databricks_host=?, databricks_token=?, genie_space_id=?, ado_org=?, ado_project=?, ado_repo=?, ado_pat=? WHERE email=?", 
              (host, token, space_id, ado_org, ado_proj, ado_repo, ado_pat, email))
    conn.commit()
    conn.close()

def generate_otp(email: str, otp_type: str = "login") -> str:
    code = str(random.randint(100000, 999999))
    expires = datetime.now() + timedelta(minutes=10)
    
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    # Remove old codes for this email and type
    c.execute("DELETE FROM otp_codes WHERE email=? AND type=?", (email, otp_type))
    c.execute("INSERT INTO otp_codes (email, code, expires_at, type) VALUES (?, ?, ?, ?)",
              (email, code, expires, otp_type))
    conn.commit()
    conn.close()
    return code

def verify_otp(email: str, code: str, otp_type: str = "login") -> bool:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT expires_at FROM otp_codes WHERE email=? AND code=? AND type=?", (email, code, otp_type))
    res = c.fetchone()
    
    if res:
        expires_at = datetime.strptime(res[0], "%Y-%m-%d %H:%M:%S.%f")
        if datetime.now() <= expires_at:
            c.execute("DELETE FROM otp_codes WHERE email=? AND type=?", (email, otp_type))
            conn.commit()
            conn.close()
            return True
            
    conn.close()
    return False

def send_email(to_email: str, subject: str, body: str):
    # Tenta usar o servidor SMTP configurado no .env
    smtp_server = os.getenv("SMTP_SERVER")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASS")
    
    if not all([smtp_server, smtp_user, smtp_pass]):
        # Fallback: Apenas imprime no console e mostra no Streamlit se não houver SMTP configurado
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
