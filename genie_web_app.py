import os
import json
import re
import base64
from datetime import datetime
from html import escape
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st
import requests
import subprocess
import shutil
from dotenv import load_dotenv
from pandas.api import types as ptypes

try:
    import pyarrow  # noqa: F401
    PYARROW_AVAILABLE = True
except Exception:
    PYARROW_AVAILABLE = False

try:
    from PIL import Image, UnidentifiedImageError
except Exception:
    Image = None
    UnidentifiedImageError = Exception

from genie_chat import (
    GenieApiClient,
    extract_conversation_id,
    extract_message_id,
    wait_for_terminal_message,
)
from auth import init_db, get_user_tokens, update_user_tokens
from auth_ui import render_auth_ui, render_token_setup_ui



class AzureDevOpsClient:
    def __init__(self, organization: str, project: str, repository: str, pat: str):
        self.organization = organization
        self.project = project
        self.repository = repository
        self.pat = pat
        self.rest_url = f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repository}"
        self.auth = ("", pat)

    def get_branch(self, branch_name: str) -> Optional[Dict[str, Any]]:
        url = f"{self.rest_url}/refs?filter=heads/{branch_name}&api-version=7.1"
        try:
            response = requests.get(url, auth=self.auth)
            if response.status_code == 200:
                value = response.json().get("value", [])
                return value[0] if value else None
        except Exception:
            return None
        return None

    def get_last_commit(self, branch_name: str) -> Optional[Dict[str, Any]]:
        url = f"{self.rest_url}/commits?searchCriteria.itemVersion.version={branch_name}&top=1&api-version=7.1"
        try:
            response = requests.get(url, auth=self.auth)
            if response.status_code == 200:
                value = response.json().get("value", [])
                return value[0] if value else None
        except Exception:
            return None
        return None

    def create_branch_if_not_exists(self, branch_name: str, base_oid: str) -> str:
        # Verifica se a branch ja existe
        existing = self.get_branch(branch_name)
        if existing:
            return existing["objectId"]
        
        # Se nao existe, cria apontando para o base_oid
        url = f"{self.rest_url}/refs?api-version=7.1"
        payload = [
            {
                "name": f"refs/heads/{branch_name}",
                "oldObjectId": "0000000000000000000000000000000000000000",
                "newObjectId": base_oid
            }
        ]
        response = requests.post(url, auth=self.auth, json=payload)
        if response.status_code not in [200, 201]:
            raise RuntimeError(f"Falha ao criar branch: {response.text}")
        return base_oid

    def get_item_exists(self, path: str, branch_name: str) -> bool:
        url = f"{self.rest_url}/items?path={path}&versionDescriptor.version={branch_name}&api-version=7.1"
        try:
            response = requests.head(url, auth=self.auth)
            return response.status_code == 200
        except Exception:
            return False

    def push_changes(self, branch_name: str, base_branch: str, changes: List[Dict[str, Any]], comment: str) -> Dict[str, Any]:
        # 1. Pegar o commit mais recente da branch base (ex: dev)
        base_ref = self.get_branch(base_branch)
        if not base_ref:
            raise ValueError(f"Branch base '{base_branch}' nao encontrada.")
        
        # 2. Garantir que a branch de destino existe e herda o historico da base
        current_oid = self.create_branch_if_not_exists(branch_name, base_ref["objectId"])

        # 3. Ajustar changeType (add vs edit) para cada arquivo
        for change in changes:
            path = change["item"]["path"]
            if self.get_item_exists(path, branch_name):
                change["changeType"] = "edit"
            else:
                change["changeType"] = "add"

        # 4. Realizar o push (commit) relativo ao ID atual da branch
        payload = {
            "refUpdates": [
                {
                    "name": f"refs/heads/{branch_name}",
                    "oldObjectId": current_oid
                }
            ],
            "commits": [
                {
                    "comment": comment,
                    "baseCommitId": current_oid, # ESSENCIAL: Mantem o historico e evita "deletar" o resto
                    "changes": changes
                }
            ]
        }
        
        url = f"{self.rest_url}/pushes?api-version=7.1"
        response = requests.post(url, auth=self.auth, json=payload)
        if response.status_code not in [200, 201]:
            raise RuntimeError(f"Falha no Push: {response.text}")
        return response.json()

    def create_pull_request(self, source_branch: str, target_branch: str, title: str, description: str) -> Dict[str, Any]:
        url = f"{self.rest_url}/pullrequests?api-version=7.1"
        payload = {
            "sourceRefName": f"refs/heads/{source_branch}",
            "targetRefName": f"refs/heads/{target_branch}",
            "title": title,
            "description": description
        }
        response = requests.post(url, auth=self.auth, json=payload)
        if response.status_code not in [200, 201]:
            raise RuntimeError(f"Falha ao abrir PR: {response.text}")
        return response.json()

    def push_changes_git_cli(self, branch_name: str, base_branch: str, sql_path: str, sql_content: str, yml_path: str, yml_content: str, comment: str):
        """
        Realiza o push usando o Git CLI para garantir que apenas os arquivos alvo sejam modificados,
        preservando o restante do repositório. Usa um clone completo para evitar problemas de merge/histórico.
        """
        import tempfile
        from pathlib import Path
        
        # 1. Preparar diretório temporário
        temp_dir = Path(tempfile.gettempdir()) / f"genie_git_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        temp_dir.mkdir(parents=True)
        
        try:
            # 2. URL com Autenticação (PAT)
            encoded_pat = requests.utils.quote(self.pat)
            repo_url = f"https://{encoded_pat}@dev.azure.com/{self.organization}/{requests.utils.quote(self.project)}/_git/{self.repository}"
            
            def run_git(args, cwd=temp_dir):
                result = subprocess.run(
                    ["git"] + args,
                    cwd=cwd,
                    capture_output=True,
                    text=True,
                    env={**os.environ, "GIT_TERMINAL_PROMPT": "0"}
                )
                if result.returncode != 0:
                    raise RuntimeError(f"Erro no Git ({args[0]}): {result.stderr}")
                return result.stdout

            # 3. Clone completo da branch específica
            run_git(["clone", "--single-branch", "--branch", base_branch, repo_url, "."])
            
            # 4. Configurar usuário (obrigatório para commit)
            run_git(["config", "user.email", "genie-bot@cyrela.com.br"])
            run_git(["config", "user.name", "Genie Bot"])
            
            # 5. Criar e mudar para a nova branch
            run_git(["checkout", "-b", branch_name])
            
            # 6. Gravar arquivos nos caminhos corretos
            full_sql_path = temp_dir / sql_path
            full_yml_path = temp_dir / yml_path
            
            full_sql_path.parent.mkdir(parents=True, exist_ok=True)
            full_yml_path.parent.mkdir(parents=True, exist_ok=True)
            
            with full_sql_path.open("w", encoding="utf-8") as f:
                f.write(sql_content)
            
            if yml_content.strip():
                with full_yml_path.open("w", encoding="utf-8") as f:
                    f.write(yml_content)
            
            # 7. Sincronizar TODAS as mudanças (add --all garante que nada suma)
            run_git(["add", "--all"])
            
            # 8. Commit
            run_git(["commit", "-m", comment])
            
            # 9. Push (Force push para sobrescrever tentativas anteriores se houver)
            run_git(["push", "origin", branch_name, "--force"])
            
            return True
        finally:
            # Limpeza
            if temp_dir.exists():
                shutil.rmtree(temp_dir, ignore_errors=True)

import plotly.graph_objects as go

DEFAULT_POLL_SECONDS = 2.0
DEFAULT_TIMEOUT_SECONDS = 600
MAX_RENDERED_MESSAGES = 8
APP_ROOT = Path(__file__).resolve().parent
ASSETS_DIR = APP_ROOT / "assets"
AGENT_SOURCE_IMAGE_NAME = "agente_cyrelinho.png"
AGENT_AVATAR_IMAGE_NAME = "agent_avatar_square.png"
USER_AVATAR_IMAGE_NAME = "cyrelinho2__de_frente_square.png"
APP_LOGO_LIGHT_FILE = "logo_rjzcyrela_branco.png"
APP_LOGO_DARK_FILE = "logo_rjzcyrela_preto.png"
APP_TOP_COVER_LIGHT_CANDIDATES = [
    "capa_linkedin",
    "capa_linkedin_rjzcyrela_branco",
]
APP_TOP_COVER_DARK_CANDIDATES = [
    "capa_linkedin_rjzcyrela_preto",
]
APP_DARK_BG_CANDIDATES = [
    "background_meet_cyrela_black"
]
APP_LIGHT_BG_CANDIDATES = [
    "background_meet_cyrela_white"
]
ANALYTICS_OPEN_TAG = "<genie_analytics>"
ANALYTICS_CLOSE_TAG = "</genie_analytics>"
UI_MODE_USER = "Usuario"
UI_MODE_DEVELOPER = "Desenvolvedor"
GENIE_SPACE_TABLES_QUERY = (
    "SELECT `table_catalog`, `table_schema`, `table_name`, "
    "COUNT(*) OVER () AS `total_tabelas` "
    "FROM `dev`.`information_schema`.`tables` "
    "WHERE `table_schema` = 'iops_rj' "
    "ORDER BY `table_name`"
)


def get_mode_storage_suffix(ui_mode: str) -> str:
    return "dev" if ui_mode == UI_MODE_DEVELOPER else "user"


def get_mode_state_keys(ui_mode: str) -> Dict[str, str]:
    suffix = get_mode_storage_suffix(ui_mode)
    return {
        "messages": f"messages_{suffix}",
        "conversation_id": f"conversation_id_{suffix}",
        "queued_question": f"queued_question_{suffix}",
    }


def encode_image_base64(image_path: Path) -> str:
    with image_path.open("rb") as f:
        return base64.b64encode(f.read()).decode("ascii")


@st.cache_data(show_spinner=False)
def get_cached_image_base64(image_path_str: str) -> str:
    return encode_image_base64(Path(image_path_str))


def encode_image_base64_if_exists(image_path: Optional[Path]) -> str:
    if image_path is None or not image_path.exists():
        return ""
    return get_cached_image_base64(str(image_path.resolve()))


def log_usage(tool_name: str, details: Optional[str] = None) -> None:
    """
    Simula o registro de uso das ferramentas para medição de KPI.
    Em um cenário real, isso gravaria em uma tabela Delta no Databricks.
    """
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        user = os.getenv("USER", "unknown_user")
        log_entry = {
            "timestamp": timestamp,
            "user": user,
            "tool": tool_name,
            "details": details
        }
        # Por enquanto, apenas logamos no terminal para fins de demonstração
        print(f"[KPI LOG] {json.dumps(log_entry)}")
    except Exception as e:
        print(f"Erro ao registrar log: {e}")


def extract_warehouse_id(input_str: str) -> str:
    """
    Extrai o ID do warehouse de uma string que pode ser o ID puro ou o HTTP Path.
    Ex: /sql/1.0/warehouses/ab0de84dfac97072 -> ab0de84dfac97072
    """
    if not input_str:
        return ""
    match = re.search(r"warehouses/([a-f0-9]+)", input_str)
    if match:
        return match.group(1)
    return input_str.strip()




def normalize_asset_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def resolve_asset_by_candidates(candidates: List[str]) -> Optional[Path]:
    supported_ext = [".png", ".jpg", ".jpeg", ".webp"]

    for stem in candidates:
        for ext in supported_ext:
            candidate = ASSETS_DIR / f"{stem}{ext}"
            if candidate.exists():
                return candidate

    for stem in candidates:
        for ext in supported_ext:
            matches = list(ASSETS_DIR.glob(f"*{stem}*{ext}"))
            if matches:
                return matches[0]

    # Final fallback for files with spaces/symbols/case differences.
    if not ASSETS_DIR.exists():
        return None

    normalized_candidates = [normalize_asset_key(c) for c in candidates]
    for file_path in ASSETS_DIR.iterdir():
        if not file_path.is_file():
            continue
        if file_path.suffix.lower() not in supported_ext:
            continue

        normalized_name = normalize_asset_key(file_path.stem)
        if any(nc in normalized_name for nc in normalized_candidates):
            return file_path

    return None


def read_env_default(name: str, fallback: str = "") -> str:
    value = os.getenv(name)
    if value is None:
        return fallback
    return value.strip()


def setup_page() -> None:
    light_bg_path = resolve_asset_by_candidates(APP_LIGHT_BG_CANDIDATES)
    light_logo_path = ASSETS_DIR / APP_LOGO_LIGHT_FILE

    light_bg_b64 = encode_image_base64_if_exists(light_bg_path)
    light_logo_b64 = encode_image_base64_if_exists(light_logo_path if light_logo_path.exists() else None)

    light_bg_css = "#f5f6fa"
    if light_bg_b64:
        light_layers: List[str] = [
            "linear-gradient(rgba(255,255,255,0.84), rgba(255,255,255,0.88))",
            f"url('data:image/png;base64,{light_bg_b64}')",
        ]
        if light_logo_b64:
            light_layers.append(f"url('data:image/png;base64,{light_logo_b64}')")
        light_bg_css = ",".join(light_layers)

    st.set_page_config(
        page_title="Genie - RJZ Cyrela",
        layout="wide",
    )
    st.markdown(
        """
        <style>
        div[data-testid="stAppViewContainer"] {
            background: __LIGHT_BG__;
            background-repeat: no-repeat, repeat, no-repeat;
            background-size: cover, cover, min(32vw, 380px);
            background-position: center center, center center, right 24px bottom 18px;
            background-attachment: fixed, fixed, fixed;
        }

        .hero-cover-wrap {
            margin-bottom: 0.55rem;
            position: relative;
            overflow: hidden;
            border-radius: 14px;
            min-height: 118px;
            box-shadow: 0 10px 24px rgba(0, 0, 0, 0.16);
            background: linear-gradient(135deg, rgba(255, 255, 255, 0.92), rgba(245, 246, 250, 0.98));
        }

        .hero-cover-wrap .cover-light {
            width: 100%;
            display: block;
            position: absolute;
            inset: 0;
            height: 100%;
            object-fit: cover;
        }

        .hero-overlay {
            position: relative;
            z-index: 3;
            min-height: 118px;
            padding: 0;
            background: linear-gradient(180deg, rgba(255, 255, 255, 0.02), rgba(255, 255, 255, 0.32));
        }

        .hero-title {
            position: absolute;
            left: 50%;
            top: 50%;
            transform: translate(-50%, -50%);
            margin: 0;
            font-size: 1rem;
            line-height: 1;
            font-weight: 800;
            color: #1f2a44;
            text-shadow: 0 1px 3px rgba(255, 255, 255, 0.35);
            display: flex;
            align-items: center;
            gap: 0;
            flex-wrap: nowrap;
            z-index: 4;
        }

        .hero-title-row {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            flex-wrap: nowrap;
        }

        .hero-genie {
            color: #f0783d;
            letter-spacing: 0.2px;
            margin: 0;
        }

        .hero-title-logo-inline {
            height: 68px;
            width: auto;
            filter: drop-shadow(0 1px 2px rgba(0, 0, 0, 0.35));
            display: inline-block;
            vertical-align: middle;
            position: absolute;
            left: 18px;
            top: 50%;
            transform: translateY(-50%);
            z-index: 4;
        }

        .hero-subtitle {
            position: absolute;
            left: auto;
            right: 16px;
            bottom: 14px;
            margin: 0;
            font-size: 0.82rem;
            color: rgba(22, 30, 48, 0.82);
            text-align: right;
            z-index: 4;
        }

        .hero-logo {
            width: min(180px, 30vw);
            display: none;
            position: absolute;
            left: 20px;
            bottom: 16px;
            z-index: 4;
            filter: drop-shadow(0 6px 10px rgba(0, 0, 0, 0.28));
        }

        .hero-logo-light {
            display: block;
        }

        div[data-testid="stSidebar"] {
            border-right: 1px solid rgba(255, 255, 255, 0.35);
            box-shadow: inset -1px 0 0 rgba(255, 255, 255, 0.15);
        }

        div[data-testid="stTextInput"] input,
        div[data-testid="stNumberInput"] input,
        div[data-testid="stChatInput"] input {
            border: 1px solid rgba(31, 42, 68, 0.22) !important;
            border-radius: 12px !important;
            box-shadow: 0 2px 10px rgba(31, 42, 68, 0.07);
        }

        div[data-testid="stTextInput"] input:focus,
        div[data-testid="stNumberInput"] input:focus,
        div[data-testid="stChatInput"] input:focus {
            border-color: rgba(229, 57, 53, 0.58) !important;
            box-shadow: 0 0 0 3px rgba(229, 57, 53, 0.16);
        }

        div[data-testid="stButton"] > button,
        div[data-testid="stDownloadButton"] > button {
            border-radius: 12px;
            border: 1px solid rgba(229, 57, 53, 0.28);
            font-weight: 700;
            transition: all .16s ease;
            box-shadow: 0 5px 14px rgba(31, 42, 68, 0.10);
            min-height: 46px;
        }

        div[data-testid="stButton"] > button {
            width: 100%;
            min-width: 0;
        }

        div[data-testid="stButton"] > button[kind="primary"] {
            background: linear-gradient(135deg, #f0783d 0%, #ff9a3d 100%);
            color: #ffffff;
            border-color: rgba(240, 120, 61, 0.55);
        }

        div[data-testid="stButton"] > button[kind="secondary"],
        div[data-testid="stDownloadButton"] > button {
            background: linear-gradient(180deg, rgba(255,255,255,0.92), rgba(255,255,255,0.80));
            color: #1f2a44;
        }

        div[data-testid="stButton"] > button[data-testid="baseButton-primary"] {
            background: linear-gradient(135deg, #f0783d 0%, #ff9a3d 100%);
        }

        div[data-testid="stButton"] > button[data-testid="baseButton-secondary"] {
            background: linear-gradient(180deg, rgba(255,255,255,0.92), rgba(255,255,255,0.80));
        }

        div[data-testid="stButton"] > button:hover,
        div[data-testid="stDownloadButton"] > button:hover {
            transform: translateY(-1px);
            box-shadow: 0 8px 18px rgba(31, 42, 68, 0.14);
            border-color: rgba(240, 120, 61, 0.55);
        }

        div[data-testid="stExpander"] {
            border: 1px solid rgba(31, 42, 68, 0.17);
            border-radius: 12px;
            overflow: hidden;
            box-shadow: 0 4px 14px rgba(31, 42, 68, 0.07);
        }

        div[data-testid="stDataFrame"] {
            border: 1px solid rgba(31, 42, 68, 0.12);
            border-radius: 10px;
            overflow: hidden;
        }

        .hero-cover-wrap .cover-light {
            display: block;
        }

        .sidebar-hidden div[data-testid="stSidebar"],
        .sidebar-hidden section[data-testid="stSidebarNav"],
        .sidebar-hidden div[data-testid="stSidebarContent"] {
            display: none;
        }

        @media (max-width: 900px) {
            .hero-title {
                font-size: 0.9rem;
            }
            .hero-subtitle {
                font-size: 0.76rem;
            }
            .hero-title-logo-inline {
                height: 54px;
            }
            .hero-logo {
                width: min(130px, 28vw);
            }
        }
        </style>
        """.replace("__LIGHT_BG__", light_bg_css),
        unsafe_allow_html=True,
    )
def init_state() -> None:
    for ui_mode in [UI_MODE_USER, UI_MODE_DEVELOPER]:
        mode_keys = get_mode_state_keys(ui_mode)
        mode_suffix = get_mode_storage_suffix(ui_mode)
        if mode_keys["messages"] not in st.session_state:
            st.session_state[mode_keys["messages"]] = []
        if mode_keys["conversation_id"] not in st.session_state:
            st.session_state[mode_keys["conversation_id"]] = None
        if mode_keys["queued_question"] not in st.session_state:
            st.session_state[mode_keys["queued_question"]] = None
        dedupe_key = f"last_processed_question_{mode_suffix}"
        if dedupe_key not in st.session_state:
            st.session_state[dedupe_key] = None

    if "active_ui_mode" not in st.session_state:
        st.session_state.active_ui_mode = UI_MODE_DEVELOPER

    if "selected_table" not in st.session_state:
        st.session_state.selected_table = None

    config_defaults = {
        "config_host": read_env_default("DATABRICKS_HOST"),
        "config_token": read_env_default("DATABRICKS_TOKEN"),
        "config_space_id": read_env_default("GENIE_SPACE_ID"),
        "config_poll_seconds": float(read_env_default("GENIE_POLL_SECONDS", str(DEFAULT_POLL_SECONDS))),
        "config_timeout_seconds": int(read_env_default("GENIE_TIMEOUT_SECONDS", str(DEFAULT_TIMEOUT_SECONDS))),
        "config_advanced_mode": True,
    }
    for state_key, default_value in config_defaults.items():
        if state_key not in st.session_state:
            st.session_state[state_key] = default_value

    st.session_state.assistant_avatar = resolve_assistant_avatar()
    st.session_state.user_avatar = resolve_user_avatar()

    if "create_space_selected_tables" not in st.session_state:
        st.session_state.create_space_selected_tables = []



def render_top_branding() -> None:
    top_cover_light_path = resolve_asset_by_candidates(APP_TOP_COVER_LIGHT_CANDIDATES)
    logo_light_path = ASSETS_DIR / APP_LOGO_LIGHT_FILE

    cover_light_b64 = encode_image_base64_if_exists(top_cover_light_path)
    logo_light_b64 = encode_image_base64_if_exists(logo_light_path if logo_light_path.exists() else None)

    if cover_light_b64:
        light_src = f"data:image/png;base64,{cover_light_b64}" if cover_light_b64 else ""
        light_logo_src = f"data:image/png;base64,{logo_light_b64}" if logo_light_b64 else ""
        st.markdown(
            (
                "<div class='hero-cover-wrap'>"
                f"<img class='cover-light' src='{light_src}' alt='Capa Light' />"
                "<div class='hero-overlay'>"
                "<h1 class='hero-title'>"
                "<span class='hero-genie'>Genie</span>"
                "</h1>"
                "<p class='hero-subtitle'>Assistente para operações com Databricks Genie.</p>"
                f"<img class='hero-title-logo-inline' src='{light_logo_src}' alt='RJZ Cyrela' />"
                "</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
    else:
        st.markdown("### Genie - RJZ Cyrela")
        st.caption("Assistente analitico local para operação com Databricks Genie.")


def resolve_assistant_avatar() -> Optional[str]:
    source_path = ASSETS_DIR / AGENT_SOURCE_IMAGE_NAME
    target_path = ASSETS_DIR / AGENT_AVATAR_IMAGE_NAME

    if not source_path.exists():
        return None

    if Image is None:
        return str(source_path)

    # Avoid rewriting the avatar file on every rerun; rewriting can trigger
    # Streamlit's file watcher and cause an endless reload loop.
    try:
        source_mtime = source_path.stat().st_mtime
    except OSError:
        source_mtime = None

    if target_path.exists():
        try:
            if source_mtime is None or target_path.stat().st_mtime >= source_mtime:
                return str(target_path)
        except OSError:
            pass

    try:
        ASSETS_DIR.mkdir(parents=True, exist_ok=True)
        with Image.open(source_path) as image:
            rgba_image = image.convert("RGBA")
            width, height = rgba_image.size
            side = min(width, height)
            left = int((width - side) / 2)
            top = int((height - side) / 2)
            cropped = rgba_image.crop((left, top, left + side, top + side))
            cropped.save(target_path)
        return str(target_path)
    except (OSError, UnidentifiedImageError):
        return str(source_path)


def resolve_user_avatar() -> Optional[str]:
    user_avatar_path = ASSETS_DIR / USER_AVATAR_IMAGE_NAME
    if user_avatar_path.exists():
        return str(user_avatar_path)
    return None


def extract_analytics_payload(answer_text: str) -> Tuple[str, Dict[str, Any]]:
    if not answer_text.strip():
        return answer_text, {}

    pattern = re.compile(
        re.escape(ANALYTICS_OPEN_TAG) + r"(.*?)" + re.escape(ANALYTICS_CLOSE_TAG),
        flags=re.IGNORECASE | re.DOTALL,
    )
    match = pattern.search(answer_text)
    if not match:
        return answer_text.strip(), {}

    payload_text = match.group(1).strip()
    cleaned_answer = (answer_text[: match.start()] + answer_text[match.end() :]).strip()

    try:
        parsed = json.loads(payload_text)
    except json.JSONDecodeError:
        return cleaned_answer, {}

    if not isinstance(parsed, dict):
        return cleaned_answer, {}

    return cleaned_answer, parsed


def extract_table_names_from_space_payload(space_payload: Dict[str, Any]) -> List[str]:
    table_names: List[str] = []
    seen: set[str] = set()

    direct_table_keys = {
        "table_name",
        "table",
        "table_full_name",
        "fully_qualified_table_name",
        "fully_qualified_name",
        "table_identifier",
    }

    def add_name(raw_name: Any) -> None:
        if not isinstance(raw_name, str):
            return
        name = raw_name.strip()
        if not name:
            return
        lowered = name.lower()
        if lowered in seen:
            return
        seen.add(lowered)
        table_names.append(name)

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            catalog = value.get("catalog_name") or value.get("catalog")
            schema = value.get("schema_name") or value.get("schema")
            table = value.get("table_name")
            if isinstance(catalog, str) and isinstance(schema, str) and isinstance(table, str):
                add_name(f"{catalog}.{schema}.{table}")

            for key, nested_value in value.items():
                if key in direct_table_keys:
                    add_name(nested_value)
                walk(nested_value)
            return

        if isinstance(value, list):
            for item in value:
                walk(item)

    walk(space_payload)
    return table_names


def merge_table_name_lists(*name_lists: List[str]) -> List[str]:
    merged: List[str] = []
    seen: set[str] = set()

    for names in name_lists:
        for name in names:
            normalized = str(name).strip()
            if not normalized:
                continue
            key = normalized.lower()
            if key in seen:
                continue
            seen.add(key)
            merged.append(normalized)

    return merged


def extract_table_names_from_sql(sql_text: str) -> List[str]:
    if not sql_text:
        return []

    # Capture table identifiers that appear after FROM/JOIN.
    pattern = re.compile(
        r"(?i)\\b(?:from|join)\\s+((?:`[^`]+`|[a-zA-Z0-9_]+)(?:\\.(?:`[^`]+`|[a-zA-Z0-9_]+)){0,2})"
    )

    names: List[str] = []
    seen: set[str] = set()
    for match in pattern.finditer(sql_text):
        candidate = match.group(1).replace("`", "").strip()
        if not candidate:
            continue
        key = candidate.lower()
        if key in seen:
            continue
        seen.add(key)
        names.append(candidate)

    return names


def extract_table_names_from_text(text: str) -> List[str]:
    if not text:
        return []

    pattern = re.compile(
        r"\\b(?:[a-zA-Z0-9_]+\\.){1,2}[a-zA-Z0-9_]+\\b"
    )
    names: List[str] = []
    seen: set[str] = set()
    for match in pattern.finditer(text):
        candidate = match.group(0).strip()
        key = candidate.lower()
        if key in seen:
            continue
        seen.add(key)
        names.append(candidate)
    return names


def probe_table_names_via_genie(client: GenieApiClient) -> List[str]:
    prompt = (
        "Liste somente os nomes completos (catalog.schema.table) das tabelas disponíveis neste Genie Space, "
        "uma por linha, sem explicações adicionais."
    )

    start_response = client.start_conversation(prompt)
    conversation_payload = start_response.get("conversation") or {}
    message_payload = start_response.get("message") or {}
    conversation_id = extract_conversation_id(conversation_payload)
    message_id = extract_message_id(message_payload)

    if not conversation_id or not message_id:
        return []

    final_message = wait_for_terminal_message(
        client=client,
        conversation_id=conversation_id,
        message_id=message_id,
        poll_seconds=1.0,
        timeout_seconds=90,
    )

    text_tables = extract_table_names_from_text(collect_text_answer(final_message))

    sql_tables: List[str] = []
    for attachment in final_message.get("attachments") or []:
        if not isinstance(attachment, dict):
            continue
        query_block = attachment.get("query") or {}
        sql_text = query_block.get("query")
        if isinstance(sql_text, str):
            sql_tables.extend(extract_table_names_from_sql(sql_text))

    return merge_table_name_lists(sql_tables, text_tables)


@st.cache_data(show_spinner=False, ttl=300)
def get_cached_genie_space_tables(host: str, token: str, space_id: str) -> Tuple[pd.DataFrame, int, str]:
    if not host or not token or not space_id or space_id == "dummy":
        return pd.DataFrame(), 0, ""

    try:
        client = GenieApiClient(host=host, token=token, space_id=space_id)
        # Fetch space with serialized content to see configured tables
        space_payload = client.get_space(include_serialized_space=True)
        
        serialized_space = space_payload.get("serialized_space", "{}")
        if isinstance(serialized_space, str):
            try:
                config_json = json.loads(serialized_space)
            except:
                config_json = {}
        else:
            config_json = serialized_space

        tables_list = config_json.get("data_sources", {}).get("tables") or []

        
        table_rows = []
        for t in tables_list:
            identifier = str(t.get("identifier") or "")
            if not identifier:
                continue
            parts = identifier.split(".")

            if len(parts) == 3:
                catalog, schema, name = parts
            elif len(parts) == 2:
                catalog, schema, name = "", parts[0], parts[1]
            else:
                catalog, schema, name = "", "", identifier
            
            table_rows.append({
                "table_catalog": catalog,
                "table_schema": schema,
                "table_name": name
            })

        if not table_rows:
            return pd.DataFrame(columns=["table_catalog", "table_schema", "table_name"]), 0, ""

        df = pd.DataFrame(table_rows)
        return df, len(df), ""
        
    except Exception as exc:
        return pd.DataFrame(), 0, str(exc)


@st.cache_data(show_spinner=False, ttl=300)
def get_cached_spaces(host: str, token: str) -> List[Dict[str, Any]]:
    if not host or not token:
        return []
    try:
        # Use a dummy space_id since list_spaces is top-level
        client = GenieApiClient(host, token, "dummy")
        res = client.list_spaces()
        return res.get("spaces") or []

    except Exception:
        return []


@st.cache_data(show_spinner=False, ttl=600)
def get_cached_catalogs(host: str, token: str, raw_warehouse_id: str) -> List[str]:
    warehouse_id = extract_warehouse_id(raw_warehouse_id)
    if not host or not token or not warehouse_id:
        return []
    try:
        client = GenieApiClient(host, token, "discovery")
        res = client.execute_sql_statement(warehouse_id=warehouse_id, statement="SHOW CATALOGS")
        rows = res.get("result", {}).get("data_array", [])
        return sorted([row[0] for row in rows])
    except Exception:
        return []

@st.cache_data(show_spinner=False, ttl=600)
def get_cached_schemas(host: str, token: str, raw_warehouse_id: str, catalog: str) -> List[str]:
    warehouse_id = extract_warehouse_id(raw_warehouse_id)
    if not host or not token or not warehouse_id or not catalog:
        return []
    try:
        client = GenieApiClient(host, token, "discovery")
        res = client.execute_sql_statement(warehouse_id=warehouse_id, statement=f"SHOW SCHEMAS IN {catalog}")
        rows = res.get("result", {}).get("data_array", [])
        return sorted([row[0] for row in rows])
    except Exception:
        return []

@st.cache_data(show_spinner=False, ttl=600)
def get_cached_tables(host: str, token: str, raw_warehouse_id: str, catalog: str, schema: str) -> List[str]:
    warehouse_id = extract_warehouse_id(raw_warehouse_id)
    if not host or not token or not warehouse_id or not catalog or not schema:
        return []
    try:
        client = GenieApiClient(host, token, "discovery")
        res = client.execute_sql_statement(warehouse_id=warehouse_id, statement=f"SHOW TABLES IN {catalog}.{schema}")
        rows = res.get("result", {}).get("data_array", [])
        return sorted([row[1] for row in rows])
    except Exception:
        return []




def render_genie_space_tables(config: Dict[str, Any]) -> None:
    st.markdown("#### Tabelas disponíveis no Genie para contexto de consulta")

    if not config.get("host") or not config.get("token") or not config.get("space_id"):
        st.caption("Preencha as credenciais para listar as tabelas selecionadas no Genie Space.")
        return

    tables_df, total_tabelas, load_error = get_cached_genie_space_tables(
        str(config.get("host", "")),
        str(config.get("token", "")),
        str(config.get("space_id", "")),
    )

    if load_error:
        st.warning(f"Erro ao carregar tabelas do Genie Space: {load_error}")
        return

    if tables_df.empty:
        st.info("Nenhuma tabela configurada neste Genie Space.")
        return

    st.caption(f"{total_tabelas} tabela(s) identificada(s) na configuração do Espaço Genie.")

    # Build full identifiers
    table_names = []
    for _, row in tables_df.iterrows():
        cat = row.get("table_catalog", "")
        sch = row.get("table_schema", "")
        nam = row.get("table_name", "")
        if cat and sch:
            full = f"{cat}.{sch}.{nam}"
        elif sch:
            full = f"{sch}.{nam}"
        else:
            full = nam
        table_names.append(full)
    
    table_names = sorted(table_names)

    if table_names:
        st.markdown("**Nomes das tabelas:**")
        items_html = "".join(
            f"<div style='white-space: nowrap; padding: 2px 0;'>{escape(str(table_name))}</div>"
            for table_name in table_names
        )

        st.markdown(
            (
                "<div style='overflow-x:auto;'>"
                "<div style='display:grid; grid-template-columns:repeat(4, minmax(210px, max-content)); "
                "column-gap:24px; row-gap:4px; min-width:max-content;'>"
                f"{items_html}"
                "</div></div>"
            ),
            unsafe_allow_html=True,
        )

        # Add selectbox for lineage
        st.markdown("#### Linhagem de dados (Unity Catalog)")
        selected_table = st.selectbox(
            "Selecione uma tabela para visualizar a linhagem de dados:",
            [""] + table_names,
            key="selected_table_select",
            help="Escolha uma tabela da lista acima para ver sua linhagem no Unity Catalog."
        )
        if selected_table:
            st.session_state.selected_table = selected_table



def safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def render_dataframe_with_fallback(df: pd.DataFrame) -> None:
    html_table = df.to_html(index=False, escape=True)
    st.markdown(
        (
            "<div style='overflow-x:auto; overflow-y:auto; max-height:420px; "
            "border:1px solid #d9d9d9; border-radius:8px; padding:8px;'>"
            f"{html_table}</div>"
        ),
        unsafe_allow_html=True,
    )


def sanitize_sheet_name(name: str, fallback: str) -> str:
    candidate = (name or "").strip()
    if not candidate:
        candidate = fallback
    candidate = re.sub(r"[\\/*?:\[\]]", "_", candidate)
    candidate = candidate.strip(" '")
    if not candidate:
        candidate = fallback
    return candidate[:31]


def dataframe_to_excel_bytes(df: pd.DataFrame, sheet_name: str) -> bytes:
    excel_df = prepare_dataframe_for_excel(df)
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        safe_sheet = sanitize_sheet_name(sheet_name, "dataset")
        excel_df.to_excel(writer, index=False, sheet_name=safe_sheet)
    output.seek(0)
    return output.getvalue()


def prepare_dataframe_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    prepared = df.copy()

    for col in prepared.columns:
        series = prepared[col]

        if ptypes.is_datetime64tz_dtype(series):
            prepared[col] = series.dt.tz_localize(None)
            continue

        if ptypes.is_object_dtype(series):
            # Some providers return timezone-aware Timestamp objects inside object columns.
            prepared[col] = series.map(
                lambda value: value.tz_localize(None)
                if isinstance(value, pd.Timestamp) and value.tzinfo is not None
                else value
            )

    return prepared


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def build_report_excel_bytes(
    datasets: List[Dict[str, Any]],
    question_text: str,
    answer_text: str,
) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        if datasets:
            for idx, dataset in enumerate(datasets):
                df: pd.DataFrame = dataset.get("dataframe", pd.DataFrame())
                excel_df = prepare_dataframe_for_excel(df)
                sheet_name = sanitize_sheet_name(
                    dataset.get("description") or dataset.get("query") or f"dataset_{idx + 1}",
                    f"dataset_{idx + 1}",
                )
                excel_df.to_excel(writer, index=False, sheet_name=sheet_name)
        else:
            pd.DataFrame([{"mensagem": "Sem datasets retornados pelo Genie."}]).to_excel(
                writer,
                index=False,
                sheet_name="resumo",
            )

        meta_df = pd.DataFrame(
            [
                {
                    "pergunta": question_text or "",
                    "resposta": answer_text or "",
                    "total_datasets": len(datasets),
                }
            ]
        )
        meta_df.to_excel(writer, index=False, sheet_name="metadata")

    output.seek(0)
    return output.getvalue()


def build_report_csv_bytes(
    datasets: List[Dict[str, Any]],
    question_text: str,
    answer_text: str,
) -> bytes:
    lines: List[str] = []
    lines.append(f"pergunta,{json.dumps(question_text or '', ensure_ascii=False)}")
    lines.append(f"resposta,{json.dumps(answer_text or '', ensure_ascii=False)}")
    lines.append(f"total_datasets,{len(datasets)}")
    lines.append("")

    for idx, dataset in enumerate(datasets):
        df: pd.DataFrame = dataset.get("dataframe", pd.DataFrame())
        title = (dataset.get("description") or dataset.get("query") or f"dataset_{idx + 1}").replace("\n", " ")
        lines.append(f"dataset_{idx + 1},{json.dumps(title, ensure_ascii=False)}")
        lines.append(df.to_csv(index=False).strip())
        lines.append("")

    return "\n".join(lines).encode("utf-8-sig")


def render_download_selector(
    label_prefix: str,
    key_prefix: str,
    excel_bytes: bytes,
    csv_bytes: bytes,
    excel_name: str,
    csv_name: str,
) -> None:
    file_format = st.radio(
        f"Formato de download ({label_prefix})",
        options=["Excel (.xlsx)", "CSV (.csv)"],
        horizontal=True,
        key=f"format_{key_prefix}",
    )

    if file_format == "Excel (.xlsx)":
        st.download_button(
            "Baixar",
            data=excel_bytes,
            file_name=excel_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"download_{key_prefix}_excel",
            use_container_width=True,
        )
    else:
        st.download_button(
            "Baixar",
            data=csv_bytes,
            file_name=csv_name,
            mime="text/csv",
            key=f"download_{key_prefix}_csv",
            use_container_width=True,
        )


def extract_genie_insights(analytics_payload: Dict[str, Any]) -> List[str]:
    raw_insights = analytics_payload.get("insights") if isinstance(analytics_payload, dict) else []
    if not isinstance(raw_insights, list):
        return []

    insights: List[str] = []
    for insight in raw_insights:
        if isinstance(insight, str) and insight.strip():
            insights.append(insight.strip())
    return insights


def coerce_dataframe_types(df: pd.DataFrame) -> pd.DataFrame:
    converted = df.copy()

    for col in converted.columns:
        series = converted[col]
        if not pd.api.types.is_object_dtype(series):
            continue

        normalized = series.replace({"": pd.NA, "None": pd.NA, "null": pd.NA})
        non_null = normalized.dropna()
        if non_null.empty:
            converted[col] = normalized
            continue

        non_null_text = non_null.astype(str).str.strip()
        valid_count = len(non_null_text)

        numeric_candidate = pd.to_numeric(non_null_text, errors="coerce")
        numeric_ratio = numeric_candidate.notna().sum() / max(valid_count, 1)
        if numeric_ratio >= 0.9:
            converted[col] = pd.to_numeric(normalized, errors="coerce")
            continue

        datetime_candidate = pd.to_datetime(non_null_text, errors="coerce", utc=False)
        datetime_ratio = datetime_candidate.notna().sum() / max(valid_count, 1)
        if datetime_ratio >= 0.9:
            converted[col] = pd.to_datetime(normalized, errors="coerce", utc=False)
            continue

        converted[col] = normalized

    return converted


def query_result_to_dataframe(query_result: Dict[str, Any]) -> pd.DataFrame:
    statement_response = query_result.get("statement_response") or {}
    manifest = statement_response.get("manifest") or {}
    schema = manifest.get("schema") or {}
    columns = schema.get("columns") or []
    column_names = [c.get("name") or f"column_{idx + 1}" for idx, c in enumerate(columns)]

    result = statement_response.get("result") or {}
    rows = result.get("data_array") or []

    if not rows:
        return pd.DataFrame(columns=column_names)

    df = pd.DataFrame(rows)
    if column_names:
        if len(column_names) == df.shape[1]:
            df.columns = column_names
        else:
            rename_map = {
                idx: column_names[idx] for idx in range(min(df.shape[1], len(column_names)))
            }
            df = df.rename(columns=rename_map)

    return coerce_dataframe_types(df)


def collect_text_answer(message: Dict[str, Any]) -> str:
    attachments = message.get("attachments") or []
    text_parts: List[str] = []

    for attachment in attachments:
        if not isinstance(attachment, dict):
            continue

        # Ignorar blocos de sugestões que o Genie as vezes coloca como texto
        if "suggested_questions" in attachment:
            continue

        text_block = attachment.get("text") or {}
        content = text_block.get("content")
        if content:
            content_str = str(content).strip()
            # Filtro para evitar que perguntas automáticas do Genie poluam a resposta
            if content_str.startswith(("Você gostaria", "Deseja ver", "Quer saber", "Como prefere", "Pode também", "Se preferir")):
                continue
            text_parts.append(content_str)

    if text_parts:
        return "\n\n".join(text_parts)

    if message.get("content"):
        return str(message["content"])

    return "Sem resposta textual no attachment desta mensagem."


def collect_suggested_questions(message: Dict[str, Any]) -> List[str]:
    suggestions: List[str] = []
    attachments = message.get("attachments") or []

    for attachment in attachments:
        if not isinstance(attachment, dict):
            continue
        suggestion_block = attachment.get("suggested_questions") or {}
        questions = suggestion_block.get("questions") or []
        for question in questions:
            if isinstance(question, str) and question.strip():
                suggestions.append(question.strip())

    return suggestions


def fetch_query_datasets(
    client: GenieApiClient,
    conversation_id: str,
    message_id: str,
    message: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[str]]:
    attachments = message.get("attachments") or []
    datasets: List[Dict[str, Any]] = []
    warnings: List[str] = []

    for attachment in attachments:
        if not isinstance(attachment, dict):
            continue

        query_info = attachment.get("query")
        attachment_id = attachment.get("attachment_id") or attachment.get("id")
        if not query_info or not attachment_id:
            continue

        try:
            query_result = client.get_attachment_query_result(
                conversation_id=conversation_id,
                message_id=message_id,
                attachment_id=str(attachment_id),
            )
            dataframe = query_result_to_dataframe(query_result)
            query_text = query_info.get("query") or ""
            query_description = query_info.get("description") or ""
            row_count = (
                (query_info.get("query_result_metadata") or {}).get("row_count")
                if isinstance(query_info, dict)
                else None
            )

            datasets.append(
                {
                    "attachment_id": str(attachment_id),
                    "query": query_text,
                    "description": query_description,
                    "row_count": row_count,
                    "dataframe": dataframe,
                }
            )
        except Exception as exc:
            warnings.append(
                f"Nao foi possivel obter query-result para attachment {attachment_id}: {exc}"
            )

    return datasets, warnings


def build_aggregate_df(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    agg_fn: str,
    top_n: int,
) -> Tuple[pd.DataFrame, str]:
    if agg_fn == "count":
        base = df[[x_col]].copy()
        base = base.dropna(subset=[x_col])
        grouped = base.groupby(x_col, dropna=False).size().reset_index(name="count")
        metric_col = "count"
    else:
        base = df[[x_col, y_col]].copy()
        base = base.dropna(subset=[x_col])
        base[y_col] = pd.to_numeric(base[y_col], errors="coerce")
        base = base.dropna(subset=[y_col])
        if base.empty:
            return pd.DataFrame(), y_col

        grouped = (
            base.groupby(x_col, dropna=False)[y_col]
            .agg(agg_fn)
            .reset_index(name=f"{agg_fn}_{y_col}")
        )
        metric_col = f"{agg_fn}_{y_col}"

    grouped = grouped.sort_values(metric_col, ascending=False).head(top_n)
    return grouped, metric_col


def select_chart_specs_for_dataset(
    analytics_payload: Dict[str, Any], dataset_idx: int
) -> List[Dict[str, Any]]:
    if not isinstance(analytics_payload, dict):
        return []

    raw_charts = analytics_payload.get("charts")
    if not isinstance(raw_charts, list):
        return []

    selected: List[Dict[str, Any]] = []
    for chart in raw_charts:
        if not isinstance(chart, dict):
            continue
        target_idx = chart.get("dataset_index")
        if target_idx is None:
            selected.append(chart)
            continue
        if safe_int(target_idx, -1) == dataset_idx:
            selected.append(chart)

    return selected


def render_genie_chart(
    df: pd.DataFrame,
    chart_spec: Dict[str, Any],
    message_idx: int,
    dataset_idx: int,
    chart_idx: int,
) -> None:
    chart_type = str(chart_spec.get("type", "")).strip().lower()
    if chart_type == "histogram":
        chart_type = "hist"

    title = str(chart_spec.get("title") or f"Grafico {chart_idx + 1}")
    x_col = str(chart_spec.get("x") or "").strip()
    y_col = str(chart_spec.get("y") or "").strip()
    aggregation = str(chart_spec.get("aggregation") or "sum").strip().lower()
    if aggregation not in {"sum", "mean", "count"}:
        aggregation = "sum"

    top_n = max(3, min(safe_int(chart_spec.get("top_n"), 10), 100))

    if chart_type in {"bar", "line", "pie"}:
        if not x_col or x_col not in df.columns:
            st.warning(f"Genie enviou grafico '{title}' sem coluna X valida.")
            return

        if aggregation != "count":
            if not y_col or y_col not in df.columns:
                st.warning(f"Genie enviou grafico '{title}' sem coluna Y valida.")
                return
        else:
            if not y_col or y_col not in df.columns:
                y_col = x_col

        grouped_df, metric_col = build_aggregate_df(df, x_col, y_col, aggregation, top_n)
        if grouped_df.empty:
            st.warning(f"Sem dados suficientes para renderizar '{title}'.")
            return

        if chart_type == "bar":
            fig = px.bar(grouped_df, x=x_col, y=metric_col, title=title)
        elif chart_type == "line":
            fig = px.line(grouped_df.sort_values(x_col), x=x_col, y=metric_col, title=title)
        else:
            fig = px.pie(grouped_df, names=x_col, values=metric_col, title=title)

        st.plotly_chart(fig, use_container_width=True, key=f"genie_plot_{message_idx}_{dataset_idx}_{chart_idx}")
        return

    if chart_type == "scatter":
        if not x_col or x_col not in df.columns or not y_col or y_col not in df.columns:
            st.warning(f"Genie enviou grafico '{title}' com colunas invalidas para dispersao.")
            return

        plot_df = df[[x_col, y_col]].copy()
        plot_df[y_col] = pd.to_numeric(plot_df[y_col], errors="coerce")
        plot_df = plot_df.dropna(subset=[y_col])
        if plot_df.empty:
            st.warning(f"Sem dados suficientes para renderizar '{title}'.")
            return

        fig = px.scatter(plot_df, x=x_col, y=y_col, title=title)
        st.plotly_chart(fig, use_container_width=True, key=f"genie_plot_{message_idx}_{dataset_idx}_{chart_idx}")
        return

    if chart_type == "hist":
        metric_col = y_col or x_col
        if not metric_col or metric_col not in df.columns:
            st.warning(f"Genie enviou grafico '{title}' sem metrica valida para histograma.")
            return

        plot_df = df[[metric_col]].copy()
        plot_df[metric_col] = pd.to_numeric(plot_df[metric_col], errors="coerce")
        plot_df = plot_df.dropna(subset=[metric_col])
        if plot_df.empty:
            st.warning(f"Sem dados suficientes para renderizar '{title}'.")
            return

        fig = px.histogram(plot_df, x=metric_col, nbins=30, title=title)
        st.plotly_chart(fig, use_container_width=True, key=f"genie_plot_{message_idx}_{dataset_idx}_{chart_idx}")
        return

    st.warning(f"Tipo de grafico '{chart_type}' nao suportado para '{title}'.")


def render_dataset(
    dataset: Dict[str, Any],
    message_idx: int,
    dataset_idx: int,
    analytics_payload: Dict[str, Any],
    show_query_details: bool,
) -> None:
    df: pd.DataFrame = dataset["dataframe"]
    title = dataset.get("query") or f"Dataset {dataset_idx + 1}"
    title = title[:120] + "..." if len(title) > 120 else title

    with st.expander(f"Dataset {dataset_idx + 1} | {title}", expanded=True):
        if dataset.get("description") and show_query_details:
            st.write(dataset["description"])

        if show_query_details:
            st.caption(f"Attachment ID: {dataset['attachment_id']}")
        if dataset.get("row_count") is not None:
            st.caption(f"Row count reportado pelo Genie: {dataset['row_count']}")

        if dataset.get("query") and show_query_details:
            st.code(dataset["query"], language="sql")

        if df.empty:
            st.info("Dataset sem linhas retornadas.")
            return

        st.caption(f"Exibindo dataset completo com {len(df)} linha(s) carregadas.")
        render_dataframe_with_fallback(df)

        excel_cache_key = "_excel_bytes"
        csv_cache_key = "_csv_bytes"
        if excel_cache_key not in dataset:
            dataset[excel_cache_key] = dataframe_to_excel_bytes(df, f"dataset_{dataset_idx + 1}")
        if csv_cache_key not in dataset:
            dataset[csv_cache_key] = dataframe_to_csv_bytes(df)

        render_download_selector(
            label_prefix=f"Dataset {dataset_idx + 1}",
            key_prefix=f"dataset_{message_idx}_{dataset_idx}",
            excel_bytes=dataset[excel_cache_key],
            csv_bytes=dataset[csv_cache_key],
            excel_name=f"genie_dataset_{message_idx + 1}_{dataset_idx + 1}.xlsx",
            csv_name=f"genie_dataset_{message_idx + 1}_{dataset_idx + 1}.csv",
        )

        chart_specs = select_chart_specs_for_dataset(analytics_payload, dataset_idx)
        if chart_specs:
            st.markdown("### Graficos gerados pelo Genie")
            for chart_idx, chart_spec in enumerate(chart_specs):
                render_genie_chart(df, chart_spec, message_idx, dataset_idx, chart_idx)
        else:
            st.info("Genie nao retornou especificacao de grafico para este dataset.")


def render_sidebar() -> Dict[str, Any]:
    with st.sidebar:
        st.header("Ferramentas do Dev")
        app_mode = st.radio(
            "Navegação", 
            [
                "🌠 Criar Novo Genie Space (API)",
                "💬 Genie Chat", 
                "📚 Dicionário e Perfil de Dados (Profiling)",
                "🛠️ Gerador de Modelos dbt/Jinja",
                "📄 Gerador de Documentação (.yml)",
                "⚡ Otimizador e Revisor SQL (Linter)",
                "🔍 Mapeador de Colunas (Legacy -> Atual)",
                "🏹 Conversor CRM XML -> SQL",
                "⚖️ Comparador de Ambientes (Dev vs Prod)",
                "🛡️ Analisador de Impacto em BI",
                "🚀 DevOps & CI/CD Hub (Auto-PR)"
            ],
            index=1 # Mantém o Chat como padrão se preferir, ou 0 para o Criar Space
        )


        st.session_state["app_mode"] = app_mode
        st.divider()

        st.header("Configuração")
        host = st.text_input(
            "DATABRICKS_HOST",
            key="config_host",
            type="password",
            help="URL do workspace Databricks. Use o icone de olho para ocultar/exibir.",
        )
        token = st.text_input(
            "DATABRICKS_TOKEN",
            key="config_token",
            type="password",
            help="Token PAT/OAuth com acesso ao Genie.",
        )

        poll_seconds = st.number_input(


            "GENIE_POLL_SECONDS",
            min_value=0.5,
            max_value=30.0,
            key="config_poll_seconds",
            step=0.5,
            help=(
                "Intervalo, em segundos, entre cada verificação de status da resposta no Genie. "
                "Valores menores atualizam mais rápido, mas fazem mais chamadas na API."
            ),
        )
        timeout_seconds = st.number_input(
            "GENIE_TIMEOUT_SECONDS",
            min_value=30,
            max_value=3600,
            key="config_timeout_seconds",
            step=30,
            help=(
                "Tempo maximo de espera (em segundos) para uma resposta do Genie antes de dar timeout."
            ),
        )
        advanced_mode = st.toggle(
            "Modo analítico avancado",
            key="config_advanced_mode",
            help=(
                "Quando ativo, o prompt inclui instruções para resposta mais técnica, "
                "com foco em métricas, tendências e recomendações."
            ),
        )

        st.divider()
        st.header("Azure DevOps Integration")
        devops_pat = st.text_input("ADO_PAT (Personal Access Token)", value=st.session_state.get("config_devops_pat", ""), type="password", key="config_devops_pat_sidebar")
        devops_org = st.text_input("ADO_ORG", value=st.session_state.get("config_devops_org", "cyrela-data-analytics"), key="config_devops_org_sidebar")
        devops_proj = st.text_input("ADO_PROJECT", value=st.session_state.get("config_devops_proj", "Data Analytics"), key="config_devops_proj_sidebar")
        devops_repo = st.text_input("ADO_REPO", value=st.session_state.get("config_devops_repo", "lakehouse"), key="config_devops_repo_sidebar")

        avatar_source = ASSETS_DIR / AGENT_SOURCE_IMAGE_NAME
        if not avatar_source.exists():
            st.caption(
                "Para usar sua imagem do agente, salve o PNG em "
                f"{avatar_source}. O recorte quadrado central e automatico."
            )

        st.divider()
        if st.button("💾 Salvar Credenciais no Meu Perfil", use_container_width=True, type="primary"):
            user_email = st.session_state.get("user_email")
            if user_email:
                space_id_to_save = str(st.session_state.get("chat_selected_space_id") or os.getenv("GENIE_SPACE_ID", "")).strip()
                update_user_tokens(
                    user_email, 
                    str(host).strip(), 
                    str(token).strip(), 
                    space_id_to_save, 
                    str(devops_org).strip(), 
                    str(devops_proj).strip(), 
                    str(devops_repo).strip(), 
                    str(devops_pat).strip()
                )
                st.success("Configurações salvas com sucesso no seu perfil na nuvem!")


    return {
        "host": str(host).strip(),
        "token": str(token).strip(),
        "space_id": str(st.session_state.get("chat_selected_space_id") or os.getenv("GENIE_SPACE_ID", "")).strip(),
        "poll_seconds": float(poll_seconds),
        "timeout_seconds": int(timeout_seconds),
        "advanced_mode": bool(advanced_mode),
        "devops": {
            "pat": devops_pat,
            "org": devops_org,
            "proj": devops_proj,
            "repo": devops_repo
        }
    }



def get_config_from_state() -> Dict[str, Any]:
    # Space ID now comes from the chat interface selection
    space_id = st.session_state.get("chat_selected_space_id")
    if not space_id:
        space_id = os.getenv("GENIE_SPACE_ID", "")

    return {
        "host": str(st.session_state.get("config_host", "")).strip(),
        "token": str(st.session_state.get("config_token", "")).strip(),
        "space_id": str(space_id).strip(),
        "poll_seconds": float(st.session_state.get("config_poll_seconds", DEFAULT_POLL_SECONDS)),
        "timeout_seconds": int(st.session_state.get("config_timeout_seconds", DEFAULT_TIMEOUT_SECONDS)),
        "advanced_mode": bool(st.session_state.get("config_advanced_mode", True)),
    }




def apply_sidebar_visibility(active_ui_mode: str) -> None:
    if active_ui_mode == UI_MODE_USER:
        st.markdown(
            """
            <style>
            div[data-testid="stSidebar"],
            section[data-testid="stSidebarNav"],
            div[data-testid="stSidebarContent"] {
                display: none;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )


def render_interface_mode_top() -> str:
    # Ocultado conforme solicitação do usuário, fixado no modo Desenvolvedor
    st.session_state.active_ui_mode = UI_MODE_DEVELOPER
    return UI_MODE_DEVELOPER


def render_chat_actions_below_input(ui_mode: str) -> None:
    mode_keys = get_mode_state_keys(ui_mode)
    action_col_a, action_col_b = st.columns([1, 1], gap="small")

    with action_col_a:
        if st.button(
            "Nova conversa",
            key=f"new_conversation_bottom_{get_mode_storage_suffix(ui_mode)}",
            type="primary",
            use_container_width=True,
        ):
            st.session_state[mode_keys["conversation_id"]] = None
            st.session_state[mode_keys["messages"]] = []
            st.session_state[mode_keys["queued_question"]] = None
            st.success(f"Conversa reiniciada no modo {ui_mode}.")

    with action_col_b:
        if st.button(
            "Limpar chat",
            key=f"clear_chat_bottom_{get_mode_storage_suffix(ui_mode)}",
            type="primary",
            use_container_width=True,
        ):
            st.session_state[mode_keys["messages"]] = []
            st.session_state[mode_keys["queued_question"]] = None
            st.success(f"Histórico local limpo no modo {ui_mode}.")


def build_final_question(question: str, advanced_mode: bool, ui_mode: str) -> str:
    instructions: List[str] = [question]

    if advanced_mode:
        instructions.append(
            "Ao responder, inclua: resumo executivo, principais metricas, "
            "tendencias relevantes, outliers, riscos, oportunidades e proximos passos."
        )

    if ui_mode == UI_MODE_USER:
        instructions.append(
            "Responda em modo Usuario: linguagem simples, didatica e orientada a negocio; "
            "evite jargao tecnico desnecessario e destaque conclusao pratica."
        )
    else:
        instructions.append(
            "Responda em modo Desenvolvedor: inclua detalhes tecnicos, racional da analise, "
            "query SQL e observacoes de qualidade dos dados quando aplicavel."
        )

    instructions.append(
        "Use ao maximo os recursos do Databricks Genie nesta resposta: "
        "retorne explicacao, consultas SQL, tabelas de resultado, graficos e insights gerados pelo proprio Genie."
    )
    instructions.append(
        "Nao aplique LIMIT artificial nas consultas SQL, a menos que eu solicite explicitamente. "
        "Quando houver grande volume, mantenha o resultado completo e informe o total de linhas."
    )
    instructions.append(
        "No final, inclua um bloco JSON valido entre as tags "
        f"{ANALYTICS_OPEN_TAG} e {ANALYTICS_CLOSE_TAG}, sem texto extra dentro do bloco. "
        "Use o schema: {\"insights\": [\"...\"], \"charts\": "
        "[{\"dataset_index\": 0, \"title\": \"...\", \"type\": \"bar|line|pie|scatter|histogram\", "
        "\"x\": \"coluna_x\", \"y\": \"coluna_y\", \"aggregation\": \"sum|mean|count\", "
        "\"top_n\": 10}]}. "
        "As colunas devem existir no resultado SQL retornado."
    )

    return "\n\n".join(instructions)


def send_question(
    config: Dict[str, Any],
    user_question_text: str,
    genie_question_payload: str,
    ui_mode: str,
) -> None:
    mode_keys = get_mode_state_keys(ui_mode)
    client = GenieApiClient(
        host=config["host"],
        token=config["token"],
        space_id=config["space_id"],
    )

    with st.spinner("Consultando Genie..."):
        if st.session_state[mode_keys["conversation_id"]] is None:
            start_response = client.start_conversation(genie_question_payload)
            conversation_payload = start_response.get("conversation") or {}
            message_payload = start_response.get("message") or {}

            conversation_id = extract_conversation_id(conversation_payload)
            message_id = extract_message_id(message_payload)
        else:
            conversation_id = st.session_state[mode_keys["conversation_id"]]
            create_response = client.create_message(conversation_id, genie_question_payload)
            message_id = extract_message_id(create_response)

        if not conversation_id or not message_id:
            raise RuntimeError("Nao foi possivel identificar conversation_id/message_id.")

        final_message = wait_for_terminal_message(
            client=client,
            conversation_id=conversation_id,
            message_id=message_id,
            poll_seconds=config["poll_seconds"],
            timeout_seconds=config["timeout_seconds"],
        )

        datasets, warnings = fetch_query_datasets(
            client=client,
            conversation_id=conversation_id,
            message_id=message_id,
            message=final_message,
        )

    raw_answer_text = collect_text_answer(final_message)
    clean_answer_text, analytics_payload = extract_analytics_payload(raw_answer_text)
    genie_insights = extract_genie_insights(analytics_payload)

    st.session_state[mode_keys["conversation_id"]] = conversation_id
    st.session_state[mode_keys["messages"]].append({"role": "user", "text": user_question_text})
    st.session_state[mode_keys["messages"]].append(
        {
            "role": "assistant",
            "status": final_message.get("status"),
            "text": clean_answer_text,
            "error": final_message.get("error"),
            "datasets": datasets,
            "warnings": warnings,
            "analytics_payload": analytics_payload,
            "genie_insights": genie_insights,
            "suggested_questions": collect_suggested_questions(final_message),
        }
    )


def render_messages(ui_mode: str) -> None:
    mode_keys = get_mode_state_keys(ui_mode)
    messages = st.session_state[mode_keys["messages"]]

    start_idx = max(0, len(messages) - MAX_RENDERED_MESSAGES)
    if start_idx > 0:
        st.info(
            f"Mostrando as {MAX_RENDERED_MESSAGES} mensagens mais recentes para manter a interface responsiva."
        )

    for msg_idx in range(start_idx, len(messages)):
        message = messages[msg_idx]
        role = message.get("role", "assistant")
        assistant_avatar = st.session_state.get("assistant_avatar")
        user_avatar = st.session_state.get("user_avatar")
        if role == "assistant" and assistant_avatar:
            chat_container = st.chat_message("assistant", avatar=assistant_avatar)
        elif role == "user" and user_avatar:
            chat_container = st.chat_message("user", avatar=user_avatar)
        else:
            chat_container = st.chat_message(role)

        with chat_container:
            if role == "user":
                st.markdown(message.get("text", ""))
                continue

            status = message.get("status")
            if status and ui_mode == "Desenvolvedor":
                st.markdown(f"Status da mensagem: **{status}**")

            text = message.get("text")
            if text:
                st.markdown(text)

            if message.get("error"):
                st.error(message["error"])

            analytics_payload = message.get("analytics_payload")
            if not isinstance(analytics_payload, dict):
                analytics_payload = {}

            if ui_mode == "Desenvolvedor":
                for warning in message.get("warnings", []):
                    st.warning(warning)

            datasets = message.get("datasets", [])
            if datasets:
                if "report_excel_bytes" not in message:
                    message["report_excel_bytes"] = build_report_excel_bytes(
                        datasets=datasets,
                        question_text=messages[msg_idx - 1].get("text", "") if msg_idx > 0 else "",
                        answer_text=text or "",
                    )
                if "report_csv_bytes" not in message:
                    message["report_csv_bytes"] = build_report_csv_bytes(
                        datasets=datasets,
                        question_text=messages[msg_idx - 1].get("text", "") if msg_idx > 0 else "",
                        answer_text=text or "",
                    )

                render_download_selector(
                    label_prefix=f"Relatorio resposta {msg_idx + 1}",
                    key_prefix=f"report_{msg_idx}",
                    excel_bytes=message["report_excel_bytes"],
                    csv_bytes=message["report_csv_bytes"],
                    excel_name=f"genie_relatorio_resposta_{msg_idx + 1}.xlsx",
                    csv_name=f"genie_relatorio_resposta_{msg_idx + 1}.csv",
                )

                for dataset_idx, dataset in enumerate(datasets):
                    render_dataset(
                        dataset,
                        msg_idx,
                        dataset_idx,
                        analytics_payload,
                        show_query_details=(ui_mode == "Desenvolvedor"),
                    )

            insights = message.get("genie_insights", [])
            if insights:
                st.markdown("### Insights gerados pelo Genie")
                for insight in insights:
                    st.write(f"- {insight}")
            else:
                st.info("Genie nao retornou bloco estruturado de insights nesta resposta.")

            suggestions = message.get("suggested_questions", [])
            if suggestions:
                st.markdown("#### Perguntas sugeridas")
                for s_idx, suggestion in enumerate(suggestions):
                    if st.button(
                        suggestion,
                        key=f"suggestion_{msg_idx}_{s_idx}",
                        use_container_width=True,
                    ):
                        st.session_state[mode_keys["queued_question"]] = suggestion


def render_lineage_graph(df: pd.DataFrame, selected_table: str) -> None:
    # Extract nodes and edges with normalized names
    def compose_table_name(catalog: Any, schema: Any, table: Any) -> str:
        parts = []
        for value in (catalog, schema, table):
            if value is None:
                continue
            text = str(value).strip()
            if not text or text.lower() in {"none", "null", "nan"}:
                continue
            parts.append(text)
        return ".".join(parts)

    nodes = set()
    edges = set()

    for _, row in df.iterrows():
        upstream_full = compose_table_name(
            row.get("source_table_catalog", ""),
            row.get("source_table_schema", ""),
            row.get("source_table_name", ""),
        )
        downstream_full = compose_table_name(
            row.get("target_table_catalog", ""),
            row.get("target_table_schema", ""),
            row.get("target_table_name", ""),
        )

        if upstream_full:
            nodes.add(upstream_full)
        if downstream_full:
            nodes.add(downstream_full)

        if upstream_full and downstream_full:
            edges.add((upstream_full, downstream_full))

    if not nodes:
        st.info("Não há dados suficientes para gerar o gráfico de linhagem.")
        return

    edges = sorted(edges)
    nodes = sorted(nodes)

    # Assign positions (simple layout: selected in center, others around)
    positions = {}
    selected_lower = str(selected_table or "").strip().lower()
    center_x, center_y = 0, 0

    # Find selected
    selected_node = None
    for node in nodes:
        if node.lower() == selected_lower:
            selected_node = node
            positions[node] = (center_x, center_y)
            break

    if not selected_node:
        selected_node = nodes[0]
        positions[selected_node] = (center_x, center_y)

    # Assign positions to others
    upstream = sorted({u for u, d in edges if d == selected_node})
    downstream = sorted({d for u, d in edges if u == selected_node})

    horizontal_gap = 3.0
    vertical_gap = 1.1

    # Upstream left (centered vertically)
    for i, node in enumerate(upstream):
        y_pos = (i - (len(upstream) - 1) / 2) * vertical_gap
        positions[node] = (-horizontal_gap, y_pos)

    # Downstream right (centered vertically)
    for i, node in enumerate(downstream):
        y_pos = (i - (len(downstream) - 1) / 2) * vertical_gap
        positions[node] = (horizontal_gap, y_pos)

    # Other connected nodes: place below center in fixed order
    other_nodes = sorted(node for node in nodes if node not in positions)
    for i, node in enumerate(other_nodes):
        positions[node] = (0.0, -((len(other_nodes) - 1) / 2 - i) * vertical_gap - (vertical_gap * 2.0))

    # Create figure
    fig = go.Figure()

    # Add edges
    for u, d in edges:
        x0, y0 = positions[u]
        x1, y1 = positions[d]
        fig.add_trace(go.Scatter(
            x=[x0, x1], y=[y0, y1],
            mode='lines',
            line=dict(width=2.5, color='#2563eb'),
            hoverinfo='skip',
            showlegend=False
        ))

    # Add nodes
    node_x = [positions[node][0] for node in nodes]
    node_y = [positions[node][1] for node in nodes]
    node_text = nodes
    node_color = ['red' if node == selected_node else 'lightblue' for node in nodes]
    text_positions = []
    for node in nodes:
        x_pos = positions[node][0]
        if node == selected_node:
            text_positions.append("top center")
        elif x_pos < 0:
            text_positions.append("middle right")
        elif x_pos > 0:
            text_positions.append("middle left")
        else:
            text_positions.append("bottom center")

    fig.add_trace(go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text',
        text=node_text,
        textposition=text_positions,
        textfont=dict(size=14, color="#111827", family="Arial, sans-serif"),
        marker=dict(size=30, color=node_color, line=dict(width=2, color="#4b5563")),
        hovertemplate="<b>%{text}</b><extra></extra>",
        showlegend=False
    ))

    all_x = [positions[node][0] for node in nodes]
    all_y = [positions[node][1] for node in nodes]
    fig.update_layout(
        title="Linhagem de Dados",
        xaxis=dict(
            showgrid=False,
            zeroline=False,
            showticklabels=False,
            range=[min(all_x) - 1.4, max(all_x) + 1.4],
        ),
        yaxis=dict(
            showgrid=False,
            zeroline=False,
            showticklabels=False,
            range=[min(all_y) - 1.2, max(all_y) + 1.2],
        ),
        margin=dict(l=150, r=150, t=70, b=40),
        width=1200,
        height=800,
        hovermode="closest",
    )

    st.plotly_chart(fig, use_container_width=False)


def render_table_lineage_section(config: Dict[str, Any]) -> None:
    if not config.get("host") or not config.get("token") or not config.get("space_id"):
        st.caption("Preencha as credenciais para usar a ferramenta de linhagem.")
        return

    selected_table = st.session_state.get("selected_table")
    if not selected_table:
        st.info("Selecione uma tabela da lista acima para visualizar a linhagem.")
        return

    table_input = selected_table
    direction = "both"

    if st.button("Confirmar e visualizar linhagem", key="btn_confirm_lineage"):
        client = GenieApiClient(
            host=config["host"],
            token=config["token"],
            space_id=config["space_id"],
        )

        with st.spinner("Consultando Unity Catalog via Genie..."):
            try:
                space_payload = client.get_space()
                warehouse_id = space_payload.get("warehouse_id")
                if not isinstance(warehouse_id, str) or not warehouse_id.strip():
                    st.error("warehouse_id não disponível no Genie Space.")
                    return

                sql_payload = client.get_table_lineage(
                    warehouse_id=warehouse_id.strip(),
                    table_full_name=table_input.strip(),
                    direction=direction,
                    timeout_seconds=config.get("timeout_seconds", 600),
                    poll_seconds=config.get("poll_seconds", 2.0),
                )

                manifest = sql_payload.get("manifest") or {}
                schema = manifest.get("schema") or {}
                columns = schema.get("columns") or []
                col_names = [str(col.get("name", "")).strip() for col in columns]
                result = sql_payload.get("result") or {}
                rows = result.get("data_array") or []

                if not rows:
                    st.info("Nenhuma linhagem encontrada para a tabela informada.")
                    return

                df = pd.DataFrame(rows)
                if len(col_names) == df.shape[1] and col_names:
                    df.columns = col_names

                # Visualize as graph
                st.markdown("**Visualização gráfica da linhagem**")
                render_lineage_graph(df, table_input.strip())

            except Exception as exc:
                st.error(f"Falha ao consultar linhagem: {exc}")


def render_data_dictionary_and_profiling(config: Dict[str, Any]) -> None:
    log_usage("Data Dictionary & Profiling")
    st.header("📚 Dicionário e Perfil de Dados (Profiling)")
    st.write("Visualize os metadados e a distribuição estatística dos dados (Data Profiling) das tabelas configuradas no seu Genie Space.")
    
    if not config.get("host") or not config.get("token") or not config.get("space_id"):
        st.caption("Preencha as credenciais na barra lateral para usar a ferramenta.")
        return

    client = GenieApiClient(config["host"], config["token"], config["space_id"])
    
    # Use tables from the Genie Space configuration
    tables_df, _, _ = get_cached_genie_space_tables(config["host"], config["token"], config["space_id"])
    table_options = []
    if not tables_df.empty:
        for _, row in tables_df.iterrows():
            cat = row.get("table_catalog", "")
            sch = row.get("table_schema", "")
            nam = row.get("table_name", "")
            if cat and sch:
                table_options.append(f"{cat}.{sch}.{nam}")
            elif sch:
                table_options.append(f"{sch}.{nam}")
            else:
                table_options.append(nam)
    table_options = sorted(table_options)

    col1, col2 = st.columns([3, 1])
    with col1:
        selected_table = st.selectbox("Selecione a Tabela do Space", [""] + table_options)
        final_table = selected_table

        
    with col2:
        st.write("")
        st.write("")
        analyze_btn = st.button("Analisar Tabela", use_container_width=True, type="primary")

    if analyze_btn:
        if not selected_table:
            st.error("Selecione uma tabela.")
            return
            
        with st.spinner("Buscando metadados e calculando o perfil dos dados (profiling)... (isso pode levar alguns minutos dependendo do tamanho da tabela)"):
            try:
                space_payload = client.get_space()
                warehouse_id = space_payload.get("warehouse_id")
                
                # 1. Get Schema
                describe_sql = f"DESCRIBE TABLE {final_table}"
                desc_payload = client.execute_sql_statement(
                    warehouse_id=warehouse_id, 
                    statement=describe_sql,
                    timeout_seconds=config.get("timeout_seconds", 600)
                )
                
                desc_rows = desc_payload.get("result", {}).get("data_array", [])
                cols = desc_payload.get("manifest", {}).get("schema", {}).get("columns", [])
                col_names = [c.get("name", "") for c in cols]
                
                if not desc_rows:
                    st.error("Tabela não encontrada ou sem colunas.")
                    return
                    
                df_desc = pd.DataFrame(desc_rows, columns=col_names)
                st.subheader("Esquema da Tabela")
                st.dataframe(df_desc, use_container_width=True)
                
                # 2. Profiling
                valid_cols = []
                for _, row in df_desc.iterrows():
                    col_name = row.get("col_name", "")
                    data_type = str(row.get("data_type", "")).upper()
                    if col_name and not col_name.startswith("#") and data_type not in ["ARRAY", "STRUCT", "MAP"]:
                        valid_cols.append(col_name)
                        
                if not valid_cols:
                    st.warning("Nenhuma coluna válida para gerar o perfil estatístico.")
                    return
                
                profiling_exprs = []
                for c in valid_cols:
                    safe_c = f"`{c}`"
                    profiling_exprs.append(f"COUNT({safe_c}) AS `{c}_count`")
                    profiling_exprs.append(f"COUNT(DISTINCT {safe_c}) AS `{c}_distinct`")
                    profiling_exprs.append(f"SUM(CASE WHEN {safe_c} IS NULL THEN 1 ELSE 0 END) AS `{c}_nulls`")
                    profiling_exprs.append(f"MIN({safe_c}) AS `{c}_min`")
                    profiling_exprs.append(f"MAX({safe_c}) AS `{c}_max`")
                    
                prof_sql = f"SELECT {', '.join(profiling_exprs)} FROM {final_table}"
                prof_payload = client.execute_sql_statement(
                    warehouse_id=warehouse_id, 
                    statement=prof_sql,
                    timeout_seconds=config.get("timeout_seconds", 600)
                )
                
                prof_rows = prof_payload.get("result", {}).get("data_array", [])
                prof_cols = prof_payload.get("manifest", {}).get("schema", {}).get("columns", [])
                prof_col_names = [c.get("name", "") for c in prof_cols]
                
                if prof_rows:
                    df_prof_raw = pd.DataFrame(prof_rows, columns=prof_col_names)
                    prof_results = []
                    for c in valid_cols:
                        total_not_null = int(df_prof_raw.at[0, f"{c}_count"])
                        nulls = int(df_prof_raw.at[0, f"{c}_nulls"])
                        total_rows = total_not_null + nulls
                        null_pct = round((nulls / total_rows * 100), 2) if total_rows > 0 else 0
                        
                        prof_results.append({
                            "Coluna": c,
                            "Não-Nulos": total_not_null,
                            "Nulos": f"{nulls} ({null_pct}%)",
                            "Distintos": df_prof_raw.at[0, f"{c}_distinct"],
                            "Mínimo": df_prof_raw.at[0, f"{c}_min"],
                            "Máximo": df_prof_raw.at[0, f"{c}_max"]
                        })
                        
                    st.subheader("Análise de Perfil de Dados (Data Profiling)")
                    st.dataframe(pd.DataFrame(prof_results), use_container_width=True)
                    
            except Exception as e:
                st.error(f"Erro na análise: {e}")

def render_sql_optimizer(config: Dict[str, Any]) -> None:
    log_usage("SQL Optimizer")
    st.header("⚡ Otimizador e Revisor SQL (Linter)")
    st.write("Receba a versão otimizada da sua query com foco em redução de custo (DBUs) e organização técnica.")
    
    if not config.get("host") or not config.get("token") or not config.get("space_id"):
        st.caption("Preencha as credenciais na barra lateral para usar a ferramenta.")
        return

    query_input = st.text_area("Insira a Query SQL", height=250)
    
    if st.button("Otimizar e Organizar Query", type="primary"):
        if not query_input.strip():
            st.error("Informe a query.")
            return
            
        prompt = (
            f"Otimize a query SQL abaixo para Databricks (custo de DBUs e performance).\n\n"
            f"Sua resposta deve seguir EXATAMENTE este formato (não escreva NADA antes ou depois):\n\n"
            f"[LISTA_DE_MELHORIAS]\n"
            f"(Descreva aqui detalhadamente as melhorias técnicas realizadas)\n\n"
            f"[QUERY_OTIMIZADA]\n"
            f"(Forneça aqui a query COMPLETA, bem identada e mantendo o bloco config() original)\n\n"
            f"REGRAS CRÍTICAS:\n"
            f"- NÃO repita o prompt, as instruções ou a query de entrada.\n"
            f"- Comece a resposta DIRETAMENTE pelo marcador [LISTA_DE_MELHORIAS].\n"
            f"- MANTENHA toda a lógica original (JOINS, CTEs, Filtros).\n\n"
            f"QUERY:\n"
            f"```sql\n{query_input}\n```"
        )
        
        client = GenieApiClient(config["host"], config["token"], config["space_id"])
        with st.spinner("Otimizando sua query..."):
            try:
                start_response = client.start_conversation(prompt)
                conversation_id = extract_conversation_id(start_response.get("conversation", {}))
                message_id = extract_message_id(start_response.get("message", {}))
                
                final_message = wait_for_terminal_message(
                    client=client,
                    conversation_id=conversation_id,
                    message_id=message_id,
                    poll_seconds=config.get("poll_seconds", 2.0),
                    timeout_seconds=config.get("timeout_seconds", 600),
                )
                
                raw_text = collect_text_answer(final_message)
                clean_text = raw_text.split("<analytics>")[0].strip()
                
                # 1. Limpeza Radical: Cortar tudo antes do primeiro marcador real
                if "[LISTA_DE_MELHORIAS]" in clean_text:
                    clean_text = clean_text[clean_text.find("[LISTA_DE_MELHORIAS]"):]
                
                # 2. Parsing das seções
                adjustments_part = ""
                attachment_sql = ""
                
                if "[QUERY_OTIMIZADA]" in clean_text:
                    parts = clean_text.split("[QUERY_OTIMIZADA]")
                    adjustments_part = parts[0].replace("[LISTA_DE_MELHORIAS]", "").strip()
                    sql_text_raw = parts[1].strip()
                    
                    # Tentar extrair bloco SQL
                    sql_blocks = re.findall(r"```sql\s*\n(.*?)\n```", sql_text_raw, re.DOTALL | re.IGNORECASE)
                    attachment_sql = sql_blocks[-1].strip() if sql_blocks else sql_text_raw.replace("```", "").strip()
                else:
                    adjustments_part = clean_text.replace("[LISTA_DE_MELHORIAS]", "").strip()

                # 3. Prioridade para anexo oficial
                for attachment in final_message.get("attachments") or []:
                    if isinstance(attachment, dict):
                        query_block = attachment.get("query") or {}
                        sql_text = query_block.get("query")
                        if isinstance(sql_text, str) and len(sql_text) > len(attachment_sql):
                            attachment_sql = sql_text
                            break
                
                # 4. Preservar config()
                config_match = re.search(r"\{\{\s*config\(.*?\)\s*\}\}", query_input, flags=re.DOTALL | re.IGNORECASE)
                if config_match and attachment_sql and not re.search(r"\{\{\s*config\(.*?\)\s*\}\}", attachment_sql, flags=re.DOTALL | re.IGNORECASE):
                    attachment_sql = config_match.group(0) + "\n\n" + attachment_sql

                # 5. Normalizar quebras de linha e identação
                if attachment_sql:
                    attachment_sql = attachment_sql.replace("\\n", "\n").replace("\\t", "    ")
                    if "\n" not in attachment_sql and "SELECT " in attachment_sql:
                        # Forçar quebras se vier em uma linha só
                        for kw in ["SELECT ", "FROM ", "LEFT JOIN ", "INNER JOIN ", "WHERE ", "WITH ", "GROUP BY ", "ORDER BY "]:
                            attachment_sql = attachment_sql.replace(kw, f"\n{kw}")

                # 6. Exibição Final
                if attachment_sql:
                    st.markdown("### 🚀 Query Otimizada")
                    st.code(attachment_sql, language="sql")

                if adjustments_part:
                    # Limpar placeholders e resquícios do prompt
                    clean_adj = adjustments_part.replace("(Descreva aqui detalhadamente as melhorias técnicas realizadas)", "").strip()
                    clean_adj = clean_adj.split("QUERY:")[0].strip()
                    
                    if len(clean_adj) > 5:
                        st.markdown("### 🛠️ Ajustes Realizados")
                        st.markdown(clean_adj)
                
            except Exception as e:
                st.error(f"Erro ao otimizar query: {e}")

def render_bi_impact_checker(config: Dict[str, Any]) -> None:
    log_usage("BI Impact Checker")
    st.header("🛡️ Analisador de Impacto em BI (Breaking Change Detector)")
    st.write("Compare a query atual com a nova versão para identificar riscos que podem quebrar seus Dashboards (Power BI).")
    
    if not config.get("host") or not config.get("token") or not config.get("space_id"):
        st.caption("Preencha as credenciais na barra lateral para usar a ferramenta.")
        return

    col1, col2 = st.columns(2)
    with col1:
        sql_atual = st.text_area("Query ATUAL (em produção)", height=300, help="A query que está rodando hoje.")
    with col2:
        sql_proposto = st.text_area("Query PROPOSTA (nova versão)", height=300, help="A versão otimizada ou alterada.")

    if st.button("Analisar Impacto", type="primary"):
        if not sql_atual.strip() or not sql_proposto.strip():
            st.error("Informe ambas as queries para comparação.")
            return

        prompt = (
            f"Atue como um Especialista em BI e Engenheiro de Dados. Sua tarefa é identificar possíveis BREAKING CHANGES "
            f"ao alterar a query SQL abaixo.\n\n"
            f"Compare a 'QUERY ATUAL' com a 'QUERY PROPOSTA' e gere um relatório técnico seguindo este formato:\n\n"
            f"[STATUS_DE_RISCO]\n"
            f"(CRÍTICO, ALERTA ou SEGURO)\n\n"
            f"[MUDANÇAS_DETECTADAS]\n"
            f"(Liste colunas removidas, renomeadas ou com alteração de tipo/lógica)\n\n"
            f"[RECOMENDAÇÃO]\n"
            f"(O que o desenvolvedor deve fazer para não quebrar o BI)\n\n"
            f"REGRAS:\n"
            f"- NÃO escreva nada antes do marcador [STATUS_DE_RISCO].\n"
            f"- Considere como CRÍTICO qualquer remoção ou renomeação de coluna.\n"
            f"- Considere como ALERTA mudanças de tipo de dado ou lógica de negócio.\n\n"
            f"QUERY ATUAL:\n```sql\n{sql_atual}\n```\n\n"
            f"QUERY PROPOSTA:\n```sql\n{sql_proposto}\n```"
        )

        client = GenieApiClient(config["host"], config["token"], config["space_id"])
        with st.spinner("Analisando impacto..."):
            try:
                start_response = client.start_conversation(prompt)
                conversation_id = extract_conversation_id(start_response.get("conversation", {}))
                message_id = extract_message_id(start_response.get("message", {}))
                
                final_message = wait_for_terminal_message(
                    client=client,
                    conversation_id=conversation_id,
                    message_id=message_id,
                    poll_seconds=config.get("poll_seconds", 2.0),
                    timeout_seconds=config.get("timeout_seconds", 600),
                )
                
                raw_text = collect_text_answer(final_message)
                clean_text = raw_text.split("<analytics>")[0].strip()

                if "[STATUS_DE_RISCO]" in clean_text:
                    clean_text = clean_text[clean_text.find("[STATUS_DE_RISCO]"):]

                # Parsing
                status = "DESCONHECIDO"
                mudancas = ""
                recomendacao = ""

                if "[STATUS_DE_RISCO]" in clean_text:
                    parts = clean_text.split("[MUDANÇAS_DETECTADAS]")
                    status_raw = parts[0].replace("[STATUS_DE_RISCO]", "").strip()
                    status = status_raw.split("\n")[0].strip()

                    if "[RECOMENDAÇÃO]" in parts[1]:
                        sub_parts = parts[1].split("[RECOMENDAÇÃO]")
                        mudancas = sub_parts[0].strip()
                        recomendacao = sub_parts[1].strip()
                    else:
                        mudancas = parts[1].strip()

                # Exibição Visual
                st.divider()
                if "CRÍTICO" in status.upper():
                    st.error(f"🔴 STATUS DE RISCO: {status}")
                elif "ALERTA" in status.upper():
                    st.warning(f"🟡 STATUS DE RISCO: {status}")
                else:
                    st.success(f"🟢 STATUS DE RISCO: {status}")

                col_res1, col_res2 = st.columns(2)
                with col_res1:
                    st.subheader("🔍 Mudanças Detectadas")
                    st.write(mudancas)
                with col_res2:
                    st.subheader("💡 Recomendação")
                    st.write(recomendacao)

            except Exception as e:
                st.error(f"Erro ao analisar impacto: {e}")

def render_devops_automation(config: Dict[str, Any]) -> None:
    log_usage("DevOps Automation")
    st.header("🚀 DevOps & CI/CD Hub (Auto-PR)")
    st.write("Automatize o envio dos seus modelos dbt e a abertura de Pull Requests no Azure DevOps.")
    
    devops_conf = config.get("devops", {})
    if not devops_conf.get("pat"):
        st.warning("Configure o seu PAT do Azure DevOps na barra lateral para usar esta ferramenta.")
        return

    client = AzureDevOpsClient(
        organization=devops_conf["org"],
        project=devops_conf["proj"],
        repository=devops_conf["repo"],
        pat=devops_conf["pat"]
    )

    # Painel de Status do Repositório
    st.subheader("📊 Status do Repositório (Azure DevOps)")
    target_branch_view = st.selectbox("Verificar Status da Branch:", ["dev", "main"], index=0)
    last_commit = client.get_last_commit(target_branch_view)
    
    if last_commit:
        with st.container(border=True):
            col_info1, col_info2 = st.columns(2)
            with col_info1:
                st.markdown(f"**Último Commit em `{target_branch_view}`:**")
                st.caption(last_commit.get("comment", "Sem comentário"))
            with col_info2:
                author = last_commit.get("author", {})
                st.markdown(f"**Autor:** {author.get('name', 'N/A')}")
                st.caption(f"📅 {last_commit.get('author', {}).get('date', '')[:19].replace('T', ' ')}")
    else:
        st.info("Nao foi possivel carregar o status da branch. Verifique seu PAT e a conexao.")

    st.divider()

    # Tentar recuperar modelos gerados recentemente (se houver)
    last_sql = st.session_state.get("last_generated_jinja_sql", "")
    last_yml = st.session_state.get("last_generated_yaml", "")
    last_name = st.session_state.get("last_generated_model_name", "novo_modelo")

    with st.form("devops_push_form"):
        st.subheader("Configuração da Alteração")
        
        # SQL e YAML primeiro, pois deles derivamos o resto
        col_sql, col_yml = st.columns(2)
        with col_sql:
            sql_content = st.text_area("Conteúdo SQL (Jinja)", value=last_sql, height=350, help="Cole aqui a query com o bloco config().")
        with col_yml:
            yml_content = st.text_area("Conteúdo YAML (Schema)", value=last_yml, height=350)

        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            base_branch = st.selectbox("Branch Base (Target)", ["dev", "main"], index=0)
            pr_title = st.text_input("Título do Pull Request", value=f"feat: otimização de modelo dbt")
        with col2:
            # Estrutura de nome de branch
            st.markdown("**Estrutura da Nova Branch**")
            col_b1, col_b2, col_b3 = st.columns([1, 1, 2])
            with col_b1:
                b_user = st.text_input("Usuário", value="mateus_daniel")
            with col_b2:
                b_type = st.selectbox("Tipo", ["feature", "hotfix"])
            with col_b3:
                b_reason = st.text_input("Motivo (Sufixo)", value="otimizacao")
        
        new_branch = f"{b_user}/{b_type}/{b_reason}"
        pr_desc = st.text_area("Descrição do PR", value="Otimização automática gerada via Genie SQL Linter.", height=80)

        submit = st.form_submit_button("🚀 Publicar no dbt & Abrir PR", use_container_width=True)

    if submit:
        if not sql_content.strip():
            st.error("O conteúdo SQL é obrigatório para identificar o modelo.")
            return

        # 1. Extrair metadados do config()
        schema_match = re.search(r"schema\s*=\s*['\"]([^'\"]+)['\"]", sql_content, re.IGNORECASE)
        alias_match = re.search(r"alias\s*=\s*['\"]([^'\"]+)['\"]", sql_content, re.IGNORECASE)
        
        target_schema = schema_match.group(1) if schema_match else "default"
        model_filename = alias_match.group(1) if alias_match else "novo_modelo"
        
        target_folder = f"dbt/models/{target_schema}"
        
        client = AzureDevOpsClient(
            organization=devops_conf["org"],
            project=devops_conf["proj"],
            repository=devops_conf["repo"],
            pat=devops_conf["pat"]
        )

        try:
            with st.spinner(f"Processando {model_filename} via Git CLI..."):
                # Caminhos relativos dentro do repo
                sql_rel_path = f"{target_folder}/{model_filename}.sql"
                yml_rel_path = f"{target_folder}/docs/{model_filename}.yml"
                
                # Executar fluxo Git Real
                client.push_changes_git_cli(
                    branch_name=new_branch,
                    base_branch=base_branch,
                    sql_path=sql_rel_path,
                    sql_content=sql_content,
                    yml_path=yml_rel_path,
                    yml_content=yml_content,
                    comment=f"Auto-commit: {model_filename} ({target_schema})"
                )
                
                st.success(f"✅ Commit realizado na branch '{new_branch}'!")
                
                # Abrir PR via API
                pr_response = client.create_pull_request(
                    source_branch=new_branch,
                    target_branch=base_branch,
                    title=f"{pr_title} - {model_filename}",
                    description=pr_desc
                )
                
                pr_url = pr_response.get("_links", {}).get("html", {}).get("href")
                st.balloons()
                st.success(f"🚀 Pull Request aberto com sucesso!")
                if pr_url:
                    st.link_button("Visualizar Pull Request no Azure DevOps", pr_url)

        except Exception as e:
            st.error(f"Erro na integração com DevOps: {e}")

def render_environment_comparator(config: Dict[str, Any]) -> None:
    log_usage("Environment Comparator")
    st.header("⚖️ Comparador de Ambientes (Dev vs Prod)")
    st.write("Compare os esquemas (schemas) de duas tabelas para identificar colunas faltantes ou tipos de dados divergentes.")
    
    if not config.get("host") or not config.get("token") or not config.get("space_id"):
        st.caption("Preencha as credenciais na barra lateral para usar a ferramenta.")
        return

    client = GenieApiClient(config["host"], config["token"], config["space_id"])
    
    colA, colB = st.columns(2)
    with colA:
        tabela_a = st.text_input("Tabela Dev (ex: dev.iops_rj.tabela)").strip()
    with colB:
        tabela_b = st.text_input("Tabela Prod (ex: prd.iops_rj.tabela)").strip()
        
    if st.button("Comparar Ambientes", type="primary"):
        if not tabela_a or not tabela_b:
            st.error("Informe as duas tabelas para comparação.")
            return
            
        with st.spinner("Buscando esquemas via DESCRIBE TABLE..."):
            try:
                space_payload = client.get_space()
                warehouse_id = space_payload.get("warehouse_id")
                
                def get_schema(table_name):
                    sql = f"DESCRIBE TABLE {table_name}"
                    res = client.execute_sql_statement(
                        warehouse_id=warehouse_id, 
                        statement=sql,
                        timeout_seconds=config.get("timeout_seconds", 60)
                    )
                    rows = res.get("result", {}).get("data_array", [])
                    cols = res.get("manifest", {}).get("schema", {}).get("columns", [])
                    col_names = [c.get("name", "") for c in cols]
                    df = pd.DataFrame(rows, columns=col_names)
                    valid_cols = []
                    for _, r in df.iterrows():
                        c_name = r.get("col_name", "")
                        dtype = str(r.get("data_type", "")).upper()
                        if c_name and not c_name.startswith("#"):
                            valid_cols.append({"col_name": c_name, "data_type": dtype})
                    return pd.DataFrame(valid_cols)
                
                df_a = get_schema(tabela_a)
                df_b = get_schema(tabela_b)
                
                if df_a.empty or df_b.empty:
                    st.error("Uma das tabelas não foi encontrada ou está vazia.")
                    return
                
                df_a = df_a.rename(columns={"data_type": "type_A"}).set_index("col_name")
                df_b = df_b.rename(columns={"data_type": "type_B"}).set_index("col_name")
                
                merged = df_a.join(df_b, how="outer")
                
                diffs = []
                for idx, row in merged.iterrows():
                    type_a = row["type_A"]
                    type_b = row["type_B"]
                    
                    if pd.isna(type_a):
                        diffs.append({"Coluna": idx, "Status": "🛑 Falta no DEV", "DEV": "-", "PROD": type_b})
                    elif pd.isna(type_b):
                        diffs.append({"Coluna": idx, "Status": "🛑 Falta no PROD", "DEV": type_a, "PROD": "-"})
                    elif type_a != type_b:
                        diffs.append({"Coluna": idx, "Status": "⚠️ Tipo Divergente", "DEV": type_a, "PROD": type_b})
                    else:
                        diffs.append({"Coluna": idx, "Status": "✅ Iguais", "DEV": type_a, "PROD": type_b})
                        
                df_diff = pd.DataFrame(diffs).sort_values("Status", ascending=False)
                
                st.subheader("Resultado da Comparação")
                st.dataframe(df_diff, use_container_width=True)
                
                errors = df_diff[df_diff["Status"].str.contains("🛑|⚠️")]
                if not errors.empty:
                    st.warning(f"Encontradas {len(errors)} divergências!")
                else:
                    st.success("Tabelas perfeitamente alinhadas!")
                    
            except Exception as e:
                error_msg = str(e)
                if "PERMISSION_DENIED" in error_msg:
                    st.warning("⚠️ Você não tem permissão para acessar um dos catálogos informados neste workspace (ex: prd). O comparador precisa de acesso de leitura em ambos os catálogos (dev e prd).")
                else:
                    st.error(f"Erro durante a comparação: {e}")

def render_create_genie_space(config: Dict[str, Any]) -> None:
    log_usage("Create Genie Space")
    st.header("🌠 Criar Novo Genie Space (via API)")
    st.write("Utilize esta ferramenta para provisionar um novo espaço do Genie programaticamente.")
    
    if not config.get("host") or not config.get("token"):
        st.warning("Preencha o HOST e o TOKEN na barra lateral para habilitar a criação via API.")
        return

    # Basic Info
    st.subheader("1. Informações Básicas")
    col1, col2 = st.columns(2)
    with col1:
        title = st.text_input("Título do Space", placeholder="Ex: Analítico Financeiro RJZ")
    with col2:
        # Try to get warehouse_id from current space to use as default
        default_wh = ""
        if config.get("host") and config.get("token") and config.get("space_id"):
            try:
                client_temp = GenieApiClient(config["host"], config["token"], config["space_id"])
                space_info = client_temp.get_space()
                default_wh = space_info.get("warehouse_id", "")
            except:
                pass
        
        # Campo fixo nos bastidores, não renderizado na tela para evitar erros
        warehouse_id = default_wh if default_wh else "ab0de84dfac97072"

    description = st.text_area("Descrição (Opcional)", placeholder="Descreva o propósito deste espaço...")

    st.divider()
    
    # Table Selection
    st.subheader("2. Seleção de Tabelas")
    st.info("Navegue pelo Unity Catalog para selecionar as tabelas que farão parte do contexto deste espaço.")
    
    # Extract ID for discovery calls
    active_warehouse_id = extract_warehouse_id(warehouse_id)

    if active_warehouse_id:
        browse_col1, browse_col2, browse_col3 = st.columns(3)
        
        catalogs = get_cached_catalogs(config["host"], config["token"], active_warehouse_id)
        with browse_col1:
            selected_catalog = st.selectbox("Catálogo", [""] + catalogs)
        
        schemas = []
        if selected_catalog:
            schemas = get_cached_schemas(config["host"], config["token"], active_warehouse_id, selected_catalog)
        with browse_col2:
            selected_schema = st.selectbox("Esquema", [""] + schemas)
            
        tables = []
        if selected_schema:
            tables = get_cached_tables(config["host"], config["token"], active_warehouse_id, selected_catalog, selected_schema)
        with browse_col3:
            selected_tables_in_browse = st.multiselect("Tabelas", tables)
            
        if st.button("Adicionar tabelas selecionadas"):
            for t in selected_tables_in_browse:
                full_name = f"{selected_catalog}.{selected_schema}.{t}"
                if full_name not in st.session_state.create_space_selected_tables:
                    st.session_state.create_space_selected_tables.append(full_name)
            st.success(f"{len(selected_tables_in_browse)} tabela(s) adicionada(s) à lista.")
    else:
        st.caption("Informe o SQL Warehouse ID para habilitar a navegação de tabelas.")

    if st.session_state.create_space_selected_tables:
        st.markdown("**Tabelas Selecionadas para o Space:**")
        # Display selected tables in a more compact way
        for i, t in enumerate(st.session_state.create_space_selected_tables):
            c_col1, c_col2 = st.columns([5, 1])
            c_col1.code(t)
            if c_col2.button("Remover", key=f"remove_t_{i}"):
                st.session_state.create_space_selected_tables.pop(i)
                st.rerun()
                
        if st.button("Limpar todas as tabelas"):
            st.session_state.create_space_selected_tables = []
            st.rerun()

    st.divider()
    
    # Advanced / Create
    st.subheader("3. Finalização")
    
    # Build JSON automatically based on selected tables
    auto_json = {
        "version": 2,
        "data_sources": {
            "tables": [{"identifier": t} for t in st.session_state.create_space_selected_tables]
        }
    }
    
    with st.expander("Configurações Avançadas (JSON Serialized Space)"):
        st.markdown("""
        **O que é este campo?**  
        O `serialized_space` é a 'alma' do Genie. Ele define não apenas as tabelas, mas também instruções personalizadas, 
        exemplos de perguntas (few-shot) e métricas.
        
        **É necessário?**  
        Sim, para que o espaço tenha contexto. No entanto, este campo é **preenchido automaticamente** conforme você 
        seleciona as tabelas acima. Você só precisa editá-lo se quiser adicionar configurações manuais complexas.
        """)
        serialized_space_val = st.text_area("JSON Payload", value=json.dumps(auto_json, indent=2), height=250)

    if st.button("Criar Space", type="primary", use_container_width=True):
        final_warehouse_id = extract_warehouse_id(warehouse_id)
        if not title or not final_warehouse_id:
            st.error("Título e Warehouse ID são obrigatórios.")
        else:
            client = GenieApiClient(config["host"], config["token"], "new")
            with st.spinner("Provisionando novo Genie Space..."):
                try:
                    # Parse and ensure tables are sorted (Databricks requirement)
                    payload_json = json.loads(serialized_space_val if serialized_space_val.strip() else json.dumps(auto_json))
                    
                    if "data_sources" in payload_json and "tables" in payload_json["data_sources"]:
                        # Sort tables by identifier
                        payload_json["data_sources"]["tables"].sort(key=lambda x: x.get("identifier", ""))
                    
                    final_serialized = json.dumps(payload_json)
                    
                    response = client.create_space(
                        title=title,
                        warehouse_id=final_warehouse_id,
                        description=description if description else None,
                        serialized_space=final_serialized
                    )


                    
                    new_space_id = response.get("space_id") or response.get("id")
                    st.success(f"### Sucesso! 🎉")
                    st.write(f"O Genie Space foi criado com o ID: `{new_space_id}`")
                    st.json(response)
                    
                    st.info("O novo espaço já está disponível para seleção no **Genie Chat**.")
                    
                    # Clear cache to ensure the new space shows up in the dropdown
                    get_cached_spaces.clear()
                    
                    # Automatically select the new space
                    st.session_state.chat_selected_space_id = new_space_id
                    
                    # Clear selection on success
                    st.session_state.create_space_selected_tables = []

                    
                except Exception as e:
                    st.error(f"Erro ao criar Genie Space: {e}")


def render_jinja_model_generator(config: Dict[str, Any]) -> None:
    log_usage("Jinja Model Generator")
    st.header("🛠️ Gerador de Modelos dbt/Jinja")
    st.write("Transforme queries SQL em modelos dbt (Jinja) formatados e visualize a linhagem das tabelas envolvidas.")
    
    if not config.get("host") or not config.get("token") or not config.get("space_id"):
        st.warning("Preencha as credenciais na barra lateral para usar esta ferramenta.")
        return

    # 1. Input SQL
    st.subheader("1. Query de Origem")
    sql_input = st.text_area(
        "Cole sua Query SQL aqui", 
        height=250, 
        placeholder="SELECT * FROM dev.iops_rj.vendas WHERE ..."
    )
    
    # 2. Configurações do dbt
    st.subheader("2. Configuração do Modelo")
    
    # Tenta sugerir um alias se a query tiver uma tabela clara
    suggested_alias = ""
    if sql_input:
        match = re.search(r"FROM\s+([a-zA-Z0-9_\.]+)", sql_input, re.IGNORECASE)
        if match:
            table_name = match.group(1).split(".")[-1]
            suggested_alias = f"iops_rj.{table_name}"
    
    dbt_alias = st.text_input("Alias do Modelo", value=suggested_alias, placeholder="Ex: iops_rj.dynamics_13_corretores")
    
    # Hardcoded values per user request
    dbt_schema = "iops_rj"
    dbt_materialized = "table"
    on_table_exists = "replace"
    
    st.caption(f"Configurações fixas: Schema: `{dbt_schema}` | Materialização: `{dbt_materialized}` | On Table Exists: `{on_table_exists}`")
    st.divider()

    if st.button("🚀 Gerar Modelo dbt", type="primary", use_container_width=True):
        if not sql_input:
            st.error("Por favor, insira a query SQL.")
            return
            
        with st.spinner("Gerando modelo dbt..."):
            # 1. Bloco de Configuração (Sempre correto)
            config_block = f"""{{{{ config(
    schema='{dbt_schema}',
    alias='{dbt_alias if dbt_alias else "nome_da_tabela"}',
    materialized='{dbt_materialized}',
    on_table_exists='{on_table_exists}',
) 
}}}}"""
            
            # 2. Transformação de Linhagem via Python (Infalível)
            # Remove blocos config() existentes se o usuário colou o resultado de volta na área de texto
            processed_sql = re.sub(r"\{\{\s*config\(.*?\)\s*\}\}", "", sql_input, flags=re.DOTALL | re.IGNORECASE).strip()
            
            # Função auxiliar para limpar nomes de tabelas (remover crases/aspas)
            def clean_name(n):
                return n.replace('`','').replace('"','').replace("'","")
            
            # Regra para 'semantic' -> ref()
            # Padrão flexível para suportar: `catalogo`.`semantic`.`tabela`, semantic.tabela, "semantic"."tabela", etc.
            semantic_pattern = r"(?:[`'\"a-zA-Z0-9_]+\.)?[`'\"]*semantic[`'\"]*\.([`'\"a-zA-Z0-9_]+)"
            processed_sql = re.sub(
                semantic_pattern, 
                lambda m: f"{{{{ ref('semantic.{clean_name(m.group(1))}') }}}}", 
                processed_sql, 
                flags=re.IGNORECASE
            )
            
            # Regra para 'clean' -> source()
            clean_pattern = r"(?:[`'\"a-zA-Z0-9_]+\.)?[`'\"]*clean[`'\"]*\.([`'\"a-zA-Z0-9_]+)"
            processed_sql = re.sub(
                clean_pattern, 
                lambda m: f"{{{{ source('clean', '{clean_name(m.group(1))}') }}}}", 
                processed_sql, 
                flags=re.IGNORECASE
            )


            
            # Montagem Final
            full_model = f"{config_block}\n\n{processed_sql}"
            
            st.subheader("✨ Modelo dbt Gerado")
            st.code(full_model, language="sql")
            
            # Download
            st.download_button(
                "Download Modelo (.sql)",
                data=full_model,
                file_name=f"{dbt_alias if dbt_alias else 'modelo'}.sql",
                mime="text/x-sql"
            )

def render_yaml_documentation_generator(config: Dict[str, Any]) -> None:
    log_usage("YAML Documentation Generator")
    st.header("📄 Gerador de Documentação (.yml)")
    st.write("Gere automaticamente o arquivo de documentação dbt (`schema.yml`) baseado na sua query, aproveitando as descrições e metadados já configurados no seu Genie Space.")
    
    if not config.get("host") or not config.get("token") or not config.get("space_id"):
        st.warning("Preencha as credenciais na barra lateral para usar esta ferramenta.")
        return

    # 1. Input SQL/Jinja
    st.subheader("1. Query ou Código Jinja")
    input_code = st.text_area(
        "Cole sua Query SQL ou Modelo dbt (Jinja) aqui", 
        height=250, 
        placeholder="SELECT * FROM {{ ref('semantic.tabela') }} ..."
    )
    
    # 2. Configurações do dbt
    st.subheader("2. Identificação do Modelo")
    
    suggested_name = ""
    if input_code:
        # Tenta pegar o nome do modelo se houver um alias no config do jinja
        match_config = re.search(r"alias=['\"](.*?)['\"]", input_code)
        if match_config:
            suggested_name = match_config.group(1)
        else:
            # Tenta pegar do FROM se for SQL puro
            match_from = re.search(r"FROM\s+([a-zA-Z0-9_\.]+)", input_code, re.IGNORECASE)
            if match_from:
                table_name = match_from.group(1).split(".")[-1]
                suggested_name = f"iops_rj.{table_name}"
    
    model_name = st.text_input("Nome do Modelo (para o YAML)", value=suggested_name, placeholder="Ex: iops_rj.dim_vendas_rj")
    
    if st.button("📄 Gerar YAML de Documentação", type="primary", use_container_width=True):
        if not input_code:
            st.error("Por favor, insira o código SQL/Jinja.")
            return
            
        client = GenieApiClient(config["host"], config["token"], config["space_id"])
        
        with st.spinner("O Genie está analisando as colunas e buscando metadados para o seu YAML..."):
            try:
                # Prompt para o Genie gerar o YAML
                # Pedimos explicitamente para ele usar o contexto do catálogo para as descrições
                prompt = (
                    f"Atue como um Engenheiro de Analytics especializado em dbt. "
                    f"Gere um arquivo YAML de documentação dbt (version: 2) para o modelo '{model_name}'.\n\n"
                    f"Baseie-se na seguinte query/código para identificar as colunas envolvidas:\n"
                    f"```sql\n{input_code}\n```\n\n"
                    f"Instruções Importantes:\n"
                    f"1. Busque no seu conhecimento do Genie Space (catálogo de dados) as descrições corretas para cada coluna.\n"
                    f"2. Se não encontrar uma descrição oficial, infira uma descrição clara e amigável em português (PT-BR) baseada no nome da coluna.\n"
                    f"3. O YAML deve seguir este formato:\n"
                    f"version: 2\n"
                    f"models:\n"
                    f"  - name: {model_name}\n"
                    f"    description: 'Descricao resumida do modelo'\n"
                    f"    columns:\n"
                    f"      - name: coluna_1\n"
                    f"        description: 'Descricao da coluna 1'\n"
                    f"4. Retorne APENAS o bloco de código YAML, sem explicações adicionais."
                )
                
                start_response = client.start_conversation(prompt)
                conversation_id = extract_conversation_id(start_response.get("conversation", {}))
                message_id = extract_message_id(start_response.get("message", {}))
                
                final_message = wait_for_terminal_message(
                    client=client,
                    conversation_id=conversation_id,
                    message_id=message_id,
                    poll_seconds=config.get("poll_seconds", 2.0),
                    timeout_seconds=config.get("timeout_seconds", 600),
                )
                
                raw_text = collect_text_answer(final_message)
                
                # Extrair apenas o bloco de código YAML da resposta
                yaml_content = ""
                code_match = re.search(r"```(?:yaml)?\s*(.*?)\s*```", raw_text, re.DOTALL | re.IGNORECASE)
                if code_match:
                    yaml_content = code_match.group(1).strip()
                else:
                    # Tenta pegar o texto limpo se não vier com backticks
                    yaml_content = raw_text.split("<analytics>")[0].strip()
                
                if yaml_content:
                    st.subheader("✨ Arquivo YAML Gerado")
                    st.code(yaml_content, language="yaml")
                    
                    # Botão de Download
                    st.download_button(
                        "Download schema.yml",
                        data=yaml_content,
                        file_name=f"{model_name}.yml",
                        mime="text/yaml"
                    )
                else:
                    st.warning("O Genie não retornou um YAML formatado. Tente novamente.")
                    st.write(raw_text) # Mostra o bruto se falhar o regex
                
            except Exception as e:
                st.error(f"Falha ao gerar documentação: {e}")

def render_column_mapper(config: Dict[str, Any]) -> None:
    log_usage("Column Mapper")
    st.header("🔍 Mapeador de Colunas (Legacy -> Atual)")
    st.write("Compare uma query antiga ou uma lista de colunas legadas com o esquema atual do seu Genie Space para identificar as correspondências corretas.")
    
    if not config.get("host") or not config.get("token") or not config.get("space_id"):
        st.warning("Preencha as credenciais na barra lateral para usar esta ferramenta.")
        return

    # 1. Input Legacy
    st.subheader("1. Query ou Colunas Legadas")
    legacy_input = st.text_area(
        "Insira a Query Antiga ou a lista de colunas (uma por linha)", 
        height=250, 
        placeholder="SELECT cod_cli, nom_cli FROM tabela_antiga ..."
    )
    
    if st.button("🔍 Mapear Colunas Atuais", type="primary", use_container_width=True):
        if not legacy_input:
            st.error("Por favor, insira o conteúdo legado.")
            return
            
        client = GenieApiClient(config["host"], config["token"], config["space_id"])
        
        with st.spinner("O Genie está analisando o legado e comparando com o catálogo atual..."):
            try:
                # 1. Buscar as tabelas configuradas no Space para dar contexto extra ao LLM
                tables_df, _, _ = get_cached_genie_space_tables(config["host"], config["token"], config["space_id"])
                space_tables_list = ""
                if not tables_df.empty:
                    space_tables_list = "\n".join([
                        f"- {row['table_catalog']}.{row['table_schema']}.{row['table_name']}" 
                        for _, row in tables_df.iterrows()
                    ])
                
                # 2. Construir o Prompt enriquecido
                prompt = (
                    f"Atue como um Especialista em Migração de Dados de Alta Performance. "
                    f"Sua missão é mapear colunas de um ambiente Legado para o ambiente Atual no Databricks.\n\n"
                    f"### CONTEXTO DO GENIE SPACE (Tabelas Principais):\n"
                    f"{space_tables_list if space_tables_list else 'O Space não possui tabelas listadas ou não foi possível carregar.'}\n\n"
                    f"### INSTRUÇÃO DE ESCOPO:\n"
                    f"Considere que o ambiente atual utiliza os esquemas 'semantic', 'clean' e 'iops_rj' (geralmente no catálogo 'dev'). "
                    f"Mesmo que uma tabela não esteja na lista acima, utilize seu conhecimento do catálogo e das instruções do Space para encontrar a melhor correspondência nestes esquemas.\n\n"
                    f"### CONTEÚDO LEGADO PARA ANÁLISE:\n"
                    f"```sql\n{legacy_input}\n```\n\n"
                    f"### TAREFA:\n"
                    f"1. Identifique todas as colunas mencionadas no código legado.\n"
                    f"2. Encontre as colunas equivalentes no banco de dados atual, priorizando as camadas semantic e clean.\n"
                    f"3. Retorne uma tabela formatada em Markdown com as colunas: 'Coluna Legada', 'Tabela de Referência', 'Coluna Atual Sugerida', 'Confiança' e 'Motivo/Explicação'.\n"
                    f"4. Na 'Tabela de Referência', indique o nome completo (catalog.schema.table). Se souber que a coluna vem de uma 'semantic' ou 'clean', use-as como preferência.\n"
                    f"5. Se uma coluna não tiver correspondente óbvio, sugira a mais próxima ou marque como 'Não encontrada'.\n"
                    f"6. Se houver mudanças de tipo de dado ou lógica de cálculo (ex: mudança de string para decimal), mencione na explicação."
                )
                
                start_response = client.start_conversation(prompt)
                conversation_id = extract_conversation_id(start_response.get("conversation", {}))
                message_id = extract_message_id(start_response.get("message", {}))
                
                final_message = wait_for_terminal_message(
                    client=client,
                    conversation_id=conversation_id,
                    message_id=message_id,
                    poll_seconds=config.get("poll_seconds", 2.0),
                    timeout_seconds=config.get("timeout_seconds", 600),
                )
                
                raw_text = collect_text_answer(final_message)
                clean_text = raw_text.split("<analytics>")[0].strip()
                
                st.markdown("### Resultado do Mapeamento")
                st.markdown(clean_text)
                
            except Exception as e:
                st.error(f"Falha ao realizar mapeamento: {e}")

def render_crm_xml_converter(config: Dict[str, Any]) -> None:
    log_usage("CRM XML Converter")
    st.header("🏹 Conversor CRM XML -> SQL")
    st.write("Converta um XML de 'Localização Avançada' do CRM em uma query SQL/dbt compatível com o ambiente atual.")
    
    if not config.get("host") or not config.get("token") or not config.get("space_id"):
        st.warning("Preencha as credenciais na barra lateral para usar esta ferramenta.")
        return

    # 1. Input XML
    st.subheader("1. XML do CRM")
    xml_input = st.text_area(
        "Cole o XML da Localização Avançada aqui", 
        height=300, 
        placeholder="<fetch version='1.0' output-format='xml-platform' mapping='logical' ...>"
    )
    
    if st.button("🏹 Gerar Query SQL", type="primary", use_container_width=True):
        if not xml_input:
            st.error("Por favor, insira o XML.")
            return
            
        client = GenieApiClient(config["host"], config["token"], config["space_id"])
        
        with st.spinner("O Genie está interpretando o XML e mapeando para o catálogo atual..."):
            try:
                # Buscar tabelas do space para contexto
                tables_df, _, _ = get_cached_genie_space_tables(config["host"], config["token"], config["space_id"])
                space_tables_list = ""
                if not tables_df.empty:
                    space_tables_list = "\n".join([
                        f"- {row['table_catalog']}.{row['table_schema']}.{row['table_name']}" 
                        for _, row in tables_df.iterrows()
                    ])

                prompt = (
                    f"### INSTRUÇÃO CRÍTICA: NÃO EXECUTE NENHUMA QUERY SQL NO BANCO DE DADOS. ###\n"
                    f"Sua tarefa é estritamente de TRADUÇÃO E DOCUMENTAÇÃO. NÃO BUSQUE DADOS.\n\n"
                    f"Atue como um Engenheiro de Dados Especialista em CRM e Databricks.\n\n"
                    f"### TAREFA:\n"
                    f"Converta o seguinte XML de 'Localização Avançada' do CRM em uma query SQL formatada (Databricks SQL) que represente a mesma lógica.\n\n"
                    f"### CONTEXTO DO CATÁLOGO ATUAL (Priorize estes nomes):\n"
                    f"{space_tables_list if space_tables_list else 'O Space não possui tabelas listadas.'}\n\n"
                    f"### XML DO CRM PARA TRADUZIR:\n"
                    f"```xml\n{xml_input}\n```\n\n"
                    f"### REGRAS DE CONVERSÃO:\n"
                    f"1. Identifique a entidade principal (ex: account, incident) e mapeie para a tabela correspondente em 'semantic' ou 'clean'.\n"
                    f"2. Mapeie link-entities para JOINs.\n"
                    f"3. Converta todos os 'attributes' selecionados no XML em colunas no SELECT.\n"
                    f"4. Converta 'filter' e 'condition' em cláusulas WHERE.\n"
                    f"5. Se as colunas atuais forem diferentes, sugira o mapeamento mais provável.\n"
                    f"6. RETORNE APENAS O CÓDIGO SQL dentro de um bloco ```sql ... ``` e uma breve explicação abaixo.\n"
                    f"7. NÃO MOSTRE RESULTADOS DE DADOS, APENAS O CÓDIGO."
                )
                
                start_response = client.start_conversation(prompt)
                conversation_id = extract_conversation_id(start_response.get("conversation", {}))
                message_id = extract_message_id(start_response.get("message", {}))
                
                final_message = wait_for_terminal_message(
                    client=client,
                    conversation_id=conversation_id,
                    message_id=message_id,
                    poll_seconds=config.get("poll_seconds", 2.0),
                    timeout_seconds=config.get("timeout_seconds", 600),
                )
                
                raw_text = collect_text_answer(final_message)
                
                # Buscar SQL nos anexos (O Genie geralmente coloca a query gerada em um bloco de anexo)
                attachment_sql = ""
                for attachment in final_message.get("attachments") or []:
                    if isinstance(attachment, dict):
                        query_block = attachment.get("query") or {}
                        sql_text = query_block.get("query")
                        if isinstance(sql_text, str) and sql_text.strip():
                            attachment_sql = sql_text
                            break
                
                clean_text = raw_text.split("<analytics>")[0].strip()
                
                st.markdown("### Query SQL Gerada")
                if attachment_sql:
                    st.code(attachment_sql, language="sql")
                
                # Se não houver anexo, mas houver blocos de código no texto, o Streamlit markdown já cuida,
                # mas exibir o clean_text garante que as explicações apareçam.
                st.markdown(clean_text)
                
            except Exception as e:
                st.error(f"Falha ao converter XML: {e}")





def run_genie_chat_mode(config: Dict[str, Any], ui_mode: str) -> None:
    log_usage("Genie Chat", details=f"Mode: {ui_mode}")

    # 1. Space Selection at the top of Chat
    st.markdown("### 🪐 Seleção do Espaço de Trabalho")
    host = config.get("host")
    token = config.get("token")
    
    if host and token:
        try:
            spaces = get_cached_spaces(host, token)
            if spaces:
                # Robustly build space options handling different possible keys
                space_options = {}
                for s in spaces:
                    sid = s.get("space_id") or s.get("id")
                    name = s.get("title") or s.get("name") or s.get("display_name") or sid or "Espaço Sem Nome"
                    if sid:
                        space_options[sid] = name

                
                # Determine default selection
                current_space_id = st.session_state.get("chat_selected_space_id") or os.getenv("GENIE_SPACE_ID")
                space_ids = list(space_options.keys())

                
                default_idx = 0
                if current_space_id in space_ids:
                    default_idx = space_ids.index(current_space_id)

                selected_space_id = st.selectbox(
                    "Genie Space Ativo",
                    options=space_ids,
                    index=default_idx,
                    format_func=lambda x: str(space_options.get(x, x)),
                    key="chat_selected_space_id",
                    help="Selecione o espaço que deseja utilizar para as consultas."
                )
                # Ensure config uses the newly selected space
                config["space_id"] = selected_space_id
            else:
                st.warning("Nenhum Genie Space encontrado. Verifique suas permissões ou crie um novo espaço.")
        except Exception as e:
            st.error(f"Erro ao listar Genie Spaces: {e}")
    else:
        st.info("Preencha o HOST e TOKEN na barra lateral para selecionar um Genie Space.")


    st.divider()

    render_genie_space_tables(config)
    render_table_lineage_section(config)
    render_messages(ui_mode)

    if not config["host"] or not config["token"] or not config["space_id"]:
        st.error("⚠️ **Configuração incompleta!**")
        st.markdown("""
        Para usar a aplicação, você precisa:
        
        1. **DATABRICKS_TOKEN**: Obter um token válido do Databricks
           - Acesse seu workspace Databricks
           - Clique em **Settings** → **Developer** → **Access tokens**
           - Clique em **Generate new token** e copie o valor
           - Preencha no campo de configuração ou adicione ao arquivo `.env`
        
        2. **DATABRICKS_HOST**: URL do seu workspace (já preenchido)
        
        3. **GENIE_SPACE_ID**: ID do seu Genie Space (já preenchido)
        
        **Opções de configuração:**
        - Use a barra lateral para preencher os dados (modo temporário)
        - Edite o arquivo `.env` no diretório da aplicação com suas credenciais (modo persistente)
        """)
        return

    typed_question = st.chat_input("Digite sua pergunta para o Genie...")
    st.caption(
        "Dica: pergunte com base nas tabelas listadas acima para obter respostas mais confiáveis."
    )
    render_chat_actions_below_input(ui_mode)

    mode_keys = get_mode_state_keys(ui_mode)
    mode_suffix = get_mode_storage_suffix(ui_mode)
    dedupe_key = f"last_processed_question_{mode_suffix}"
    queued_question = st.session_state.pop(mode_keys["queued_question"], None)
    question = typed_question or queued_question

    if not question:
        st.session_state[dedupe_key] = None
        st.info("Envie uma pergunta para iniciar a conversa com o Genie.")
        return

    normalized_question = str(question).strip()
    if not normalized_question:
        st.session_state[dedupe_key] = None
        st.info("Envie uma pergunta para iniciar a conversa com o Genie.")
        return

    # Prevent duplicate sends if the browser/session triggers repeated reruns.
    if typed_question is None and st.session_state.get(dedupe_key) == normalized_question:
        return
    st.session_state[dedupe_key] = normalized_question

    final_question = build_final_question(
        normalized_question,
        config["advanced_mode"],
        ui_mode,
    )

    try:
        send_question(
            config=config,
            user_question_text=normalized_question,
            genie_question_payload=final_question,
            ui_mode=ui_mode,
        )
        st.rerun()
    except Exception as exc:
        st.session_state[dedupe_key] = None
        st.error(f"Falha ao consultar Genie: {exc}")


def main() -> None:
    load_dotenv(dotenv_path=APP_ROOT / ".env")
    init_db()
    
    setup_page()
    
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
        
    if not st.session_state.authenticated:
        render_auth_ui()
        return
        
    user_email = st.session_state.get('user_email', '')
    user_tokens = get_user_tokens(user_email)
    
    # Se o usuário não é admin (ou se você quiser que o admin também defina), 
    # verificamos se os tokens estão preenchidos.
    if not user_tokens['host'] or not user_tokens['token'] or not user_tokens['ado_pat']:
        render_token_setup_ui(user_email, 
                              user_tokens['host'], user_tokens['token'], user_tokens['space_id'],
                              user_tokens['ado_org'], user_tokens['ado_project'], user_tokens['ado_repo'], user_tokens['ado_pat'])
        return
        
    init_state()
    
    # Sobrescreve as variáveis de ambiente com os tokens individuais do usuário
    st.session_state.config_host = user_tokens['host']
    st.session_state.config_token = user_tokens['token']
    st.session_state.config_space_id = user_tokens['space_id']
    
    st.session_state.config_devops_org = user_tokens['ado_org']
    st.session_state.config_devops_proj = user_tokens['ado_project']
    st.session_state.config_devops_repo = user_tokens['ado_repo']
    st.session_state.config_devops_pat = user_tokens['ado_pat']

    st.sidebar.markdown(f"👤 Logado como **{user_email}**")
    if st.sidebar.button("🚪 Sair", use_container_width=True):
        st.session_state.authenticated = False
        st.rerun()
    st.sidebar.divider()

    apply_sidebar_visibility(st.session_state.active_ui_mode)

    if st.session_state.active_ui_mode == UI_MODE_USER:
        config = get_config_from_state()
        app_mode = "💬 Genie Chat"
    else:
        config = render_sidebar()
        app_mode = st.session_state.get("app_mode", "💬 Genie Chat")

    render_top_branding()
    ui_mode = render_interface_mode_top()

    if app_mode == "🌠 Criar Novo Genie Space (API)":
        render_create_genie_space(config)
    elif app_mode == "💬 Genie Chat":
        run_genie_chat_mode(config, ui_mode)
    elif app_mode == "🛠️ Gerador de Modelos dbt/Jinja":
        render_jinja_model_generator(config)
    elif app_mode == "📄 Gerador de Documentação (.yml)":
        render_yaml_documentation_generator(config)
    elif app_mode == "🔍 Mapeador de Colunas (Legacy -> Atual)":
        render_column_mapper(config)
    elif app_mode == "🏹 Conversor CRM XML -> SQL":
        render_crm_xml_converter(config)
    elif app_mode == "📚 Dicionário e Perfil de Dados (Profiling)":
        render_data_dictionary_and_profiling(config)
    elif app_mode == "⚡ Otimizador e Revisor SQL (Linter)":
        render_sql_optimizer(config)
    elif app_mode == "⚖️ Comparador de Ambientes (Dev vs Prod)":
        render_environment_comparator(config)
    elif app_mode == "🛡️ Analisador de Impacto em BI":
        render_bi_impact_checker(config)
    elif app_mode == "🚀 DevOps & CI/CD Hub (Auto-PR)":
        render_devops_automation(config)





if __name__ == "__main__":
    main()
