import os
import json
import re
import base64
from html import escape
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st
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
    if not host or not token or not space_id:
        return pd.DataFrame(), 0, ""

    try:
        client = GenieApiClient(host=host, token=token, space_id=space_id)
        space_payload = client.get_space()
        warehouse_id = space_payload.get("warehouse_id")
        if not isinstance(warehouse_id, str) or not warehouse_id.strip():
            return pd.DataFrame(), 0, "warehouse_id não disponível no Genie Space."

        sql_payload = client.execute_sql_statement(
            warehouse_id=warehouse_id.strip(),
            statement=GENIE_SPACE_TABLES_QUERY,
            timeout_seconds=90,
            poll_seconds=1.0,
        )

        manifest = sql_payload.get("manifest") or {}
        schema = manifest.get("schema") or {}
        columns = schema.get("columns") or []
        col_names = [str(col.get("name", "")).strip() for col in columns]
        result = sql_payload.get("result") or {}
        rows = result.get("data_array") or []

        if not rows:
            return pd.DataFrame(columns=["table_catalog", "table_schema", "table_name", "total_tabelas"]), 0, ""

        df = pd.DataFrame(rows)
        if len(col_names) == df.shape[1] and col_names:
            df.columns = col_names

        expected_cols = ["table_catalog", "table_schema", "table_name", "total_tabelas"]
        for col in expected_cols:
            if col not in df.columns:
                df[col] = ""

        df = df[expected_cols]
        if "total_tabelas" in df.columns:
            df["total_tabelas"] = pd.to_numeric(df["total_tabelas"], errors="coerce").fillna(0).astype(int)
            total = int(df["total_tabelas"].iloc[0]) if not df.empty else 0
        else:
            total = int(len(df))

        return df, total, ""
    except Exception as exc:
        return pd.DataFrame(), 0, str(exc)


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
        st.warning("Não foi possível carregar as tabelas do Genie Space no momento.")
        return

    if tables_df.empty:
        st.info("Nenhuma tabela encontrada para table_schema = 'iops_rj'.")
        return

    st.caption(f"{total_tabelas} tabela(s) identificada(s) no schema iops_rj.")

    table_names = (
        tables_df["table_name"].dropna().astype(str).str.strip().tolist()
        if "table_name" in tables_df.columns
        else []
    )
    if table_names:
        st.markdown("**Nomes das tabelas:**")
        items_html = "".join(
            f"<div style='white-space: nowrap; padding: 2px 0;'>{escape(table_name)}</div>"
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

        text_block = attachment.get("text") or {}
        content = text_block.get("content")
        if content:
            text_parts.append(str(content))

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
                "💬 Genie Chat", 
                "📚 Dicionário e Perfil de Dados (Profiling)", 
                "⚡ Otimizador e Revisor SQL (Linter)",
                "⚖️ Comparador de Ambientes (Dev vs Prod)"
            ]
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
        space_id = st.text_input(
            "GENIE_SPACE_ID",
            key="config_space_id",
            type="password",
            help="ID da room/space Genie. Use o icone de olho para ocultar/exibir.",
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

        avatar_source = ASSETS_DIR / AGENT_SOURCE_IMAGE_NAME
        if not avatar_source.exists():
            st.caption(
                "Para usar sua imagem do agente, salve o PNG em "
                f"{avatar_source}. O recorte quadrado central e automatico."
            )

    return {
        "host": str(host).strip(),
        "token": str(token).strip(),
        "space_id": str(space_id).strip(),
        "poll_seconds": float(poll_seconds),
        "timeout_seconds": int(timeout_seconds),
        "advanced_mode": bool(advanced_mode),
    }


def get_config_from_state() -> Dict[str, Any]:
    return {
        "host": str(st.session_state.get("config_host", "")).strip(),
        "token": str(st.session_state.get("config_token", "")).strip(),
        "space_id": str(st.session_state.get("config_space_id", "")).strip(),
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
    st.markdown(
        "<h3 style='text-align:center; margin-bottom:0.2rem;'>Controle do Chat</h3>",
        unsafe_allow_html=True,
    )
    st.markdown(
        "<p style='text-align:center; margin-top:0; color: rgba(49, 51, 63, 0.65);'>"
        "Cada modo possui conversa independente. Selecione o modo e acione os comandos abaixo."
        "</p>",
        unsafe_allow_html=True,
    )

    st.markdown("**Escolha o modo da interface**")
    col_user_mode, col_dev_mode = st.columns(2)
    with col_user_mode:
        user_is_active = st.session_state.active_ui_mode == UI_MODE_USER
        if st.button(
            "Usuário",
            key="active_ui_mode_user",
            type="primary" if user_is_active else "secondary",
            use_container_width=True,
            help=(
                "Modo Usuário: foco em explicação simples e objetiva, linguagem de negócio, "
                "com menor detalhe técnico e SQL apenas quando essencial."
            ),
        ):
            st.session_state.active_ui_mode = UI_MODE_USER
            st.rerun()

    with col_dev_mode:
        dev_is_active = st.session_state.active_ui_mode == UI_MODE_DEVELOPER
        if st.button(
            "Desenvolvedor",
            key="active_ui_mode_dev",
            type="primary" if dev_is_active else "secondary",
            use_container_width=True,
            help=(
                "Modo Desenvolvedor: foco em detalhe técnico, SQL, métrica, validação e rastreabilidade "
                "da resposta para analise aprofundada."
            ),
        ):
            st.session_state.active_ui_mode = UI_MODE_DEVELOPER
            st.rerun()

    selected_mode = st.session_state.active_ui_mode
    st.caption(f"Modo ativo: {selected_mode}")

    return selected_mode


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

    table_input = f"dev.iops_rj.{selected_table}"
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
    st.header("📚 Dicionário e Perfil de Dados (Profiling)")
    st.write("Visualize os metadados e a distribuição estatística dos dados (Data Profiling) das tabelas no esquema `dev.iops_rj`.")
    
    if not config.get("host") or not config.get("token") or not config.get("space_id"):
        st.caption("Preencha as credenciais na barra lateral para usar a ferramenta.")
        return

    client = GenieApiClient(config["host"], config["token"], config["space_id"])
    
    if "dev_iops_rj_tables" not in st.session_state:
        with st.spinner("Carregando lista de tabelas de dev.iops_rj..."):
            try:
                space_payload = client.get_space()
                warehouse_id = space_payload.get("warehouse_id")
                
                show_payload = client.execute_sql_statement(
                    warehouse_id=warehouse_id,
                    statement="SHOW TABLES IN dev.iops_rj",
                    timeout_seconds=config.get("timeout_seconds", 60)
                )
                
                rows = show_payload.get("result", {}).get("data_array", [])
                cols = show_payload.get("manifest", {}).get("schema", {}).get("columns", [])
                col_names = [c.get("name", "") for c in cols]
                
                table_names = []
                if rows and col_names:
                    df_tables = pd.DataFrame(rows, columns=col_names)
                    if "tableName" in df_tables.columns:
                        table_names = df_tables["tableName"].tolist()
                    else:
                        table_names = [row[1] if len(row) > 1 else row[0] for row in rows]
                
                st.session_state["dev_iops_rj_tables"] = sorted(table_names)
            except Exception as e:
                st.error(f"Não foi possível carregar a lista de tabelas: {e}")
                st.session_state["dev_iops_rj_tables"] = []

    table_options = st.session_state.get("dev_iops_rj_tables", [])
    
    col1, col2 = st.columns([3, 1])
    with col1:
        selected_table = st.selectbox("Selecione a Tabela em dev.iops_rj", [""] + table_options)
        final_table = f"dev.iops_rj.{selected_table}" if selected_table else ""
        
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
    st.header("⚡ Otimizador e Revisor SQL (Linter)")
    st.write("Cole sua query abaixo para receber análises de performance, dicas de otimização e revisão automática de boas práticas de código (SQL Linter) no Databricks.")
    
    if not config.get("host") or not config.get("token") or not config.get("space_id"):
        st.caption("Preencha as credenciais na barra lateral para usar a ferramenta.")
        return

    query_input = st.text_area("Insira a Query SQL", height=250)
    
    if st.button("Analisar e Otimizar Query", type="primary"):
        if not query_input.strip():
            st.error("Informe a query.")
            return
            
        prompt = (
            f"Atue como um Engenheiro de Dados Especialista em Databricks. "
            f"Analise a seguinte query SQL e forneça um relatório técnico contendo:\n"
            f"1. Resumo do que a query faz.\n"
            f"2. Sugestões de Otimização de Performance (ex: particionamento, Z-Order, hints de join, evitar cross joins).\n"
            f"3. Dicas de legibilidade e boas práticas.\n"
            f"4. A query refatorada e otimizada (se aplicável).\n\n"
            f"Query:\n```sql\n{query_input}\n```"
        )
        
        client = GenieApiClient(config["host"], config["token"], config["space_id"])
        
        with st.spinner("O Genie está analisando sua query. Isso pode levar alguns segundos..."):
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
                
                # strip <analytics> if it exists
                clean_text = raw_text.split("<analytics>")[0].strip()
                
                st.markdown("### Análise e Sugestões")
                st.markdown(clean_text)
                
            except Exception as e:
                st.error(f"Falha ao analisar a query: {e}")

def render_environment_comparator(config: Dict[str, Any]) -> None:
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

def run_genie_chat_mode(config: Dict[str, Any], ui_mode: str) -> None:
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
    setup_page()
    init_state()

    apply_sidebar_visibility(st.session_state.active_ui_mode)

    if st.session_state.active_ui_mode == UI_MODE_USER:
        config = get_config_from_state()
        app_mode = "💬 Genie Chat"
    else:
        config = render_sidebar()
        app_mode = st.session_state.get("app_mode", "💬 Genie Chat")

    render_top_branding()
    ui_mode = render_interface_mode_top()

    if app_mode == "💬 Genie Chat":
        run_genie_chat_mode(config, ui_mode)
    elif app_mode == "📚 Dicionário e Perfil de Dados (Profiling)":
        render_data_dictionary_and_profiling(config)
    elif app_mode == "⚡ Otimizador e Revisor SQL (Linter)":
        render_sql_optimizer(config)
    elif app_mode == "⚖️ Comparador de Ambientes (Dev vs Prod)":
        render_environment_comparator(config)


if __name__ == "__main__":
    main()
