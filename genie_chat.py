import argparse
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

TERMINAL_STATUSES = {"COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"}
REQUEST_TIMEOUT_SECONDS = 90
SQL_TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "CANCELED", "CLOSED"}


class GenieApiClient:
    def __init__(self, host: str, token: str, space_id: str) -> None:
        self.host = host.rstrip("/")
        self.space_id = space_id
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        payload: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = f"{self.host}{path}"
        response = self.session.request(
            method,
            url,
            json=payload,
            params=params,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )

        if response.status_code >= 400:
            body_preview = response.text[:3000]
            raise RuntimeError(
                f"{method} {path} failed with status {response.status_code}: {body_preview}"
            )

        if not response.text:
            return {}

        return response.json()

    def start_conversation(self, content: str) -> Dict[str, Any]:
        return self._request(
            "POST",
            f"/api/2.0/genie/spaces/{self.space_id}/start-conversation",
            payload={"content": content},
        )

    def create_message(self, conversation_id: str, content: str) -> Dict[str, Any]:
        return self._request(
            "POST",
            f"/api/2.0/genie/spaces/{self.space_id}/conversations/{conversation_id}/messages",
            payload={"content": content},
        )

    def get_message(self, conversation_id: str, message_id: str) -> Dict[str, Any]:
        return self._request(
            "GET",
            f"/api/2.0/genie/spaces/{self.space_id}/conversations/{conversation_id}/messages/{message_id}",
        )

    def get_attachment_query_result(
        self, conversation_id: str, message_id: str, attachment_id: str
    ) -> Dict[str, Any]:
        return self._request(
            "GET",
            (
                f"/api/2.0/genie/spaces/{self.space_id}/conversations/{conversation_id}"
                f"/messages/{message_id}/attachments/{attachment_id}/query-result"
            ),
        )

    def list_spaces(self) -> Dict[str, Any]:
        return self._request(
            "GET",
            "/api/2.0/genie/spaces",
        )

    def get_space(self, include_serialized_space: bool = False) -> Dict[str, Any]:
        params = {}
        if include_serialized_space:
            params["include_serialized_space"] = "true"
        return self._request(
            "GET",
            f"/api/2.0/genie/spaces/{self.space_id}",
            params=params
        )

    def ask_question(self, question: str, poll_seconds: float = 1.0, timeout_seconds: int = 60) -> Dict[str, Any]:
        """
        Inicia uma conversa e aguarda a resposta final (terminal).
        Retorna a mensagem final e o texto da resposta.
        """
        start_res = self.start_conversation(question)
        message = start_res.get("message") or {}
        message_id = extract_message_id(message)
        conversation = start_res.get("conversation") or {}
        conversation_id = extract_conversation_id(conversation)
        
        if not conversation_id or not message_id:
            raise RuntimeError("Não foi possível iniciar a conversa com o Genie.")
            
        final_msg = wait_for_terminal_message(
            self, 
            conversation_id, 
            message_id, 
            poll_seconds,
            timeout_seconds
        )
        
        # Robust text extraction
        text = ""
        text_obj = final_msg.get("text")
        if isinstance(text_obj, dict):
            text = text_obj.get("plain_text", "")
        elif isinstance(text_obj, str):
            text = text_obj
            
        if not text:
            # Fallback for empty responses or unusual structures
            status = final_msg.get("status", "UNKNOWN")
            if status == "SUCCEEDED":
                text = "Solicitação processada com sucesso, mas nenhum texto foi retornado."
            else:
                error_obj = final_msg.get("error")
                if error_obj:
                    text = f"O Genie falhou com erro: {json.dumps(error_obj, ensure_ascii=False)}"
                else:
                    text = f"O Genie finalizou com status: {status}"

            
        return {
            "message": text,
            "conversation_id": conversation_id,
            "message_id": message_id,
            "full_response": final_msg
        }




    def execute_sql_statement(
        self,
        *,
        warehouse_id: str,
        statement: str,
        timeout_seconds: int = 90,
        poll_seconds: float = 1.0,
    ) -> Dict[str, Any]:
        response = self._request(
            "POST",
            "/api/2.0/sql/statements",
            payload={
                "warehouse_id": warehouse_id,
                "statement": statement,
                "wait_timeout": "10s",
                "on_wait_timeout": "CONTINUE",
            },
        )

        statement_id = response.get("statement_id")
        if not statement_id:
            raise RuntimeError("Databricks SQL API did not return statement_id.")

        current = response
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            state = (current.get("status") or {}).get("state")
            if state in SQL_TERMINAL_STATUSES:
                break
            time.sleep(poll_seconds)
            current = self._request("GET", f"/api/2.0/sql/statements/{statement_id}")

        state = (current.get("status") or {}).get("state")
        if state != "SUCCEEDED":
            raise RuntimeError(
                f"SQL statement finished with state={state}. Response: {json.dumps(current, ensure_ascii=False)[:2000]}"
            )

        return current

    def get_table_lineage(
        self,
        *,
        warehouse_id: str,
        table_full_name: str,
        direction: str = "both",
        timeout_seconds: int = 90,
        poll_seconds: float = 1.0,
    ) -> Dict[str, Any]:
        """
        Query Unity Catalog lineage via system.access.table_lineage.
        table_full_name should be 'catalog.schema.table' or 'schema.table' or 'table'.
        direction: 'upstream', 'downstream', or 'both'
        """
        if not warehouse_id:
            raise RuntimeError("warehouse_id is required to run SQL statements for lineage.")

        table_lower = str(table_full_name or "").strip().lower()
        # sanitize single quotes
        table_safe = table_lower.replace("'", "''")

        def fq(col_prefix: str) -> str:
            return (
                f"lower(concat_ws('.', {col_prefix}_table_catalog, {col_prefix}_table_schema, {col_prefix}_table_name))"
            )

        where_clauses: List[str] = []
        if direction in ("both", "downstream"):
            # downstream: tables that the given table depends on (sources)
            where_clauses.append(f"{fq('source')} = '{table_safe}'")
        if direction in ("both", "upstream"):
            # upstream: tables that depend on the given table (targets)
            where_clauses.append(f"{fq('target')} = '{table_safe}'")

        if not where_clauses:
            raise RuntimeError("Invalid direction for lineage query. Use 'upstream', 'downstream' or 'both'.")

        where_sql = " OR ".join(where_clauses)
        sql = f"SELECT * FROM system.access.table_lineage WHERE {where_sql}"
        return self.execute_sql_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            timeout_seconds=timeout_seconds,
            poll_seconds=poll_seconds,
        )

    def create_space(
        self, 
        title: str, 
        warehouse_id: str, 
        description: Optional[str] = None, 
        serialized_space: Optional[str] = None
    ) -> Dict[str, Any]:
        payload = {
            "title": title,
            "warehouse_id": warehouse_id,
        }
        if description:
            payload["description"] = description
        if serialized_space:
            payload["serialized_space"] = serialized_space
            
        return self._request("POST", "/api/2.0/genie/spaces", payload=payload)



def extract_message_id(payload: Dict[str, Any]) -> Optional[str]:
    return payload.get("message_id") or payload.get("id")


def extract_conversation_id(payload: Dict[str, Any]) -> Optional[str]:
    return payload.get("conversation_id") or payload.get("id")


def wait_for_terminal_message(
    client: GenieApiClient,
    conversation_id: str,
    message_id: str,
    poll_seconds: float,
    timeout_seconds: int,
) -> Dict[str, Any]:
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        message = client.get_message(conversation_id, message_id)
        status = message.get("status")
        if status in TERMINAL_STATUSES:
            return message

        time.sleep(poll_seconds)

    raise TimeoutError(
        f"Timeout waiting for message status after {timeout_seconds} seconds."
    )


def extract_attachment_ids(message: Dict[str, Any]) -> List[str]:
    attachment_ids: List[str] = []
    attachments = message.get("attachments") or []

    if not isinstance(attachments, list):
        return attachment_ids

    for attachment in attachments:
        if not isinstance(attachment, dict):
            continue
        attachment_id = attachment.get("attachment_id") or attachment.get("id")
        if attachment_id:
            attachment_ids.append(attachment_id)

    return attachment_ids


def print_query_result_preview(query_result: Dict[str, Any], max_rows: int) -> None:
    statement_response = query_result.get("statement_response") or {}
    manifest = statement_response.get("manifest") or {}
    schema = manifest.get("schema") or {}
    columns = schema.get("columns") or []
    result = statement_response.get("result") or {}
    rows = result.get("data_array") or []

    if columns:
        column_names = [column.get("name", "") for column in columns]
        print("\nColumns:")
        print(column_names)

    if rows:
        print(f"\nRows (first {min(max_rows, len(rows))}):")
        for row in rows[:max_rows]:
            print(row)
    else:
        print("\nNo tabular rows returned in this chunk.")


def print_message(message: Dict[str, Any]) -> None:
    print("\nStatus:", message.get("status"))

    error = message.get("error")
    if error:
        print("Error from Genie:")
        print(json.dumps(error, indent=2, ensure_ascii=False))

    attachments = message.get("attachments") or []
    if attachments:
        print("\nAttachments summary:")
        print(json.dumps(attachments, indent=2, ensure_ascii=False))
    else:
        print("\nNo attachments returned.")


def run_question(
    client: GenieApiClient,
    question: str,
    *,
    poll_seconds: float,
    timeout_seconds: int,
    max_rows: int,
    conversation_id: Optional[str] = None,
) -> Tuple[str, Dict[str, Any]]:
    if conversation_id is None:
        start_response = client.start_conversation(question)
        conversation = start_response.get("conversation") or {}
        message = start_response.get("message") or {}

        conversation_id = extract_conversation_id(conversation)
        message_id = extract_message_id(message)

        if not conversation_id or not message_id:
            raise RuntimeError(
                "Could not parse conversation_id/message_id from start-conversation response."
            )
    else:
        message = client.create_message(conversation_id, question)
        message_id = extract_message_id(message)
        if not message_id:
            raise RuntimeError("Could not parse message_id from create_message response.")

    final_message = wait_for_terminal_message(
        client,
        conversation_id,
        message_id,
        poll_seconds,
        timeout_seconds,
    )

    print_message(final_message)

    for attachment_id in extract_attachment_ids(final_message):
        try:
            query_result = client.get_attachment_query_result(
                conversation_id, message_id, attachment_id
            )
            print(f"\nQuery result for attachment_id={attachment_id}:")
            print_query_result_preview(query_result, max_rows)
        except Exception as exc:
            print(
                f"\nSkipping query-result fetch for attachment_id={attachment_id}: {exc}",
                file=sys.stderr,
            )

    return conversation_id, final_message


def read_required_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {var_name}")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Interactive Databricks Genie API client."
    )
    parser.add_argument(
        "--question",
        help="Initial question. If omitted, the script asks interactively.",
    )
    parser.add_argument(
        "--no-followup",
        action="store_true",
        help="Send one question and exit.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=float(os.getenv("GENIE_POLL_SECONDS", "2")),
        help="Polling interval while waiting message completion.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=int(os.getenv("GENIE_TIMEOUT_SECONDS", "600")),
        help="Max wait time for each message.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=20,
        help="Max number of rows to print from query result preview.",
    )
    return parser


def main() -> int:
    load_dotenv()
    parser = build_parser()
    args = parser.parse_args()

    try:
        host = read_required_env("DATABRICKS_HOST")
        token = read_required_env("DATABRICKS_TOKEN")
        space_id = read_required_env("GENIE_SPACE_ID")
    except RuntimeError as exc:
        print(exc, file=sys.stderr)
        print("Copy .env.example to .env and fill all required values.", file=sys.stderr)
        return 1

    client = GenieApiClient(host=host, token=token, space_id=space_id)

    initial_question = args.question
    if not initial_question:
        initial_question = input("Initial question: ").strip()

    if not initial_question:
        print("No question provided.", file=sys.stderr)
        return 1

    try:
        conversation_id, _ = run_question(
            client,
            initial_question,
            poll_seconds=args.poll_seconds,
            timeout_seconds=args.timeout_seconds,
            max_rows=args.max_rows,
        )
        print(f"\nconversation_id={conversation_id}")
    except Exception as exc:
        print(f"Failed to run initial question: {exc}", file=sys.stderr)
        return 1

    if args.no_followup:
        return 0

    while True:
        followup = input("\nFollow-up question (or 'exit'): ").strip()
        if followup.lower() in {"exit", "quit", "sair"}:
            break
        if not followup:
            continue

        try:
            conversation_id, _ = run_question(
                client,
                followup,
                poll_seconds=args.poll_seconds,
                timeout_seconds=args.timeout_seconds,
                max_rows=args.max_rows,
                conversation_id=conversation_id,
            )
        except Exception as exc:
            print(f"Failed to run follow-up: {exc}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
