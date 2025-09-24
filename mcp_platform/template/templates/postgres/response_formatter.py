#!/usr/bin/env python3
"""
Postgres response formatter for the Postgres template.

Provides a PostgresResponseFormatter class that knows how to parse and render
common Postgres tool outputs (databases, schemas, tables, describe_table,
query results, connection info) using rich tables and panels.
"""

import json
import logging
from typing import Any, Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

logger = logging.getLogger(__name__)


class PostgresResponseFormatter:
    """Formatter for Postgres tool responses."""

    def __init__(self, console: Optional[Console] = None):
        self.console = console or Console()

    def format_tool_response(self, tool_name: str, raw_response: str) -> None:
        try:
            if tool_name == "list_databases":
                self._format_list_databases(raw_response)
            elif tool_name == "list_schemas":
                self._format_list_schemas(raw_response)
            elif tool_name == "list_tables":
                self._format_list_tables(raw_response)
            elif tool_name == "describe_table":
                self._format_describe_table(raw_response)
            elif tool_name == "execute_query":
                self._format_execute_query(raw_response)
            elif tool_name in ("get_database_info", "get_connection_info"):
                self._format_connection_info(raw_response)
            else:
                self._format_default(raw_response)
        except Exception as e:
            logger.warning("Postgres formatter failed for %s: %s", tool_name, e)
            self._format_default(raw_response)

    def _clean(self, text: str) -> str:
        if not text:
            return ""
        text = text.strip()
        if text.startswith('"') and text.endswith('"'):
            try:
                text = json.loads(text)
            except Exception:
                text = text[1:-1]
        return text

    def _parse_json(self, text: str) -> Optional[Any]:
        try:
            return json.loads(text)
        except Exception:
            return None

    def _format_list_databases(self, response: str) -> None:
        text = self._clean(response)
        data = self._parse_json(text)

        dbs = []
        if isinstance(data, dict) and "databases" in data:
            dbs = data.get("databases") or []
        elif isinstance(data, list):
            dbs = data
        else:
            dbs = [line.strip() for line in text.splitlines() if line.strip()]

        if not dbs:
            self.console.print("[yellow]No databases found[/yellow]")
            return

        table = Table(title=f"Databases ({len(dbs)})", show_header=True)
        table.add_column("#", style="cyan", width=4)
        table.add_column("Database", style="green")

        for i, d in enumerate(dbs, 1):
            table.add_row(str(i), str(d))

        self.console.print(table)

    def _format_list_schemas(self, response: str) -> None:
        text = self._clean(response)
        data = self._parse_json(text)

        schemas = []
        if isinstance(data, dict) and "schemas" in data:
            schemas = data.get("schemas") or []
        elif isinstance(data, list):
            schemas = data
        else:
            schemas = [line.strip() for line in text.splitlines() if line.strip()]

        if not schemas:
            self.console.print("[yellow]No schemas found[/yellow]")
            return

        table = Table(title=f"Schemas ({len(schemas)})", show_header=True)
        table.add_column("#", style="cyan", width=4)
        table.add_column("Schema", style="green")

        for i, s in enumerate(schemas, 1):
            table.add_row(str(i), str(s))

        self.console.print(table)

    def _format_list_tables(self, response: str) -> None:
        text = self._clean(response)
        data = self._parse_json(text)

        tables = []
        if isinstance(data, dict) and "tables" in data:
            tables = data.get("tables") or []
        elif isinstance(data, list):
            tables = data
        else:
            tables = [line.strip() for line in text.splitlines() if line.strip()]

        if not tables:
            self.console.print("[yellow]No tables found[/yellow]")
            return

        rows = []
        for t in tables:
            if isinstance(t, dict):
                rows.append(t)
            else:
                if "." in str(t):
                    schema, table_name = str(t).split(".", 1)
                    rows.append({"schema": schema, "table": table_name})
                else:
                    rows.append({"table": str(t)})

        table = Table(title=f"Tables ({len(rows)})", show_header=True)
        table.add_column("#", style="cyan", width=4)
        cols = [k for k in ("schema", "table") if any(k in r for r in rows)]
        for c in cols:
            table.add_column(c.title(), style="yellow")

        for i, r in enumerate(rows, 1):
            values = [str(r.get(c, "")) for c in cols]
            table.add_row(str(i), *values)

        self.console.print(table)

    def _format_describe_table(self, response: str) -> None:
        text = self._clean(response)
        data = self._parse_json(text)

        columns = []
        if isinstance(data, dict) and "columns" in data:
            columns = data.get("columns") or []
        elif isinstance(data, list):
            columns = data
        else:
            lines = [line for line in text.splitlines() if line.strip()]
            for ln in lines:
                parts = [p.strip() for p in ln.split("|")]
                if len(parts) >= 2:
                    columns.append(
                        {
                            "column": parts[0],
                            "type": parts[1],
                            "comment": parts[2] if len(parts) > 2 else "",
                        }
                    )

        if not columns:
            self.console.print("[yellow]No schema information available[/yellow]")
            return

        tbl = Table(title=f"Table Schema ({len(columns)} columns)", show_header=True)
        tbl.add_column("#", style="cyan", width=4)
        tbl.add_column("Column", style="yellow")
        tbl.add_column("Type", style="green")
        tbl.add_column("Nullable", style="magenta", width=8)
        tbl.add_column("Comment", style="white", overflow="ellipsis")

        for i, col in enumerate(columns, 1):
            name = col.get("column") or col.get("name") or col.get("field") or ""
            ctype = col.get("type") or col.get("datatype") or ""
            nullable = str(col.get("nullable", ""))
            comment = col.get("comment", "")
            tbl.add_row(str(i), name, ctype, nullable, comment)

        self.console.print(tbl)

    def _format_execute_query(self, response: str) -> None:
        text = self._clean(response)
        data = self._parse_json(text)

        # Detect obvious error payload
        if isinstance(data, dict) and data.get("error"):
            self.console.print(
                Panel(str(data.get("error")), title="Error", border_style="red")
            )
            return

        rows = None
        columns = None
        # Support several shapes from tool output
        if isinstance(data, dict):
            if "data" in data and isinstance(data["data"], list):
                rows = data["data"]
            elif "rows" in data and isinstance(data["rows"], list):
                rows = data["rows"]
            elif "structuredContent" in data and isinstance(
                data["structuredContent"], dict
            ):
                sc = data["structuredContent"]
                if isinstance(sc.get("rows"), list):
                    rows = sc.get("rows")

            if "columns" in data and isinstance(data["columns"], list):
                columns = data["columns"]

        if rows is None:
            # Nothing to tabulate: show raw text
            if text:
                self.console.print(Panel(text, title="Result", border_style="dim"))
            else:
                self.console.print("[yellow]No result returned[/yellow]")
            return

        if not rows:
            self.console.print("[yellow]No rows returned[/yellow]")
            return

        # If rows are list of dicts, infer columns
        if columns is None and isinstance(rows, list) and isinstance(rows[0], dict):
            columns = list(rows[0].keys())

        table = Table(title=f"Query Results ({len(rows)} rows)", show_header=True)
        table.add_column("#", style="cyan", width=4)
        for c in columns or []:
            table.add_column(str(c), style="yellow")

        for i, r in enumerate(rows, 1):
            if isinstance(r, dict):
                values = [str(r.get(c, "")) for c in (columns or [])]
            else:
                values = [str(v) for v in r]
            table.add_row(str(i), *values)

        self.console.print(table)

    def _format_connection_info(self, response: str) -> None:
        text = self._clean(response)
        data = self._parse_json(text)
        if not data:
            self.console.print(Panel(text, title="Connection Info", border_style="dim"))
            return

        # Pretty print key/value pairs
        tbl = Table(show_header=False)
        tbl.add_column("Key", style="cyan", width=24)
        tbl.add_column("Value", style="white")

        if isinstance(data, dict):
            for k, v in data.items():
                tbl.add_row(
                    str(k),
                    (
                        json.dumps(v)
                        if not isinstance(v, (str, int, float, bool, type(None)))
                        else str(v)
                    ),
                )

        self.console.print(Panel(tbl, title="Connection Info"))

    def _format_default(self, response: str) -> None:
        text = self._clean(response)
        if not text:
            self.console.print("[yellow]No response[/yellow]")
            return
        self.console.print(Panel(text, title="Response"))
