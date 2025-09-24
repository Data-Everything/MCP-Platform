"""
Enhanced Gateway CLI commands with authentication, database management, and improved features.

Provides comprehensive gateway management including user management, API key creation,
database operations, and advanced configuration options.
"""

import asyncio
import getpass
import logging
import os
import secrets
from pathlib import Path
from typing import Optional

import aiohttp
import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .auth import initialize_auth
from .database import initialize_database
from .gateway_server import MCPGatewayServer
from .models import AuthConfig, DatabaseConfig, GatewayConfig

logger = logging.getLogger(__name__)
console = Console()

# Gateway CLI app
gateway_app = typer.Typer(
    name="gateway",
    help="Enhanced MCP Gateway management commands",
    rich_markup_mode="rich",
)


@gateway_app.command("start")
def start_gateway(
    host: str = typer.Option(
        "0.0.0.0", "--host", "-h", help="Host to bind the gateway to"
    ),
    port: int = typer.Option(8080, "--port", "-p", help="Port to bind the gateway to"),
    database_url: Optional[str] = typer.Option(
        None, "--database", "-d", help="Database URL (defaults to SQLite)"
    ),
    registry_file: Optional[str] = typer.Option(
        "registry.json", "--registry", "-r", help="Fallback registry file"
    ),
    secret_key: Optional[str] = typer.Option(
        None, "--secret-key", help="JWT secret key (generates random if not provided)"
    ),
    cors_origins: str = typer.Option(
        "*", "--cors-origins", help="CORS allowed origins (comma-separated)"
    ),
    log_level: str = typer.Option("INFO", "--log-level", help="Log level"),
    workers: int = typer.Option(1, "--workers", help="Number of worker processes"),
    reload: bool = typer.Option(
        False, "--reload", help="Enable auto-reload for development"
    ),
):
    """
    Start the enhanced MCP Gateway server with authentication and database persistence.

    The gateway provides a unified HTTP endpoint for accessing all deployed MCP servers
    with authentication, load balancing, health checking, and comprehensive management.
    """
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Default database URL
    if not database_url:
        db_path = Path.home() / ".mcp" / "gateway.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        database_url = f"sqlite:///{db_path}"

    # Generate secret key if not provided
    if not secret_key:
        secret_key = os.getenv("GATEWAY_SECRET_KEY")
        if not secret_key:
            secret_key = secrets.token_urlsafe(32)
            console.print(
                "[yellow]Generated random secret key. Set GATEWAY_SECRET_KEY environment variable for production.[/yellow]"
            )

    # Parse CORS origins
    cors_origins_list = [origin.strip() for origin in cors_origins.split(",")]

    try:
        # Create configuration
        config = GatewayConfig(
            host=host,
            port=port,
            reload=reload,
            workers=workers,
            log_level=log_level.lower(),
            cors_origins=cors_origins_list,
            database=DatabaseConfig(url=database_url),
            auth=AuthConfig(secret_key=secret_key),
        )

        # Create gateway server
        gateway = MCPGatewayServer(config)

        # Display startup info
        console.print(
            Panel.fit(
                f"[bold]Enhanced MCP Gateway Server[/bold]\n\n"
                f"• Gateway URL: [cyan]http://{host}:{port}[/cyan]\n"
                f"• Database: [dim]{database_url}[/dim]\n"
                f"• Registry fallback: [dim]{registry_file}[/dim]\n"
                f"• Workers: [dim]{workers}[/dim]\n"
                f"• Log level: [dim]{log_level}[/dim]\n\n"
                f"[bold]Authentication Endpoints:[/bold]\n"
                f"• Login: [cyan]POST /auth/login[/cyan]\n"
                f"• Create user: [cyan]POST /auth/users[/cyan]\n"
                f"• Create API key: [cyan]POST /auth/api-keys[/cyan]\n\n"
                f"[bold]Gateway Endpoints:[/bold]\n"
                f"• Health: [cyan]GET /gateway/health[/cyan]\n"
                f"• Stats: [cyan]GET /gateway/stats[/cyan]\n"
                f"• Templates: [cyan]GET /gateway/templates[/cyan]\n\n"
                f"[bold]MCP Endpoints:[/bold]\n"
                f"• List tools: [cyan]GET /mcp/{{template}}/tools/list[/cyan]\n"
                f"• Call tool: [cyan]POST /mcp/{{template}}/tools/call[/cyan]",
                title="🚀 Starting Enhanced Gateway",
                border_style="green",
            )
        )

        # Run the server
        gateway.run()

    except Exception as e:
        console.print(f"[red]✗ Failed to start gateway: {e}[/red]")
        raise typer.Exit(1)


@gateway_app.command("create-user")
def create_user(
    username: str = typer.Argument(..., help="Username for the new user"),
    email: Optional[str] = typer.Option(None, "--email", "-e", help="User email"),
    password: Optional[str] = typer.Option(
        None, "--password", "-p", help="User password"
    ),
    superuser: bool = typer.Option(False, "--superuser", help="Create as superuser"),
    database_url: Optional[str] = typer.Option(
        None, "--database", "-d", help="Database URL"
    ),
):
    """
    Create a new user for gateway authentication.

    If password is not provided, you will be prompted to enter it securely.
    """
    if not password:
        password = getpass.getpass("Password: ")
        confirm_password = getpass.getpass("Confirm password: ")
        if password != confirm_password:
            console.print("[red]Passwords do not match[/red]")
            raise typer.Exit(1)

    async def _create_user():
        # Default database URL
        if not database_url:
            db_path = Path.home() / ".mcp" / "gateway.db"
            db_url = f"sqlite:///{db_path}"
        else:
            db_url = database_url

        # Initialize database and auth
        config = GatewayConfig(
            database=DatabaseConfig(url=db_url),
            auth=AuthConfig(secret_key=secrets.token_urlsafe(32)),
        )

        db = await initialize_database(config)
        auth = initialize_auth(config.auth, db)

        try:
            user = await auth.create_user(
                username=username,
                email=email,
                password=password,
                is_superuser=superuser,
            )
            console.print(f"[green]✓ Created user '{username}' (ID: {user.id})[/green]")
            if superuser:
                console.print("[yellow]User has superuser privileges[/yellow]")
        except Exception as e:
            console.print(f"[red]✗ Failed to create user: {e}[/red]")
            raise typer.Exit(1)
        finally:
            await db.close()

    asyncio.run(_create_user())


@gateway_app.command("create-api-key")
def create_api_key(
    username: str = typer.Argument(..., help="Username to create API key for"),
    name: str = typer.Option(..., "--name", "-n", help="API key name"),
    description: Optional[str] = typer.Option(
        None, "--description", help="API key description"
    ),
    scopes: str = typer.Option(
        "gateway:read,gateway:write,tools:call",
        "--scopes",
        help="Comma-separated list of scopes",
    ),
    expires_days: int = typer.Option(30, "--expires", help="Expiration in days"),
    database_url: Optional[str] = typer.Option(
        None, "--database", "-d", help="Database URL"
    ),
):
    """
    Create an API key for a user.

    The API key will be displayed once and cannot be retrieved again.
    """

    async def _create_api_key():
        # Default database URL
        if not database_url:
            db_path = Path.home() / ".mcp" / "gateway.db"
            db_url = f"sqlite:///{db_path}"
        else:
            db_url = database_url

        # Initialize database and auth
        config = GatewayConfig(
            database=DatabaseConfig(url=db_url),
            auth=AuthConfig(secret_key=secrets.token_urlsafe(32)),
        )

        db = await initialize_database(config)
        auth = initialize_auth(config.auth, db)

        try:
            # Get user
            from .database import UserCRUD

            user_crud = UserCRUD(db)
            user = await user_crud.get_by_username(username)
            if not user:
                console.print(f"[red]✗ User '{username}' not found[/red]")
                raise typer.Exit(1)

            # Parse scopes
            scopes_list = [scope.strip() for scope in scopes.split(",")]

            # Create API key
            api_key_record, api_key = await auth.create_api_key(
                user_id=user.id,
                name=name,
                description=description,
                scopes=scopes_list,
                expires_days=expires_days,
            )

            console.print(
                f"[green]✓ Created API key '{name}' for user '{username}'[/green]"
            )
            console.print(f"[bold]API Key:[/bold] [cyan]{api_key}[/cyan]")
            console.print(
                "[yellow]⚠️  Save this key securely. It cannot be retrieved again.[/yellow]"
            )

            # Display details
            table = Table(title="API Key Details")
            table.add_column("Property", style="bold")
            table.add_column("Value")
            table.add_row("ID", str(api_key_record.id))
            table.add_row("Name", api_key_record.name)
            table.add_row("Description", api_key_record.description or "None")
            table.add_row("Scopes", ", ".join(api_key_record.scopes))
            table.add_row(
                "Expires",
                (
                    api_key_record.expires_at.isoformat()
                    if api_key_record.expires_at
                    else "Never"
                ),
            )
            console.print(table)

        except Exception as e:
            console.print(f"[red]✗ Failed to create API key: {e}[/red]")
            raise typer.Exit(1)
        finally:
            await db.close()

    asyncio.run(_create_api_key())


@gateway_app.command("list-users")
def list_users(
    database_url: Optional[str] = typer.Option(
        None, "--database", "-d", help="Database URL"
    ),
):
    """
    List all users in the gateway database.
    """

    async def _list_users():
        # Default database URL
        if not database_url:
            db_path = Path.home() / ".mcp" / "gateway.db"
            db_url = f"sqlite:///{db_path}"
        else:
            db_url = database_url

        # Initialize database
        config = GatewayConfig(
            database=DatabaseConfig(url=db_url),
            auth=AuthConfig(secret_key=secrets.token_urlsafe(32)),
        )

        db = await initialize_database(config)

        try:
            from .database import UserCRUD

            user_crud = UserCRUD(db)
            users = await user_crud.list_all() if hasattr(user_crud, "list_all") else []

            if not users:
                console.print("[yellow]No users found[/yellow]")
                return

            table = Table(title="Gateway Users")
            table.add_column("ID", style="bold")
            table.add_column("Username")
            table.add_column("Email")
            table.add_column("Active")
            table.add_column("Superuser")
            table.add_column("Created")

            for user in users:
                table.add_row(
                    str(user.id),
                    user.username,
                    user.email or "None",
                    "✓" if user.is_active else "✗",
                    "✓" if user.is_superuser else "✗",
                    (
                        user.created_at.strftime("%Y-%m-%d %H:%M")
                        if user.created_at
                        else "Unknown"
                    ),
                )

            console.print(table)

        except Exception as e:
            console.print(f"[red]✗ Failed to list users: {e}[/red]")
            raise typer.Exit(1)
        finally:
            await db.close()

    asyncio.run(_list_users())


@gateway_app.command("db-init")
def initialize_database_cmd(
    database_url: Optional[str] = typer.Option(
        None, "--database", "-d", help="Database URL"
    ),
    force: bool = typer.Option(False, "--force", help="Force re-initialization"),
):
    """
    Initialize the gateway database with required tables.
    """

    async def _init_db():
        # Default database URL
        if not database_url:
            db_path = Path.home() / ".mcp" / "gateway.db"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            db_url = f"sqlite:///{db_path}"
        else:
            db_url = database_url

        config = GatewayConfig(
            database=DatabaseConfig(url=db_url),
            auth=AuthConfig(secret_key=secrets.token_urlsafe(32)),
        )

        try:
            db = await initialize_database(config)
            console.print("[green]✓ Database initialized successfully[/green]")
            console.print(f"Database URL: [cyan]{db_url}[/cyan]")
            await db.close()
        except Exception as e:
            console.print(f"[red]✗ Failed to initialize database: {e}[/red]")
            raise typer.Exit(1)

    asyncio.run(_init_db())


@gateway_app.command("status")
def gateway_status(
    host: str = typer.Option("localhost", "--host", help="Gateway host"),
    port: int = typer.Option(8080, "--port", help="Gateway port"),
    api_key: Optional[str] = typer.Option(
        None, "--api-key", help="API key for authentication"
    ),
):
    """
    Check the status of a running gateway server.
    """

    async def _check_status():
        gateway_url = f"http://{host}:{port}"
        headers = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        try:
            async with aiohttp.ClientSession(headers=headers) as session:
                # Check health
                async with session.get(f"{gateway_url}/gateway/health") as resp:
                    if resp.status == 200:
                        health_data = await resp.json()
                        console.print(
                            f"[green]✓ Gateway is running at {gateway_url}[/green]"
                        )

                        # Display health info
                        table = Table(title="Gateway Health")
                        table.add_column("Component", style="bold")
                        table.add_column("Status")
                        table.add_column("Details")

                        table.add_row(
                            "Gateway", health_data.get("status", "unknown"), ""
                        )
                        table.add_row(
                            "Database", health_data.get("database", "unknown"), ""
                        )
                        table.add_row(
                            "Templates", "info", str(health_data.get("templates", 0))
                        )

                        instances = health_data.get("instances", {})
                        table.add_row(
                            "Instances",
                            "info",
                            f"Total: {instances.get('total', 0)}, Healthy: {instances.get('healthy', 0)}",
                        )

                        uptime = health_data.get("uptime", 0)
                        table.add_row("Uptime", "info", f"{uptime:.1f}s")

                        console.print(table)
                    else:
                        console.print(
                            f"[red]✗ Gateway health check failed (status: {resp.status})[/red]"
                        )

        except aiohttp.ClientError as e:
            console.print(
                f"[red]✗ Cannot connect to gateway at {gateway_url}: {e}[/red]"
            )
        except Exception as e:
            console.print(f"[red]✗ Error checking gateway status: {e}[/red]")

    asyncio.run(_check_status())


@gateway_app.command("test-auth")
def test_authentication(
    username: str = typer.Argument(..., help="Username to test"),
    password: Optional[str] = typer.Option(None, "--password", help="Password"),
    host: str = typer.Option("localhost", "--host", help="Gateway host"),
    port: int = typer.Option(8080, "--port", help="Gateway port"),
):
    """
    Test authentication with the gateway server.
    """
    if not password:
        password = getpass.getpass("Password: ")

    async def _test_auth():
        gateway_url = f"http://{host}:{port}"

        try:
            async with aiohttp.ClientSession() as session:
                # Try to login
                login_data = {
                    "username": username,
                    "password": password,
                }

                async with session.post(
                    f"{gateway_url}/auth/login", json=login_data
                ) as resp:
                    if resp.status == 200:
                        token_data = await resp.json()
                        console.print("[green]✓ Authentication successful[/green]")
                        console.print(
                            f"Access token: [cyan]{token_data['access_token'][:50]}...[/cyan]"
                        )
                        console.print(
                            f"Expires in: [dim]{token_data['expires_in']}s[/dim]"
                        )

                        # Test authenticated request
                        headers = {
                            "Authorization": f"Bearer {token_data['access_token']}"
                        }
                        async with session.get(
                            f"{gateway_url}/gateway/stats", headers=headers
                        ) as stats_resp:
                            if stats_resp.status == 200:
                                console.print(
                                    "[green]✓ Authenticated request successful[/green]"
                                )
                            else:
                                console.print(
                                    f"[yellow]⚠️  Authenticated request failed (status: {stats_resp.status})[/yellow]"
                                )
                    else:
                        error_data = await resp.json()
                        console.print(
                            f"[red]✗ Authentication failed: {error_data.get('detail', 'Unknown error')}[/red]"
                        )

        except Exception as e:
            console.print(f"[red]✗ Error testing authentication: {e}[/red]")

    asyncio.run(_test_auth())


@gateway_app.command("register")
def register_server(
    template_name: str = typer.Argument(..., help="Template name to register"),
    endpoint: Optional[str] = typer.Option(
        None, "--endpoint", "-e", help="HTTP endpoint URL"
    ),
    command: Optional[str] = typer.Option(
        None, "--command", "-c", help="Command to run for stdio server"
    ),
    working_dir: Optional[str] = typer.Option(
        None, "--working-dir", "-w", help="Working directory for stdio server"
    ),
    transport: str = typer.Option(
        "http", "--transport", help="Transport type (http/stdio)"
    ),
    backend: str = typer.Option("docker", "--backend", help="Backend type"),
    host: str = typer.Option("localhost", "--host", help="Gateway host"),
    port: int = typer.Option(8080, "--port", help="Gateway port"),
    api_key: Optional[str] = typer.Option(
        None, "--api-key", help="API key for authentication"
    ),
):
    """
    Register a server instance with the gateway.

    Examples:
      mcpp gateway register mytemplate --endpoint http://localhost:7071
      mcpp gateway register mytemplate --command "python server.py" --working-dir /app
    """
    console.print(
        "[yellow]Note: register command is not yet fully implemented[/yellow]"
    )
    console.print(f"Would register template '{template_name}' with:")
    if endpoint:
        console.print(f"  Endpoint: {endpoint}")
    if command:
        console.print(f"  Command: {command}")
        if working_dir:
            console.print(f"  Working dir: {working_dir}")


@gateway_app.command("list")
def list_instances(
    template: Optional[str] = typer.Option(
        None, "--template", help="Filter by template name"
    ),
    show_all: bool = typer.Option(
        False, "--all", help="Show all instances including unhealthy"
    ),
    host: str = typer.Option("localhost", "--host", help="Gateway host"),
    port: int = typer.Option(8080, "--port", help="Gateway port"),
    api_key: Optional[str] = typer.Option(
        None, "--api-key", help="API key for authentication"
    ),
):
    """
    List registered server instances and templates.
    """

    async def _list_instances():
        gateway_url = f"http://{host}:{port}"
        headers = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        try:
            async with aiohttp.ClientSession(headers=headers) as session:
                # Get templates and instances
                async with session.get(f"{gateway_url}/gateway/templates") as resp:
                    if resp.status == 200:
                        templates_data = await resp.json()

                        if not templates_data.get("templates"):
                            console.print("[yellow]No templates registered[/yellow]")
                            return

                        # Create table
                        table = Table(title="Registered Gateway Instances")
                        table.add_column("Template", style="bold cyan")
                        table.add_column("Instance Count")
                        table.add_column("Healthy")
                        table.add_column("Strategy")

                        for template_name in templates_data["templates"]:
                            if template and template_name != template:
                                continue

                            # For now, show template info - actual instances would need API support
                            table.add_row(template_name, "0", "0", "round_robin")

                        console.print(table)

                    else:
                        console.print(
                            f"[red]✗ Failed to list instances (status: {resp.status})[/red]"
                        )

        except aiohttp.ClientError as e:
            console.print(
                f"[red]✗ Cannot connect to gateway at {gateway_url}: {e}[/red]"
            )
        except Exception as e:
            console.print(f"[red]✗ Error listing instances: {e}[/red]")

    asyncio.run(_list_instances())


@gateway_app.command("remove")
def remove_instance(
    template_name: str = typer.Argument(..., help="Template name"),
    instance_id: str = typer.Argument(..., help="Instance ID to remove"),
    host: str = typer.Option("localhost", "--host", help="Gateway host"),
    port: int = typer.Option(8080, "--port", help="Gateway port"),
):
    """
    Remove/deregister a server instance from the gateway.
    """
    console.print("[yellow]Note: remove command is not yet fully implemented[/yellow]")
    console.print(
        f"Would remove instance '{instance_id}' from template '{template_name}'"
    )


@gateway_app.command("stop")
def stop_gateway(
    host: str = typer.Option("localhost", "--host", help="Gateway host"),
    port: int = typer.Option(8080, "--port", help="Gateway port"),
    force: bool = typer.Option(
        False, "--force", help="Force stop without confirmation"
    ),
):
    """
    Stop the gateway server gracefully.
    """

    if not force:
        confirm = typer.confirm("Are you sure you want to stop the gateway server?")
        if not confirm:
            console.print("[yellow]Cancelled[/yellow]")
            return

    # For now, just show what would happen
    console.print("[yellow]Note: stop command is not yet fully implemented[/yellow]")
    console.print(f"Would attempt to stop gateway at http://{host}:{port}")
    console.print(
        "[yellow]Use Ctrl+C in the terminal where gateway is running, or kill the process[/yellow]"
    )
