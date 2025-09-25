"""
Docker backend for managing deployments using Docker containers.
"""

import ipaddress
import json
import logging
import os
import socket
import subprocess
import time
import uuid
from contextlib import suppress
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from rich.console import Console
from rich.panel import Panel

from mcp_platform.backends import BaseDeploymentBackend
from mcp_platform.template.utils.discovery import TemplateDiscovery
from mcp_platform.utils import SubProcessRunDummyResult

logger = logging.getLogger(__name__)
console = Console()


STDIO_TIMEOUT = os.getenv("MCP_STDIO_TIMEOUT", "30")
MCP_PLATFORM_NETWORK_NAME = os.getenv("MCP_PLATFORM_NETWORK_NAME", "mcp-platform")

if isinstance(STDIO_TIMEOUT, str):
    try:
        STDIO_TIMEOUT = int(STDIO_TIMEOUT)
    except ValueError:
        logger.warning(
            "Invalid MCP_STDIO_TIMEOUT value '%s', using default 30 seconds",
            os.getenv("MCP_STDIO_TIMEOUT", "30"),
        )
        STDIO_TIMEOUT = 30


class DockerDeploymentService(BaseDeploymentBackend):
    """Docker deployment service using CLI commands.

    This service manages container deployments using Docker CLI commands.
    It handles image pulling, container lifecycle, and provides status monitoring.
    """

    def __init__(self, skip_check: bool = False):
        """Initialize Docker service and verify Docker is available."""
        super().__init__()
        self.backend_name = "docker"
        if not skip_check:
            self._ensure_docker_available()

    @property
    def is_available(self):
        """
        Ensure backend is available
        """

        with suppress(RuntimeError):
            self._ensure_docker_available()
            return True

        return False

    def create_network(self):
        """
        Create a Docker network if it doesn't already exist.
        """
        # If the network already exists, nothing to do
        result = self._run_command(
            ["docker", "network", "inspect", MCP_PLATFORM_NETWORK_NAME], check=False
        )
        if result.returncode == 0:
            logger.debug(
                "Docker network '%s' already exists", MCP_PLATFORM_NETWORK_NAME
            )
            return MCP_PLATFORM_NETWORK_NAME

        logger.info(
            "Creating Docker network '%s' (selecting non-conflicting subnet)",
            MCP_PLATFORM_NETWORK_NAME,
        )

        # Gather existing subnets from current docker networks to avoid conflicts
        existing_subnets = set()
        try:
            out = self._run_command(
                ["docker", "network", "ls", "--format", "{{.Name}}"], check=False
            )
            if out and out.stdout:
                for net_name in out.stdout.splitlines():
                    try:
                        inspect_res = self._run_command(
                            ["docker", "network", "inspect", net_name], check=False
                        )
                        if inspect_res and inspect_res.stdout:
                            try:
                                data = json.loads(inspect_res.stdout)
                            except json.JSONDecodeError:
                                continue
                            # data can be list (docker) - handle both
                            entries = data if isinstance(data, list) else [data]
                            for entry in entries:
                                ipam = entry.get("IPAM", {})
                                for cfg in ipam.get("Config", []) or []:
                                    subnet = cfg.get("Subnet")
                                    if subnet:
                                        try:
                                            existing_subnets.add(
                                                ipaddress.ip_network(subnet)
                                            )
                                        except Exception:
                                            continue
                    except Exception:
                        continue
        except Exception:
            # If we can't list networks, fall back to creating a simple network later
            existing_subnets = set()

        # Candidate /24 ranges to try (private address space, avoids common default 172.17/16)
        candidates = [
            ipaddress.ip_network("172.30.0.0/24"),
            ipaddress.ip_network("172.31.0.0/24"),
            ipaddress.ip_network("172.28.0.0/24"),
            ipaddress.ip_network("172.29.0.0/24"),
            ipaddress.ip_network("172.32.0.0/24"),
        ]

        chosen = None
        for cand in candidates:
            # Check overlap with any existing subnet
            conflict = False
            for existing in existing_subnets:
                if cand.overlaps(existing):
                    conflict = True
                    break
            if not conflict:
                chosen = cand
                break

        # Try creating the network with an IPAM config if we found a candidate
        if chosen:
            subnet = str(chosen)
            gateway = (
                str(list(chosen.hosts())[0])
                if list(chosen.hosts())
                else subnet.split("/")[0]
            )
            ip_range = (
                str(ipaddress.ip_network(f"{chosen.network_address + 256}/{24}"))
                if chosen.prefixlen < 24
                else subnet
            )
            # Ensure ip_range is inside subnet and sane — fallback to subnet if calculation fails
            try:
                if not ipaddress.ip_network(ip_range).subnet_of(chosen):
                    ip_range = subnet
            except Exception:
                ip_range = subnet

            try:
                logger.debug(
                    "Attempting to create docker network %s with subnet %s",
                    MCP_PLATFORM_NETWORK_NAME,
                    subnet,
                )
                self._run_command(
                    [
                        "docker",
                        "network",
                        "create",
                        "--driver",
                        "bridge",
                        "--subnet",
                        subnet,
                        "--gateway",
                        gateway,
                        "--ip-range",
                        ip_range,
                        MCP_PLATFORM_NETWORK_NAME,
                    ]
                )
                logger.info(
                    "Created Docker network '%s' with subnet %s",
                    MCP_PLATFORM_NETWORK_NAME,
                    subnet,
                )
                return MCP_PLATFORM_NETWORK_NAME
            except subprocess.CalledProcessError as e:
                logger.warning(
                    "Failed to create docker network with subnet %s: %s", subnet, e
                )

            except Exception as e:
                logger.warning(
                    "Unexpected error creating docker network with subnet %s: %s",
                    subnet,
                    e,
                )

        # If we couldn't pick a candidate or creation failed, try basic create without IPAM
        try:
            logger.debug(
                "Falling back to basic docker network create for %s",
                MCP_PLATFORM_NETWORK_NAME,
            )
            self._run_command(
                [
                    "docker",
                    "network",
                    "create",
                    "--driver",
                    "bridge",
                    MCP_PLATFORM_NETWORK_NAME,
                ]
            )
            logger.info(
                "Created Docker network '%s' without explicit subnet",
                MCP_PLATFORM_NETWORK_NAME,
            )
            return MCP_PLATFORM_NETWORK_NAME
        except subprocess.CalledProcessError as e:
            logger.error(
                "Failed to create Docker network '%s': %s", MCP_PLATFORM_NETWORK_NAME, e
            )
        except Exception as e:
            logger.error(
                "Unexpected error creating Docker network '%s': %s",
                MCP_PLATFORM_NETWORK_NAME,
                e,
            )

        return None

    # Docker Infrastructure Methods
    def _run_command(
        self, command: List[str], check: bool = True, **kwargs: Any
    ) -> subprocess.CompletedProcess:
        """Execute a shell command and return the result.

        Args:
            command: List of command parts to execute
            check: Whether to raise exception on non-zero exit code

        Returns:
            CompletedProcess with stdout, stderr, and return code

        Raises:
            subprocess.CalledProcessError: If command fails and check=True
        """

        if "stdout" in kwargs or "stderr" in kwargs:
            capture_output = False
        else:
            capture_output = True

        try:
            logger.debug("Running command: %s", " ".join(command))
            result = subprocess.run(  # nosec B603
                command, capture_output=capture_output, text=True, check=check, **kwargs
            )
            logger.debug("Command output: %s", result.stdout)
            if result.stderr:
                logger.debug("Command stderr: %s", result.stderr)
            return result
        except subprocess.CalledProcessError as e:
            logger.debug("Command failed: %s", " ".join(command))
            logger.debug("Exit code: %d", e.returncode)
            logger.debug("Stdout: %s", e.stdout)
            logger.debug("Stderr: %s", e.stderr)
            raise

    def _ensure_docker_available(self):
        """Check if Docker is available and running.

        Raises:
            RuntimeError: If Docker daemon is not available or not running
        """
        try:
            result = self._run_command(
                [self.backend_name, "version", "--format", "json"]
            )
            version_info = json.loads(result.stdout)
            logger.debug(
                "Docker client version: %s",
                version_info.get("Client", {}).get("Version", "unknown"),
            )
            logger.debug(
                "Docker server version: %s",
                version_info.get("Server", {}).get("Version", "unknown"),
            )
        except (subprocess.CalledProcessError, json.JSONDecodeError) as exc:
            logger.error("Docker is not available or not running: %s", exc)
            raise RuntimeError("Docker daemon is not available or not running") from exc

    # Template Deployment Methods
    def deploy_template(
        self,
        template_id: str,
        config: Dict[str, Any],
        template_data: Dict[str, Any],
        backend_config: Dict[str, Any],
        pull_image: bool = True,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Deploy a template using Docker CLI.

        Args:
            template_id: Unique identifier for the template
            config: Configuration parameters for the deployment
            template_data: Template metadata including image, ports, commands, etc.
            backend_config: Any banckend specific configuration
            pull_image: Whether to pull the container image before deployment
            dry_run: Whether to performm actual depolyment. False means yes, True means No

        Returns:
            Dict containing deployment information

        Raises:
            Exception: If deployment fails for any reason
        """

        if backend_config:
            raise ValueError("Docker backend configuration is not supported")

        # Prepare deployment configuration
        env_vars = self._prepare_environment_variables(config, template_data)

        # Check if this is a stdio deployment
        is_stdio = self._identify_stdio_deployment(env_vars)

        # Also check the template's default transport
        template_transport = template_data.get("transport", {})
        default_transport = template_transport.get("default", "http")
        # If stdio transport is detected, prevent deployment
        if is_stdio is True or (is_stdio is None and default_transport == "stdio"):
            # Import here to avoid circular import
            from mcp_platform.core.tool_manager import ToolManager

            tool_manager = ToolManager(backend_type=self.backend_name)
            tool_names = []
            if not dry_run:
                # Get available tools for this template
                try:
                    discovery_result = tool_manager.list_tools(
                        template_id,
                    )
                    tools = discovery_result.get("tools", [])
                    tool_names = [tool.get("name", "unknown") for tool in tools]
                except Exception as e:
                    logger.warning(
                        "Failed to discover tools for %s: %s", template_id, e
                    )

            # Create error message with available tools
            console.line()
            console.print(
                Panel(
                    f"❌ [red]Cannot deploy stdio transport MCP servers[/red]\n\n"
                    f"The template [cyan]{template_id}[/cyan] uses stdio transport, which doesn't require deployment.\n"
                    f"Stdio MCP servers run interactively and cannot be deployed as persistent containers.\n\n"
                    f"[yellow]Available tools in this template:[/yellow]\n"
                    + (
                        f"{chr(10).join(f'  • {tool}' for tool in tool_names)}"
                        if tool_names
                        else "  • No tools discovered"
                    )
                    + "\n\n"
                    f"[green]To use this template, run tools directly:[/green]\n"
                    f"  mcpp interactive\n"
                    f"  mcpp> tools {template_id}                    # List available tools\n"
                    f"  mcpp> call {template_id} <tool_name>     # Run a specific tool\n"
                    f"  echo '{json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': 'tools/list'})}' | \\\n"
                    f"    {self.backend_name} run -i --rm {template_data.get('image', template_data.get('docker_image', f'mcp-{template_id}:latest'))}",
                    title="Stdio Transport Detected",
                    border_style="yellow",
                )
            )

            raise ValueError(
                f"Cannot deploy stdio transport template '{template_id}'. "
                "Stdio templates run interactively and don't support persistent deployment."
            )

        container_name = self._generate_container_name(template_id)

        try:
            volumes = self._prepare_volume_mounts(template_data)
            ports = self._prepare_port_mappings(template_data)
            command_args = template_data.get("command", [])
            image_name = template_data.get("image", f"mcp-{template_id}:latest")
            # Pull image if requested
            if pull_image and not dry_run:
                self._pull_image(image_name)

            # Deploy the container
            container_id = self._deploy_container(
                container_name,
                template_id,
                image_name,
                env_vars,
                volumes,
                ports,
                command_args,
                is_stdio=is_stdio,
                dry_run=dry_run,
            )

            # Wait for container to stabilize
            time.sleep(2)

            return {
                "deployment_name": container_name,
                "container_id": container_id,
                "template_id": template_id,
                "configuration": config,
                "status": "deployed",
                "created_at": datetime.now().isoformat(),
                "image": image_name,
            }

        except Exception as e:
            # Cleanup on failure
            self._cleanup_failed_deployment(container_name)
            raise e

    def _generate_container_name(self, template_id: str) -> str:
        """Generate a unique container name for the template."""
        timestamp = datetime.now().strftime("%m%d-%H%M%S")
        return f"mcp-{template_id}-{timestamp}-{str(uuid.uuid4())[:8]}"

    def _prepare_environment_variables(
        self, config: Dict[str, Any], template_data: Dict[str, Any]
    ) -> List[str]:
        """Prepare environment variables for container deployment."""
        env_vars = []
        env_dict = {}  # Use dict to prevent duplicates

        # First, add defaults from config schema
        config_schema = template_data.get("config_schema", {})
        properties = config_schema.get("properties", {})

        for prop_name, prop_config in properties.items():
            env_mapping = prop_config.get("env_mapping", prop_name.upper())
            default_value = prop_config.get("default")

            if default_value is not None:
                env_dict[env_mapping] = str(default_value)

        # Process user configuration (override defaults)
        for key, value in config.items():
            if isinstance(value, bool):
                env_value = "true" if value else "false"
            elif isinstance(value, list):
                env_value = ",".join(str(item) for item in value)
            else:
                env_value = str(value)

            # Check if this key maps to an env variable through config schema
            env_key = key
            for prop_name, prop_config in properties.items():
                if prop_name == key:
                    env_key = prop_config.get("env_mapping", key.upper())
                    break

            env_dict[env_key] = env_value

        # Add template default env vars (only if not already present)
        template_env = template_data.get("env_vars", {})
        for key, value in template_env.items():
            if key not in env_dict:  # Don't override user config or schema defaults
                env_dict[key] = str(value)

        # Add transport configuration for HTTP deployment
        transport_config = template_data.get("transport", {})
        if isinstance(transport_config, dict):
            default_transport = transport_config.get("default", "http")
            transport_port = transport_config.get("port", 8080)
        else:
            # Legacy format handling
            default_transport = "http"
            transport_port = template_data.get("port", 8080)

        # Set transport environment variables for HTTP deployment
        if default_transport == "http":
            env_dict["MCP_TRANSPORT"] = "http"
            env_dict["MCP_PORT"] = str(transport_port)

        # Convert dict to docker --env format
        for key, value in env_dict.items():
            # Properly quote values that contain spaces or special characters
            if (
                " " in value
                or '"' in value
                or "'" in value
                or "&" in value
                or "|" in value
            ):
                # Escape double quotes and wrap in double quotes
                escaped_value = value.replace('"', '\\"')
                env_vars.extend(["--env", f'{key}="{escaped_value}"'])
            else:
                env_vars.extend(["--env", f"{key}={value}"])

        return env_vars

    def _prepare_volume_mounts(self, template_data: Dict[str, Any]) -> List[str]:
        """Prepare volume mounts for container deployment."""
        volumes = []
        template_volumes = template_data.get("volumes", {})
        for host_path, container_path in template_volumes.items():
            expanded_path = os.path.expanduser(self.resolve_host_path(host_path))
            try:
                os.makedirs(expanded_path, exist_ok=True)
            except FileExistsError:
                pass
            volumes.extend(["--volume", f"{expanded_path}:{container_path}"])
        return volumes

    def _prepare_port_mappings(self, template_data: Dict[str, Any]) -> List[str]:
        """Prepare port mappings for container deployment, using a free port if needed."""
        ports = []
        template_ports = template_data.get("ports", {})
        for host_port, container_port in template_ports.items():
            port_to_use = int(host_port)
            # Check if port is available
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                    s.bind(("", port_to_use))
                    s.listen(1)
                except OSError:
                    # Port is in use, find a free port
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as free_sock:
                        free_sock.bind(("", 0))
                        port_to_use = free_sock.getsockname()[1]
                    logger.warning(
                        "Port %s is in use, remapping to free port %s for container port %s",
                        host_port,
                        port_to_use,
                        container_port,
                    )
            ports.extend(["-p", f"{port_to_use}:{container_port}"])
        return ports

    @staticmethod
    def _identify_stdio_deployment(
        env_vars: List[str],
    ) -> bool:
        """Identify if the deployment is using stdio transport."""

        is_stdio = None
        for env_var in env_vars:
            if len(env_var.split("=")) == 2:
                key, value = env_var.split("=", 1)
                if key == "MCP_TRANSPORT":
                    if value == "stdio":
                        is_stdio = True
                    else:
                        is_stdio = False
                    break

        return is_stdio

    def _build_docker_command(
        self,
        container_name: str,
        template_id: str,
        image_name: str,
        env_vars: List[str],
        volumes: List[str],
        ports: List[str],
        command_args: List[str],
        is_stdio: bool = False,
        detached: bool = True,
    ) -> List[str]:
        """Build the Docker command with all configuration."""
        docker_command = [
            self.backend_name,
            "run",
        ]

        if detached:
            docker_command.append("--detach")

        docker_command.extend(
            [
                "--name",
                container_name,
            ]
        )

        if not is_stdio:
            docker_command.extend(["--restart", "unless-stopped"])

        docker_command.extend(
            [
                "--label",
                f"template={template_id}",
                "--label",
                "managed-by=mcp-template",
            ]
        )

        # Network setup
        self.create_network()
        docker_command.extend(
            [
                "--network",
                MCP_PLATFORM_NETWORK_NAME,
            ]
        )

        docker_command.extend(ports)
        docker_command.extend(env_vars)
        docker_command.extend(volumes)
        docker_command.append(image_name)
        docker_command.extend(command_args)

        return docker_command

    def _deploy_container(
        self,
        container_name: str,
        template_id: str,
        image_name: str,
        env_vars: List[str],
        volumes: List[str],
        ports: List[str],
        command_args: List[str],
        is_stdio: bool = False,
        dry_run: bool = False,
    ) -> str:
        """Deploy the Docker container with all configuration."""
        # Build the Docker command
        docker_command = self._build_docker_command(
            container_name,
            template_id,
            image_name,
            env_vars,
            volumes,
            ports,
            command_args,
            is_stdio,
            detached=True,
        )

        console.line()
        console.print(
            Panel(
                f"Running command: {' '.join(docker_command)}",
                title="Docker Command Execution",
                style="magenta",
            )
        )
        # Run the container
        if not dry_run:
            result = self._run_command(docker_command)
        else:
            result = SubProcessRunDummyResult(
                args=["Dry", "Run", "Dummy", "Response"],
                returncode=0,
                stdout="dummycontainer",
            )
        container_id = result.stdout.strip()

        logger.info("Started container %s with ID %s", container_name, container_id)
        return container_id

    def _check_image_exists(self, image_name: str) -> bool:
        """Check if a Docker image exists."""

        try:
            self._run_command([self.backend_name, "image", "inspect", image_name])
            return True
        except Exception:
            return False

    def _pull_image(self, image_name: str, force: bool = False) -> None:
        """Pull a Docker image if it does not exist."""
        if not self._check_image_exists(image_name) or force:
            self._run_command([self.backend_name, "pull", image_name])

    def run_stdio_command(
        self,
        template_id: str,
        config: Dict[str, Any],
        template_data: Dict[str, Any],
        json_input: str,
        pull_image: bool = True,
    ) -> Dict[str, Any]:
        """Run a stdio MCP command directly and return the result."""
        try:
            # Prepare deployment configuration
            env_vars = self._prepare_environment_variables(config, template_data)

            # CRITICAL: Ensure MCP_TRANSPORT=stdio is set for stdio execution
            # Convert env_vars from list format to dict to ensure we can override
            env_dict = {}
            for i in range(0, len(env_vars), 2):
                if i + 1 < len(env_vars) and env_vars[i] == "--env":
                    key_value = env_vars[i + 1]
                    if "=" in key_value:
                        key, value = key_value.split("=", 1)
                        env_dict[key] = value

            # Override with stdio transport
            env_dict["MCP_TRANSPORT"] = "stdio"

            # Convert back to docker --env format
            env_vars = []
            for key, value in env_dict.items():
                # Properly quote values that contain spaces or special characters
                if (
                    " " in value
                    or '"' in value
                    or "'" in value
                    or "&" in value
                    or "|" in value
                ):
                    # Escape double quotes and wrap in double quotes
                    escaped_value = value.replace('"', '\\"')
                    env_vars.extend(["--env", f'{key}="{escaped_value}"'])
                else:
                    env_vars.extend(["--env", f"{key}={value}"])

            volumes = self._prepare_volume_mounts(template_data)
            command_args = template_data.get("command", [])
            image_name = template_data.get("image", f"mcp-{template_id}:latest")

            # Pull image if requested
            if pull_image:
                self._pull_image(image_name)

            # Ensure network exists before running stdio command
            self.create_network()

            logger.info("Running stdio command for template %s", template_id)

            # Parse the original JSON input to extract the tool call
            try:
                tool_request = json.loads(json_input)
                tool_method = tool_request.get("method")
                tool_params = tool_request.get("params", {})
            except json.JSONDecodeError:
                return {
                    "template_id": template_id,
                    "status": "failed",
                    "error": "Invalid JSON input",
                    "executed_at": datetime.now().isoformat(),
                }

            # Create the proper MCP initialization sequence
            mcp_commands = [
                # 1. Initialize the connection
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "initialize",
                        "params": {
                            "protocolVersion": "2024-11-05",
                            "capabilities": {},
                            "clientInfo": {"name": "mcp-template", "version": "1.0.0"},
                        },
                    }
                ),
                # 2. Send initialized notification
                json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}),
                # 3. Send the actual tool call or request
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": 3,
                        "method": tool_method,
                        "params": tool_params,
                    }
                ),
            ]

            # Join commands with newlines for proper MCP communication
            full_input = "\n".join(mcp_commands)

            logger.debug("Full MCP input: %s", full_input)

            # Execute the command with MCP input sequence using bash heredoc
            # Include network parameter to ensure stdio containers run in the correct network
            bash_command = [
                "/bin/bash",
                "-c",
                f"""{self.backend_name} run -i --rm --network {MCP_PLATFORM_NETWORK_NAME} {" ".join(env_vars)} {" ".join(volumes)} {" ".join(["--label", f"template={template_id}"])} {image_name} {" ".join(command_args)} << 'EOF'
{full_input}
EOF""",
            ]

            result = subprocess.run(
                bash_command,
                capture_output=True,
                text=True,
                check=True,
                timeout=STDIO_TIMEOUT,
            )

            return {
                "template_id": template_id,
                "status": "completed",
                "stdout": result.stdout,
                "stderr": result.stderr,
                "executed_at": datetime.now().isoformat(),
            }

        except subprocess.CalledProcessError as e:
            logger.error("Stdio command failed for template %s: %s", template_id, e)
            return {
                "template_id": template_id,
                "status": "failed",
                "stdout": e.stdout or "",
                "stderr": e.stderr or "",
                "error": str(e),
                "executed_at": datetime.now().isoformat(),
            }
        except subprocess.TimeoutExpired:
            logger.error(
                "Stdio command timed out for template %s after %d seconds",
                template_id,
                STDIO_TIMEOUT,
            )
            return {
                "template_id": template_id,
                "status": "timeout",
                "error": f"Command execution timed out after {STDIO_TIMEOUT} seconds",
                "executed_at": datetime.now().isoformat(),
            }
        except Exception as e:
            logger.error("Unexpected error running stdio command: %s", e)
            return {
                "template_id": template_id,
                "status": "error",
                "error": str(e),
                "executed_at": datetime.now().isoformat(),
            }

    def _cleanup_failed_deployment(self, container_name: str):
        """Clean up a failed deployment by removing the container."""
        try:
            self._run_command(
                [self.backend_name, "rm", "-f", container_name], check=False
            )
        except Exception:
            pass  # Ignore cleanup failures

    def _get_host_port(
        self,
        ports: Union[Dict, List, str],
    ):
        """
        Resolve ports list to extract hot port
        """

        host_port = None
        if isinstance(ports, list):
            # Podman format: Ports is a list of port objects
            if ports:
                try:
                    port_obj = ports[0]
                    if isinstance(port_obj, dict):
                        host_port = port_obj.get(
                            "host_port", port_obj.get("HostPort", "")
                        )
                        host_port = str(host_port) if host_port else ""
                except (IndexError, KeyError):
                    host_port = ""

        elif isinstance(ports, dict):
            # Example - {'7090/tcp': [{'HostIp': '0.0.0.0', 'HostPort': '7090'}, {'HostIp': '::', 'HostPort': '7090'}]}
            for _, val in ports.items():
                host_port = self._get_host_port(val)
                if host_port:
                    break

        elif isinstance(ports, str) and ports:
            # Docker format: Ports is a string
            try:
                port_parts = ports.split(", ")[-1].split(":")[-1].split("/")
                host_port = port_parts[0] if port_parts else ""
            except (IndexError, AttributeError):
                host_port = str(ports)

        return host_port

    def _prepare_deployment_result(
        self,
        container: Dict,
        include_logs: bool = False,
        lines: int = 100,
    ):
        """
        Prepare consistent deployment result from container result
        """

        try:
            labels = container.get(
                "Labels", container.get("Config", {}).get("Labels", "")
            )
            template_name = "unknown"

            # Handle different label formats
            if isinstance(labels, dict):
                # Podman format: Labels is a dictionary
                template_name = labels.get("template", template_name)
            elif isinstance(labels, str) and labels:
                # Docker format: Labels is a comma-separated string
                if "template=" in labels:
                    for label in labels.split(","):
                        if label.strip().startswith("template="):
                            template_name = label.split("=", 1)[1]
                            break

            # Handle port parsing safely - Podman has different port format
            ports_str = container.get(
                "Ports", container.get("NetworkSettings", {}).get("Ports", "")
            )
            ports_display = self._get_host_port(ports_str) or ""

            # Handle Names field - can be array or string
            names = container.get("Names", container.get("Name", "unknown")).lstrip("/")
            if isinstance(names, list) and names:
                name = names[0]
            else:
                name = str(names)

            # Compose endpoint (Docker: always localhost + port if available)
            if ports_display:
                splitters = ["->", "-", ":", "/"]
                for splitter in splitters:
                    parts = ports_display.split(splitter)
                    if len(parts) > 1:
                        host_port = parts[0].strip()
                        break
                else:
                    host_port = ports_display.strip()
            else:
                host_port = "unknown"

            endpoint = f"http://localhost:{host_port}"
            # Transport: if port is present, assume http, else stdio
            transport = "http" if ports_display else "stdio"
            state = container.get("State", "unknown")
            raw_status = "unknown"

            # Extract raw status from docker/podman output
            if isinstance(state, dict):
                raw_status = state.get("Status", "unknown")
            elif isinstance(state, str):
                # Docker 'ps' output sometimes sets State as a string like 'running'
                raw_status = container.get("Status", None) or state

            # Determine running boolean robustly for both docker and podman formats
            if isinstance(state, dict):
                is_running = bool(state.get("Running", False))
            else:
                # For docker ps string formats, treat 'running' or 'up' as running
                rs = (str(raw_status) or "").lower()
                is_running = rs == "running" or rs.startswith("up")

            # Normalize status to 'running' when running, otherwise keep raw_status
            status = "running" if is_running else raw_status
            deployment = {
                "id": container.get("ID", container.get("Id", "unknown")),
                "name": name,
                "template": template_name,
                "state": state,
                "status": status,
                "running": is_running,
                "created": container.get(
                    "CreatedAt", container.get("Created", "unknown")
                ),
                "since": (
                    container.get("RunningFor", state.get("StartedAt", "unknown"))
                    if isinstance(state, dict)
                    else "unknown"
                ),
                "image": container.get(
                    "Image", container.get("Config", {}).get("Image", "unknown")
                ),
                "ports": ports_display,
                "endpoint": endpoint,
                "transport": transport,
                "health": container.get("Health", {}).get("Status", "unknown"),
            }
            if include_logs:
                try:
                    log_result = self._run_command(
                        [
                            self.backend_name,
                            "logs",
                            "--tail",
                            str(int(lines)),
                            container.get("ID", "unknown"),
                        ],
                        check=False,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,  # Because docker logs are sent to stderr by default
                    )
                    deployment["logs"] = log_result.stdout
                except Exception:
                    deployment["logs"] = "Unable to fetch logs"
        except:
            deployment = None

        return deployment

    # Container Management Methods
    def list_deployments(self, template: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all MCP deployments managed by this Docker service.

        Returns:
            List of deployment information dictionaries
        """
        try:
            cmd = [
                self.backend_name,
                "ps",
                "-a",
                "--filter",
                "label=managed-by=mcp-template",
                "--format",
                "json",
            ]
            if template:
                template.extend(["--filter", f"label=template={template}"])

            # Get containers with the managed-by label
            result = self._run_command(cmd)

            deployments = []
            if result.stdout.strip():
                # Handle both Docker (newline-separated JSON objects) and Podman (JSON array) formats
                stdout = result.stdout.strip()
                containers = []

                if stdout.startswith("["):
                    # Podman format: JSON array
                    try:
                        containers = json.loads(stdout)
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse JSON array: {e}")
                        return []
                else:
                    # Docker format: newline-separated JSON objects
                    for line in stdout.split("\n"):
                        if line.strip():
                            try:
                                containers.append(json.loads(line))
                            except json.JSONDecodeError as e:
                                logger.debug(
                                    f"Failed to parse container JSON line: {line}, error: {e}"
                                )
                                continue

                # Process each container
                for container in containers:
                    deployment_info = self._prepare_deployment_result(container)
                    if deployment_info:
                        deployments.append(deployment_info)

            return deployments

        except subprocess.CalledProcessError as e:
            logger.error("Failed to list deployments: %s", e)
            return []

    def get_deployment_info(
        self, deployment_name: str, include_logs: bool = False, lines: int = 10
    ) -> Dict[str, Any]:
        """Get detailed information about a specific deployment.

        Args:
            deployment_name: Name or ID of the deployment
            include_logs: Whether to include container logs in the response
            lines: Number of log lines to retrieve (only if include_logs=True)

        Returns:
            Dictionary with deployment information, or None if not found
        """
        try:
            # Get detailed container information
            result = self._run_command(
                [
                    self.backend_name,
                    "inspect",
                    deployment_name,
                ]
            )

            if result.stdout.strip():
                containers = json.loads(result.stdout)
                if containers:
                    container = containers[0]
                    return self._prepare_deployment_result(
                        container=container,
                        include_logs=include_logs,
                        lines=lines,
                    )

            return None

        except (subprocess.CalledProcessError, json.JSONDecodeError, KeyError) as e:
            logger.debug(f"Failed to get deployment info for {deployment_name}: {e}")
            return None

    def get_deployment_logs(
        self,
        deployment_name: str,
        lines: int = 100,
        follow: bool = False,
        since: Optional[str] = None,
        until: Optional[str] = None,
    ) -> dict:
        """Get logs from a deployment.

        Args:
            deployment_name: Name or ID of the deployment
            lines: Number of log lines to retrieve
            follow: Whether to stream logs (not implemented for this method)
            since: Start time for log filtering
            until: End time for log filtering

        Returns:
            Dictionary with success status and logs or error message
        """
        try:
            cmd = [self.backend_name, "logs"]

            if lines:
                cmd.extend(["--tail", str(lines)])
            if since:
                cmd.extend(["--since", since])
            if until:
                cmd.extend(["--until", until])

            cmd.append(deployment_name)

            result = self._run_command(cmd)

            if result.returncode == 0:
                return {
                    "success": True,
                    "logs": result.stderr + "\n" + result.stdout or "",
                }
            else:
                return {
                    "success": False,
                    "error": result.stderr or "Unknown error retrieving logs",
                }

        except Exception as e:
            logger.error(f"Failed to get logs for deployment {deployment_name}: {e}")
            return {"success": False, "error": str(e)}

    def delete_deployment(
        self, deployment_name: str, raise_on_failure: bool = False
    ) -> bool:
        """Delete a deployment by stopping and removing the container.

        Args:
            deployment_name: Name of the deployment to delete

        Returns:
            True if deletion was successful, False otherwise
        """
        try:
            # Stop and remove the container
            self._run_command(
                [self.backend_name, "stop", deployment_name], check=raise_on_failure
            )
            self._run_command(
                [self.backend_name, "rm", deployment_name], check=raise_on_failure
            )
            logger.info("Deleted deployment %s", deployment_name)
            return True
        except subprocess.CalledProcessError as e:
            logger.error("Failed to delete deployment %s: %s", deployment_name, e)
            return False

    def stop_deployment(self, deployment_name: str, force: bool = False) -> bool:
        """Stop a deployment.

        Args:
            deployment_name: Name of the deployment to stop
            force: Whether to force stop the deployment

        Returns:
            True if stop was successful, False otherwise
        """
        try:
            if force:
                self._run_command([self.backend_name, "kill", deployment_name])
            else:
                self._run_command([self.backend_name, "stop", deployment_name])
            return True
        except subprocess.CalledProcessError:
            return False

    def _build_internal_image(
        self, template_id: str, image_name: str, template_data: Dict[str, Any]
    ) -> None:
        """Build Docker image for internal templates."""

        discovery = TemplateDiscovery()
        template_dir = discovery.templates_dir / template_id

        if not template_dir.exists() or not (template_dir / "Dockerfile").exists():
            logger.error(
                "Dockerfile not found for internal template %s in %s",
                template_id,
                template_dir,
            )
            raise ValueError(f"Internal template {template_id} missing Dockerfile")

        logger.info(
            "Building image %s for internal template %s", image_name, template_id
        )

        # Build the Docker image
        build_command = [
            self.backend_name,
            "build",
            "-t",
            image_name,
            str(template_dir),
        ]
        self._run_command(build_command)

    def connect_to_deployment(self, deployment_id: str):
        """
        Connect to deployment shell with improved shell detection.
        Args:
            deployment_id: Name or ID of the deployment

        Returns:
            None - Gives access to deployment shell
        """

        # Check if container is running
        container_info = self.get_deployment_info(deployment_id)
        if not container_info or container_info.get("status") != "running":
            raise RuntimeError(f"Container {deployment_id} is not running")

        # Try to detect available shells in order of preference
        shells_to_try = [
            "bash",  # Most feature-rich
            "sh",  # Basic POSIX shell
            "zsh",  # Modern alternative
            "ash",  # Alpine Linux default
            "dash",  # Debian/Ubuntu minimal
        ]

        logger.info(f"Attempting to connect to container {deployment_id}")

        # First, try to detect which shells are available
        available_shells = []
        for shell in shells_to_try:
            try:
                # Check if shell exists in container
                check_cmd = [self.backend_name, "exec", deployment_id, "which", shell]
                result = subprocess.run(
                    check_cmd, capture_output=True, text=True, timeout=5
                )
                if result.returncode == 0:
                    available_shells.append(shell)
                    logger.debug(f"Found shell: {shell}")
            except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
                continue

        if not available_shells:
            # Fallback: try shells without checking availability
            available_shells = ["sh", "bash"]
            logger.warning("Could not detect available shells, trying fallback options")

        # Try to connect using available shells
        for shell in available_shells:
            try:
                cmd = [self.backend_name, "exec", "-it", deployment_id, shell]
                logger.info(f"Connecting with {shell}...")

                # Use os.execvp to replace current process for proper terminal handling
                os.execvp(self.backend_name, cmd)

                # Note: execvp only returns if it fails, so this line should never be reached
                # in normal operation. However, in testing scenarios where execvp is mocked,
                # we return here to indicate success.
                return

            except Exception as e:
                logger.debug(f"Failed to connect with {shell}: {e}")
                continue

        # If we get here, all shells failed
        raise RuntimeError(
            f"Could not connect to container {deployment_id}. No working shell found."
        )

    def cleanup_stopped_containers(
        self, template_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Clean up stopped containers.

        Args:
            template_name: If provided, only clean containers for this template

        Returns:
            Dict with cleanup results
        """
        try:
            # Find containers to clean up
            if template_name:
                # Get all containers for this template
                cmd = [
                    self.backend_name,
                    "ps",
                    "-a",
                    "--filter",
                    f"label=mcp.template={template_name}",
                    "--filter",
                    "status=exited",
                    "--format",
                    "{{.ID}}\t{{.Names}}\t{{.Status}}",
                ]
            else:
                # Get all stopped MCP containers
                cmd = [
                    self.backend_name,
                    "ps",
                    "-a",
                    "--filter",
                    "label=mcp.template",
                    "--filter",
                    "status=exited",
                    "--format",
                    "{{.ID}}\t{{.Names}}\t{{.Status}}",
                ]

            result = subprocess.run(cmd, capture_output=True, text=True, check=True)

            if not result.stdout.strip():
                return {
                    "success": True,
                    "cleaned_containers": [],
                    "message": "No stopped containers to clean up",
                }

            # Parse container information
            containers_to_clean = []
            for line in result.stdout.strip().split("\n"):
                if line:
                    parts = line.split("\t")
                    if len(parts) >= 3:
                        containers_to_clean.append(
                            {"id": parts[0], "name": parts[1], "status": parts[2]}
                        )

            # Remove the containers
            cleaned_containers = []
            failed_cleanups = []

            for container in containers_to_clean:
                try:
                    subprocess.run(
                        [self.backend_name, "rm", container["id"]],
                        check=True,
                        capture_output=True,
                    )
                    cleaned_containers.append(container)
                    logger.info(
                        f"Cleaned up container: {container['name']} ({container['id'][:12]})"
                    )
                except subprocess.CalledProcessError as e:
                    failed_cleanups.append({"container": container, "error": str(e)})
                    logger.warning(
                        f"Failed to clean up container {container['name']}: {e}"
                    )

            return {
                "success": len(failed_cleanups) == 0,
                "cleaned_containers": cleaned_containers,
                "failed_cleanups": failed_cleanups,
                "message": f"Cleaned up {len(cleaned_containers)} containers",
            }

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to list containers for cleanup: {e}")
            return {
                "success": False,
                "error": f"Failed to list containers: {e}",
                "cleaned_containers": [],
                "failed_cleanups": [],
            }

    def cleanup_dangling_images(self) -> Dict[str, Any]:
        """
        Clean up dangling Docker images related to MCP templates.

        Returns:
            Dict with cleanup results
        """
        try:
            # Find dangling images
            cmd = [self.backend_name, "images", "--filter", "dangling=true", "-q"]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)

            if not result.stdout.strip():
                return {
                    "success": True,
                    "cleaned_images": [],
                    "message": "No dangling images to clean up",
                }

            image_ids = result.stdout.strip().split("\n")

            # Remove dangling images
            try:
                subprocess.run(
                    [self.backend_name, "rmi"] + image_ids,
                    check=True,
                    capture_output=True,
                )

                return {
                    "success": True,
                    "cleaned_images": image_ids,
                    "message": f"Cleaned up {len(image_ids)} dangling images",
                }

            except subprocess.CalledProcessError as e:
                return {
                    "success": False,
                    "error": f"Failed to remove dangling images: {e}",
                    "cleaned_images": [],
                }

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to list dangling images: {e}")
            return {
                "success": False,
                "error": f"Failed to list dangling images: {e}",
                "cleaned_images": [],
            }
