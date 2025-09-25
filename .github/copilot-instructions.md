# GitHub Copilot Instructions for MCP Platform

## Overview

This repository contains the MCP (Model Context Protocol) Platform - a comprehensive framework for creating, deploying, and managing MCP servers. When contributing to this codebase, follow these guidelines to maintain code quality, consistency, and robust testing practices.

## Development Principles

### 1. Understanding Before Development
- **Always** read and understand existing code before making changes
- Study the module's purpose, dependencies, and integration points
- Review related tests to understand expected behavior
- Check documentation in `docs/` directory for context
- Examine similar implementations in the codebase for patterns

### 2. Test-Driven Development
- **Always** build comprehensive unit and integration tests following the current test directory structure
- Write tests BEFORE implementing new features when possible
- Ensure both happy path and error cases are covered
- Maintain high test coverage (target: 80%+)

### 3. Code Quality Standards
- Write **clean, scalable, readable, bug-free, and well-formatted code**
- Follow existing code patterns and architectural decisions
- **Reuse code wherever possible** - check for existing utilities and helpers
- **Only create new files when necessary** - extend existing modules when appropriate
- **Focus on cleanup** - remove unused code, files, or lines during development

## Test Structure and Guidelines

### Test Organization
```
tests/
├── test_unit/           # Fast unit tests (no external dependencies)
├── test_integration/    # Integration tests (may require Docker/external services)
├── conftest.py         # Test fixtures and utilities
├── runner.py           # Comprehensive test runner
└── mcp_test_utils.py   # Test helper utilities
```

### Test Markers
Use appropriate pytest markers to categorize tests:
- `@pytest.mark.unit` - Unit tests (fast, isolated)
- `@pytest.mark.integration` - Integration tests (slower, external dependencies)
- `@pytest.mark.docker` - Tests requiring Docker
- `@pytest.mark.kubernetes` - Tests requiring Kubernetes
- `@pytest.mark.slow` - Long-running tests
- `@pytest.mark.template` - Template-specific tests
- `@pytest.mark.e2e` - End-to-end tests

### Running Tests
```bash
# Quick unit tests
python tests/runner.py --unit

# Integration tests
python tests/runner.py --integration

# All tests
python tests/runner.py --all

# Specific template tests
make test-template TEMPLATE=template-name

# Using Makefile shortcuts
make test-unit
make test-integration
make test-all
```

### Test Requirements
- **Unit tests**: Test individual functions/classes in isolation
- **Integration tests**: Test component interactions and workflows
- **Template tests**: Test MCP server templates end-to-end
- **Coverage**: Maintain 80%+ code coverage
- **Documentation**: Include docstrings explaining test purpose

## Code Quality and Formatting

### Formatting Tools
```bash
# Format code (required before commits)
make format
# Runs: black mcp_platform/ tests/ && isort mcp_platform/ tests/

# Lint code
make lint
# Runs: flake8 mcp_platform/ tests/ && bandit -r mcp_platform/

# Type checking
make type-check
# Runs: mypy mcp_platform/
```

### Code Style Guidelines
- **Use Black for code formatting** (configured in pyproject.toml)
- **Follow PEP 8 guidelines** (enforced by flake8)
- **Add type hints** where possible (checked by mypy)
- **Write clear docstrings** for all public functions and classes
- **Use descriptive variable and function names**
- **Keep functions focused and small** (single responsibility)

### Import Organization
```python
# Standard library imports
import os
import sys
from pathlib import Path

# Third-party imports
import pytest
import aiohttp
from rich.console import Console

# Local imports
from mcp_platform.core.tool_manager import ToolManager
from mcp_platform.template.utils import TemplateCreator
```

## MCP Platform Specific Guidelines

### Template Development
- **Follow template structure**: Each template must include `template.json`, tests, documentation
- **Use template utilities**: Leverage `mcp_platform.template.utils` for common operations
- **Test template configurations**: Validate `template.json` schema and configuration
- **Document capabilities**: Include clear examples in template documentation

### Backend Integration
- **Support multiple backends**: Docker, Kubernetes, local execution
- **Use backend abstraction**: Leverage `mcp_platform.core.backends` for consistency
- **Handle backend-specific logic**: Isolate backend differences in appropriate modules
- **Test backend switching**: Ensure functionality works across supported backends

### Configuration Management
- **Use schema validation**: Validate all configuration inputs
- **Support environment variables**: Allow environment-based configuration
- **Provide sensible defaults**: Ensure components work with minimal configuration
- **Document configuration**: Include clear examples and explanations

## Development Workflow

### 1. Before Starting
```bash
# Set up development environment
make install
make install-dev

# Verify setup
python tests/runner.py --unit
make lint
```

### 2. During Development
```bash
# Run relevant tests frequently
python tests/runner.py --unit  # For logic changes
python tests/runner.py --integration  # For component changes
make test-template TEMPLATE=your-template  # For template changes

# Check code quality
make format  # Format code
make lint    # Check linting
make type-check  # Check types
```

### 3. Before Committing
```bash
# Run full validation
make format
make lint
make type-check
python tests/runner.py --all

# Clean up unused imports/code
# Review changes for unnecessary additions
```

## File Organization and Patterns

### Creating New Modules
- **Check existing modules first** - extend rather than duplicate
- **Follow package structure**: `mcp_platform/core/`, `mcp_platform/template/`, etc.
- **Include `__init__.py`** with appropriate exports
- **Add corresponding tests** in matching test directory structure

### Extending Existing Modules
- **Maintain backward compatibility** when possible
- **Follow existing patterns** and coding style
- **Update related tests** and documentation
- **Consider impact on dependent modules**

### Removing Code
- **Check for usage** across the entire codebase before removing
- **Remove related tests** and documentation
- **Update imports** in dependent modules
- **Consider deprecation warnings** for public APIs

## Documentation Standards

### Code Documentation
- **Module docstrings**: Explain module purpose and main functionality
- **Class docstrings**: Describe class purpose, main methods, and usage
- **Function docstrings**: Include Args, Returns, Raises sections
- **Inline comments**: Explain complex logic or business rules

### Example Docstring Format
```python
def create_mcp_server(template_name: str, config: Dict[str, Any]) -> MCPServer:
    """Create and configure an MCP server from a template.

    Args:
        template_name: Name of the template to use
        config: Configuration dictionary for the server

    Returns:
        Configured MCPServer instance

    Raises:
        TemplateNotFoundError: If template doesn't exist
        ConfigurationError: If config is invalid

    Example:
        >>> server = create_mcp_server("file-server", {"base_path": "/data"})
        >>> server.start()
    """
```

## Error Handling and Logging

### Error Handling
- **Use specific exception types** rather than generic Exception
- **Provide helpful error messages** with context
- **Handle expected errors gracefully** with appropriate fallbacks
- **Log errors with sufficient detail** for debugging

### Logging Guidelines
- **Use structured logging** with appropriate levels
- **Include relevant context** in log messages
- **Avoid logging sensitive information** (credentials, personal data)
- **Use logger instances** rather than print statements

## Performance and Resource Management

### Performance Considerations
- **Use async/await** for I/O operations when appropriate
- **Implement connection pooling** for external services
- **Cache expensive operations** when safe to do so
- **Profile code for bottlenecks** in critical paths

### Resource Management
- **Use context managers** for resource cleanup
- **Close connections explicitly** when not using context managers
- **Handle timeouts appropriately** for external calls
- **Monitor memory usage** in long-running operations

## Security Best Practices

### Input Validation
- **Validate all external inputs** (user input, file contents, API responses)
- **Sanitize file paths** to prevent directory traversal
- **Use parameterized queries** for database operations
- **Validate configuration schemas** strictly

### Secrets Management
- **Never commit secrets** to the repository
- **Use environment variables** for sensitive configuration
- **Provide clear documentation** for required secrets
- **Use secure defaults** for security-related settings

## Common Patterns and Utilities

### Reusable Components
- **Configuration processing**: Use `mcp_platform.config` utilities
- **Template operations**: Leverage `mcp_platform.template.utils`
- **Backend abstraction**: Use `mcp_platform.core.backends`
- **Test utilities**: Use helpers from `tests/mcp_test_utils.py`

### Anti-Patterns to Avoid
- **Duplicating existing functionality** without checking for existing implementations
- **Hardcoding values** that should be configurable
- **Ignoring error conditions** or using bare except clauses
- **Creating deep inheritance hierarchies** - prefer composition
- **Mixing business logic with I/O operations** - separate concerns

## Troubleshooting Development Issues

### Common Issues
1. **Tests failing**: Check test markers and dependencies
2. **Import errors**: Verify Python path and package structure
3. **Linting failures**: Run `make format` and fix remaining issues
4. **Type errors**: Add appropriate type hints and handle Optional types
5. **Docker issues**: Ensure Docker is running for integration tests

### Getting Help
- **Check existing issues** in the repository
- **Review similar code** for patterns and examples
- **Run tests in verbose mode** for detailed error information
- **Check logs** for detailed error context

## Summary Checklist

Before submitting code changes, ensure:

- [ ] **Understanding**: Code purpose and integration points are clear
- [ ] **Tests**: Comprehensive unit and integration tests are included
- [ ] **Quality**: Code is clean, readable, and follows project standards
- [ ] **Reuse**: Existing utilities and patterns are leveraged
- [ ] **Cleanup**: Unused code, imports, and files are removed
- [ ] **Formatting**: Code is properly formatted (`make format`)
- [ ] **Linting**: No linting errors (`make lint`)
- [ ] **Types**: Type checking passes (`make type-check`)
- [ ] **Coverage**: Test coverage is maintained or improved
- [ ] **Documentation**: Relevant documentation is updated

Following these guidelines ensures that your contributions maintain the high quality and consistency of the MCP Platform codebase.
