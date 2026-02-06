.PHONY: help release-minor release-patch release-major test clean install docs

# Python executable (use venv if available)
PYTHON := $(shell if [ -f .venv/bin/python ]; then echo .venv/bin/python; else echo python3; fi)
PYTEST := $(shell if [ -f .venv/bin/pytest ]; then echo .venv/bin/pytest; else echo pytest; fi)

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
NC := \033[0m # No Color

help: ## Show this help message
	@echo '$(BLUE)Available targets:$(NC)'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'

# Get the latest tag, or default to v0.0.0 if no tags exist
get-version:
	@git fetch --tags 2>/dev/null || true
	@git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0"

# Extract current version components
current-version: ## Show current version
	$(eval CURRENT_TAG := $(shell git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0"))
	$(eval VERSION := $(shell echo $(CURRENT_TAG) | sed 's/^v//'))
	@echo "Current version: $(YELLOW)$(CURRENT_TAG)$(NC)"

# Bump minor version (e.g., v0.4.4 -> v0.5.0)
release-minor: ## Create a new minor release (v0.4.4 -> v0.5.0)
	@echo "$(BLUE)Creating minor release...$(NC)"
	$(eval CURRENT_TAG := $(shell git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0"))
	$(eval VERSION := $(shell echo $(CURRENT_TAG) | sed 's/^v//'))
	$(eval MAJOR := $(shell echo $(VERSION) | cut -d. -f1))
	$(eval MINOR := $(shell echo $(VERSION) | cut -d. -f2))
	$(eval NEW_MINOR := $(shell echo $$(($(MINOR) + 1))))
	$(eval NEW_VERSION := $(MAJOR).$(NEW_MINOR).0)
	$(eval NEW_TAG := v$(NEW_VERSION))
	@echo "Bumping from $(YELLOW)$(CURRENT_TAG)$(NC) to $(GREEN)$(NEW_TAG)$(NC)"
	@echo ""
	@echo "$(BLUE)Step 1:$(NC) Updating pyproject.toml..."
	@sed -i '' 's/^version = ".*"/version = "$(NEW_VERSION)"/' pyproject.toml
	@echo "$(GREEN)✓$(NC) Updated pyproject.toml to version $(NEW_VERSION)"
	@echo ""
	@echo "$(BLUE)Step 2:$(NC) Running tests..."
	@$(PYTEST) tests/ -q || (echo "$(YELLOW)⚠$(NC) Tests failed! Fix them before releasing." && exit 1)
	@echo "$(GREEN)✓$(NC) Tests passed"
	@echo ""
	@echo "$(BLUE)Step 3:$(NC) Committing changes..."
	@git add pyproject.toml
	@git commit -m "Bump version to $(NEW_VERSION)" || echo "Nothing to commit"
	@echo ""
	@echo "$(BLUE)Step 4:$(NC) Creating tag $(NEW_TAG)..."
	@git tag -a $(NEW_TAG) -m "Release $(NEW_TAG)"
	@echo "$(GREEN)✓$(NC) Created tag $(NEW_TAG)"
	@echo ""
	@echo "$(BLUE)Step 5:$(NC) Pushing to remote..."
	@git push origin main
	@git push origin $(NEW_TAG)
	@echo ""
	@echo "$(GREEN)✓ Release complete!$(NC)"
	@echo "  Tag $(GREEN)$(NEW_TAG)$(NC) has been pushed."
	@echo "  GitHub Actions will now build and publish to PyPI."
	@echo "  Monitor at: https://github.com/$$(git remote get-url origin | sed 's/.*github.com[:/]\(.*\)\.git/\1/')/actions"

# Bump patch version (e.g., v0.4.4 -> v0.4.5)
release-patch: ## Create a new patch release (v0.4.4 -> v0.4.5)
	@echo "$(BLUE)Creating patch release...$(NC)"
	$(eval CURRENT_TAG := $(shell git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0"))
	$(eval VERSION := $(shell echo $(CURRENT_TAG) | sed 's/^v//'))
	$(eval MAJOR := $(shell echo $(VERSION) | cut -d. -f1))
	$(eval MINOR := $(shell echo $(VERSION) | cut -d. -f2))
	$(eval PATCH := $(shell echo $(VERSION) | cut -d. -f3))
	$(eval NEW_PATCH := $(shell echo $$(($(PATCH) + 1))))
	$(eval NEW_VERSION := $(MAJOR).$(MINOR).$(NEW_PATCH))
	$(eval NEW_TAG := v$(NEW_VERSION))
	@echo "Bumping from $(YELLOW)$(CURRENT_TAG)$(NC) to $(GREEN)$(NEW_TAG)$(NC)"
	@echo ""
	@echo "$(BLUE)Step 1:$(NC) Updating pyproject.toml..."
	@sed -i '' 's/^version = ".*"/version = "$(NEW_VERSION)"/' pyproject.toml
	@echo "$(GREEN)✓$(NC) Updated pyproject.toml to version $(NEW_VERSION)"
	@echo ""
	@echo "$(BLUE)Step 2:$(NC) Running tests..."
	@$(PYTEST) tests/ -q || (echo "$(YELLOW)⚠$(NC) Tests failed! Fix them before releasing." && exit 1)
	@echo "$(GREEN)✓$(NC) Tests passed"
	@echo ""
	@echo "$(BLUE)Step 3:$(NC) Committing changes..."
	@git add pyproject.toml
	@git commit -m "Bump version to $(NEW_VERSION)" || echo "Nothing to commit"
	@echo ""
	@echo "$(BLUE)Step 4:$(NC) Creating tag $(NEW_TAG)..."
	@git tag -a $(NEW_TAG) -m "Release $(NEW_TAG)"
	@echo "$(GREEN)✓$(NC) Created tag $(NEW_TAG)"
	@echo ""
	@echo "$(BLUE)Step 5:$(NC) Pushing to remote..."
	@git push origin main
	@git push origin $(NEW_TAG)
	@echo ""
	@echo "$(GREEN)✓ Release complete!$(NC)"
	@echo "  Tag $(GREEN)$(NEW_TAG)$(NC) has been pushed."
	@echo "  GitHub Actions will now build and publish to PyPI."

# Bump major version (e.g., v0.4.4 -> v1.0.0)
release-major: ## Create a new major release (v0.4.4 -> v1.0.0)
	@echo "$(BLUE)Creating major release...$(NC)"
	$(eval CURRENT_TAG := $(shell git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0"))
	$(eval VERSION := $(shell echo $(CURRENT_TAG) | sed 's/^v//'))
	$(eval MAJOR := $(shell echo $(VERSION) | cut -d. -f1))
	$(eval NEW_MAJOR := $(shell echo $$(($(MAJOR) + 1))))
	$(eval NEW_VERSION := $(NEW_MAJOR).0.0)
	$(eval NEW_TAG := v$(NEW_VERSION))
	@echo "$(YELLOW)⚠ WARNING:$(NC) This is a major version bump (breaking changes)"
	@echo "Bumping from $(YELLOW)$(CURRENT_TAG)$(NC) to $(GREEN)$(NEW_TAG)$(NC)"
	@echo ""
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ ! $$REPLY =~ ^[Yy]$$ ]]; then \
		echo "Aborted."; \
		exit 1; \
	fi
	@echo ""
	@echo "$(BLUE)Step 1:$(NC) Updating pyproject.toml..."
	@sed -i '' 's/^version = ".*"/version = "$(NEW_VERSION)"/' pyproject.toml
	@echo "$(GREEN)✓$(NC) Updated pyproject.toml to version $(NEW_VERSION)"
	@echo ""
	@echo "$(BLUE)Step 2:$(NC) Running tests..."
	@$(PYTEST) tests/ -q || (echo "$(YELLOW)⚠$(NC) Tests failed! Fix them before releasing." && exit 1)
	@echo "$(GREEN)✓$(NC) Tests passed"
	@echo ""
	@echo "$(BLUE)Step 3:$(NC) Committing changes..."
	@git add pyproject.toml
	@git commit -m "Bump version to $(NEW_VERSION)" || echo "Nothing to commit"
	@echo ""
	@echo "$(BLUE)Step 4:$(NC) Creating tag $(NEW_TAG)..."
	@git tag -a $(NEW_TAG) -m "Release $(NEW_TAG)"
	@echo "$(GREEN)✓$(NC) Created tag $(NEW_TAG)"
	@echo ""
	@echo "$(BLUE)Step 5:$(NC) Pushing to remote..."
	@git push origin main
	@git push origin $(NEW_TAG)
	@echo ""
	@echo "$(GREEN)✓ Release complete!$(NC)"
	@echo "  Tag $(GREEN)$(NEW_TAG)$(NC) has been pushed."
	@echo "  GitHub Actions will now build and publish to PyPI."

test: ## Run tests
	@$(PYTEST) tests/ -v

test-cov: ## Run tests with coverage
	@$(PYTEST) tests/ -v --cov=scripthut --cov-report=term-missing

test-fast: ## Run tests quickly (no output)
	@$(PYTEST) tests/ -q


lint: ## Run linting
	@ruff check src/scripthut/ tests/

format: ## Format code
	@ruff format src/scripthut/ tests/

clean: ## Clean build artifacts
	@rm -rf build/ dist/ *.egg-info
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete

install: ## Install package in development mode
	@pip install -e ".[dev]"

# Pre-release checks (can be run manually before releasing)
check: test ## Run all checks
	@echo "$(GREEN)✓ All checks passed!$(NC)"
