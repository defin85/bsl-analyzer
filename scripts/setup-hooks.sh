#!/usr/bin/env bash

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GIT_HOOKS_DIR="$PROJECT_ROOT/.git/hooks"

echo -e "${YELLOW}Setting up git hooks...${NC}\n"

if [ ! -d "$PROJECT_ROOT/.git" ]; then
    echo "Error: Not a git repository!"
    exit 1
fi

mkdir -p "$GIT_HOOKS_DIR"

echo -e "${YELLOW}Installing pre-commit hook...${NC}"
cp "$SCRIPT_DIR/pre-commit" "$GIT_HOOKS_DIR/pre-commit"
chmod +x "$GIT_HOOKS_DIR/pre-commit"
echo -e "${GREEN}✓ Pre-commit hook installed${NC}\n"

echo -e "${GREEN}✓ Git hooks installed successfully!${NC}\n"
echo "Pre-commit hook will now run before each commit, over the STAGED tree:"
echo "  - cargo fmt --all -- --check (checks; run cargo fmt yourself to fix)"
echo "  - cargo clippy --all-targets --all-features -- -D warnings"
echo "  - cargo test --all (run all tests)"
echo ""
echo "Unstaged changes are set aside for the duration and restored afterwards,"
echo "so a partial commit is checked as what it will actually be."
echo ""
echo "To skip hooks temporarily, use: git commit --no-verify"
echo ""
