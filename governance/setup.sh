#!/bin/bash
# Repository Governance Setup Script
# This script sets up the repository governance system and verifies installation

set -e

echo "🚀 Setting up Repository Governance System"
echo "==========================================="

# Check if we're in the correct directory
if [ ! -f "governance/config/repository-governance-config.yml" ]; then
    echo "❌ Error: Please run this script from the repository root directory"
    exit 1
fi

echo "📦 Installing Python dependencies..."
pip3 install -r governance/requirements.txt --user

echo "🔧 Setting up directory structure..."
mkdir -p governance/audit-logs
mkdir -p governance/temp

echo "✅ Verifying configuration..."
python3 governance/cli.py show-config

echo ""
echo "🧪 Running validation tests..."

# Test repository request validation
echo "   Testing repository request validation..."
python3 governance/cli.py validate-repo governance/examples/repository-request-example.json > /dev/null
echo "   ✅ Repository validation working"

# Test team request validation  
echo "   Testing team request validation..."
python3 governance/cli.py validate-team governance/examples/team-request-example.json > /dev/null
echo "   ✅ Team validation working"

# Test audit log querying
echo "   Testing audit log querying..."
python3 governance/cli.py query-logs --limit 3 > /dev/null
echo "   ✅ Audit log queries working"

# Test compliance reporting
echo "   Testing compliance reporting..."
python3 governance/cli.py compliance-report --days 1 > /dev/null
echo "   ✅ Compliance reporting working"

echo ""
echo "🎉 Repository Governance System Setup Complete!"
echo ""
echo "📋 Next Steps:"
echo "   1. Configure your GitHub repository settings"
echo "   2. Set up the ABB - GitHub Owners team"
echo "   3. Configure GitHub Actions permissions"
echo "   4. Test the issue templates"
echo ""
echo "📚 Documentation: governance/README.md"
echo "🔧 CLI Tool: python3 governance/cli.py --help"
echo "📊 Example Usage:"
echo "   • Create repository request: Use GitHub issue template"
echo "   • Query audit logs: python3 governance/cli.py query-logs"
echo "   • Generate reports: python3 governance/cli.py compliance-report"
echo ""
echo "✅ Setup completed successfully!"