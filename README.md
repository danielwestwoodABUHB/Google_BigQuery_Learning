# Google_BigQuery_Learning

This repository contains test codes and examples for working with Google BigQuery sandbox, along with a comprehensive repository governance system for managing GitHub repositories, teams, and user access.

## Contents

### BigQuery Learning Materials
- Test codes and examples for Google BigQuery
- COVID-19 data analysis with Prophet forecasting
- Dimension table creation scripts
- Data analytics and visualization examples

**Google Cloud Console**: https://console.cloud.google.com/

### Repository Governance System

This repository now includes a comprehensive governance system for managing repository creation, team management, and user access with proper approval workflows and audit trails.

#### Key Features
- **Repository Creation Governance**: Structured approval process for new repositories
- **Team Management**: Controlled team creation and user access management  
- **Audit Logging**: Complete audit trail for all governance actions
- **Approval Workflows**: ABB - GitHub Owners approval for critical actions
- **Compliance Reporting**: Generate reports for security and compliance reviews

#### Quick Start with Governance
1. **Request a new repository**: Use the [Repository Request](.github/ISSUE_TEMPLATE/repository-request.md) template
2. **Request team changes**: Use the [Team Request](.github/ISSUE_TEMPLATE/team-request.md) template
3. **View audit logs**: Check `governance/audit-logs/` for historical records
4. **CLI tool**: Use `python governance/cli.py --help` for command-line operations

#### Documentation
- [Complete Governance Guide](governance/README.md)
- [Configuration Reference](governance/config/repository-governance-config.yml)
- [Example Requests](governance/examples/)

For detailed information about the governance system, see the [governance documentation](governance/README.md).
