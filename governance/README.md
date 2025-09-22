# Repository Governance System

## Overview

This repository governance system provides a structured approach to managing GitHub repositories, teams, and user access within the organization. It ensures that all repository creation, team management, and user access changes follow proper approval workflows and maintain comprehensive audit trails.

## Key Features

### 1. Repository Creation Governance
- **Structured Request Process**: Standardized issue templates for repository requests
- **Naming Convention Enforcement**: Automatic validation of repository names with required prefixes
- **Privacy and Security Controls**: Classification-based privacy level requirements
- **Approval Workflows**: Mandatory approval from ABB - GitHub Owners group
- **Audit Logging**: Complete audit trail for all repository creation activities

### 2. Team Management
- **Team Creation Requests**: Structured process for creating and modifying teams
- **Permission Management**: Role-based access control with defined permission levels
- **User Access Control**: Managed addition and removal of team members
- **Business Justification**: Required justification for all team changes

### 3. Audit and Compliance
- **Comprehensive Logging**: All governance actions are logged with unique audit IDs
- **Long-term Retention**: 7-year retention policy for audit logs
- **Searchable History**: Query audit logs by date, user, action type, or audit ID
- **Compliance Reporting**: Generate reports for compliance and security reviews

## System Components

### Configuration Files
- `governance/config/repository-governance-config.yml` - Main governance configuration
- `.github/workflows/repository-governance.yml` - GitHub Actions workflow for automation

### Issue Templates
- `.github/ISSUE_TEMPLATE/repository-request.md` - Repository creation request template
- `.github/ISSUE_TEMPLATE/team-request.md` - Team management request template

### Scripts and Utilities
- `governance/scripts/audit_logger.py` - Audit logging functionality
- `governance/scripts/validator.py` - Request validation and policy enforcement

### Audit Logs
- `governance/audit-logs/` - Directory containing audit log files (JSONL format)

## Usage Guide

### Requesting a New Repository

1. **Create a New Issue** using the "Repository Creation Request" template
2. **Fill in all required fields**:
   - Repository name (must include approved prefix)
   - Description
   - Privacy level (public/internal/private)
   - Data classification (public/internal/confidential/restricted)
   - Business justification
   - Team assignments

3. **Submit the request** - it will automatically be assigned to ABB - GitHub Owners
4. **Wait for approval** - ABB - GitHub Owners will review and approve/reject
5. **Repository creation** - Upon approval, the repository will be automatically created

### Requesting Team Changes

1. **Create a New Issue** using the "Team Management Request" template
2. **Specify the request type**:
   - Create new team
   - Modify existing team
   - Add users to team
   - Remove users from team
   - Change team permissions

3. **Provide business justification** for the requested changes
4. **Submit for approval** - ABB - GitHub Owners will review the request

### Repository Naming Convention

All repositories must follow the established naming convention:

**Required Prefixes:**
- `data-` - Data-related repositories
- `analytics-` - Analytics and reporting repositories
- `ml-` - Machine learning and AI repositories
- `bigquery-` - BigQuery-specific repositories
- `sandbox-` - Sandbox and experimental repositories

**Naming Rules:**
- Maximum 50 characters
- Lowercase letters, numbers, hyphens, and underscores only
- Must start with an approved prefix

**Examples:**
- ✅ `data-customer-analytics`
- ✅ `bigquery-dashboard-reports`
- ✅ `ml-prediction-models`
- ❌ `CustomerAnalytics` (no prefix, capitalized)
- ❌ `data_analytics_very_long_repository_name_that_exceeds_limits` (too long)

### Privacy Levels and Data Classification

#### Privacy Levels
- **Public**: Visible to everyone on the internet
- **Internal**: Visible to all organization members
- **Private**: Visible only to repository collaborators

#### Data Classifications
- **Public**: Information that can be freely shared
- **Internal**: Information for internal use only
- **Confidential**: Sensitive information requiring special handling
- **Restricted**: Highly sensitive information with strict access controls

### Team Roles and Permissions

- **Admin**: Full administrative access to the repository
- **Maintain**: Maintain the repository without admin privileges
- **Push**: Read and write access to the repository
- **Triage**: Read access and ability to manage issues/PRs
- **Pull**: Read-only access to the repository

## Approval Process

### ABB - GitHub Owners Group

The ABB - GitHub Owners group is responsible for:
- Reviewing all repository creation requests
- Validating business justifications
- Ensuring compliance with security policies
- Approving or rejecting team management requests
- Maintaining governance policies and procedures

### Approval Criteria

Repository requests are evaluated based on:
1. **Business Justification**: Clear business need for the repository
2. **Security Requirements**: Appropriate data classification and privacy settings
3. **Compliance**: Adherence to organizational policies
4. **Resource Impact**: Consideration of organizational resources
5. **Team Assignments**: Appropriate team structure and permissions

## Audit and Monitoring

### Audit Log Structure

Each audit log entry contains:
- **Audit ID**: Unique identifier for tracking
- **Event Type**: Type of governance action
- **Timestamp**: ISO format timestamp
- **User Information**: Who performed the action
- **Action Details**: Specific details of the action
- **Status**: Current status of the request/action

### Audit Log Types

- `repository_request` - Repository creation request submitted
- `repository_approval` - Repository request approved/rejected
- `repository_created` - Repository successfully created
- `team_request` - Team management request submitted
- `team_approval` - Team request approved/rejected
- `user_access_change` - User access modified

### Querying Audit Logs

Use the audit logger script to query logs:

```python
from governance.scripts.audit_logger import GovernanceAuditLogger

logger = GovernanceAuditLogger()

# Get all entries for a specific audit ID
entries = logger.get_audit_history(audit_id="a1b2c3d4e5f6g7h8")

# Get all repository requests in date range
entries = logger.get_audit_history(
    event_type="repository_request",
    start_date="2024-01-01",
    end_date="2024-01-31"
)
```

## Security Considerations

### Access Control
- All governance actions require proper authentication
- ABB - GitHub Owners group has elevated privileges
- Regular access reviews should be conducted

### Data Protection
- Audit logs contain sensitive information and should be protected
- Access to audit logs should be restricted to authorized personnel
- Regular backups of audit logs should be maintained

### Compliance
- The system maintains detailed audit trails for compliance purposes
- All actions are logged with user attribution and timestamps
- Policies can be updated to meet changing compliance requirements

## Troubleshooting

### Common Issues

1. **Repository name validation fails**
   - Ensure the name includes an approved prefix
   - Check that the name meets length and character requirements

2. **Approval requests not routing properly**
   - Verify ABB - GitHub Owners team exists and has proper permissions
   - Check GitHub notifications settings

3. **Audit logs not being created**
   - Ensure the audit log directory exists and is writable
   - Verify the audit logger script has proper permissions

### Support

For issues with the governance system:
1. Check the audit logs for error details
2. Verify configuration files are properly formatted
3. Contact the ABB - GitHub Owners group for assistance
4. Submit a support request with relevant audit IDs

## Configuration Management

### Updating Governance Policies

To update governance policies:
1. Modify the configuration file: `governance/config/repository-governance-config.yml`
2. Test changes in a development environment
3. Submit changes for review by ABB - GitHub Owners
4. Deploy changes following change management procedures

### Adding New Prefixes or Classifications

To add new repository prefixes or data classifications:
1. Update the configuration file
2. Update issue templates if necessary
3. Update validation scripts
4. Document changes in this README
5. Communicate changes to all users

## Version History

- **v1.0.0** - Initial governance system implementation
  - Repository creation governance
  - Team management workflows
  - Audit logging system
  - Issue templates and automation

## Contributing

Contributions to the governance system should follow the same approval process as repository requests. Submit changes as pull requests with proper business justification and security review.