#!/usr/bin/env python3
"""
Repository Governance CLI Tool

Command-line interface for managing repository governance operations including:
- Validating repository requests
- Querying audit logs
- Generating compliance reports
- Managing governance configuration
"""

import argparse
import json
import sys
import os
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

# Add governance scripts to path
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(script_dir, 'scripts'))

try:
    from audit_logger import GovernanceAuditLogger
    from validator import RepositoryGovernanceValidator
except ImportError as e:
    print(f"Error importing governance modules: {e}")
    print("Please ensure you're running this from the repository root directory.")
    sys.exit(1)


class GovernanceCLI:
    """Command-line interface for repository governance operations."""
    
    def __init__(self):
        """Initialize the CLI tool."""
        self.logger = GovernanceAuditLogger()
        self.validator = RepositoryGovernanceValidator()
    
    def validate_repository_request(self, request_file: str) -> None:
        """Validate a repository request from a JSON file.
        
        Args:
            request_file: Path to JSON file containing request data
        """
        try:
            with open(request_file, 'r', encoding='utf-8') as f:
                request_data = json.load(f)
        except FileNotFoundError:
            print(f"Error: Request file '{request_file}' not found.")
            return
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON in request file: {e}")
            return
        
        is_valid, errors = self.validator.validate_repository_request(request_data)
        
        if is_valid:
            print("✅ Repository request validation successful")
            
            if self.validator.requires_approval("repository_creation"):
                approvers = self.validator.get_approvers("repository_creation")
                print(f"📋 Approval required from: {', '.join(approvers)}")
                
                # Log the request
                audit_id = self.logger.log_repository_request(request_data)
                print(f"📝 Audit ID: {audit_id}")
        else:
            print("❌ Repository request validation failed:")
            for error in errors:
                print(f"   • {error}")
    
    def validate_team_request(self, request_file: str) -> None:
        """Validate a team request from a JSON file.
        
        Args:
            request_file: Path to JSON file containing request data
        """
        try:
            with open(request_file, 'r', encoding='utf-8') as f:
                request_data = json.load(f)
        except FileNotFoundError:
            print(f"Error: Request file '{request_file}' not found.")
            return
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON in request file: {e}")
            return
        
        is_valid, errors = self.validator.validate_team_request(request_data)
        
        if is_valid:
            print("✅ Team request validation successful")
            
            if self.validator.requires_approval("team_creation"):
                approvers = self.validator.get_approvers("team_creation")
                print(f"📋 Approval required from: {', '.join(approvers)}")
                
                # Log the request
                audit_id = self.logger.log_team_request(request_data)
                print(f"📝 Audit ID: {audit_id}")
        else:
            print("❌ Team request validation failed:")
            for error in errors:
                print(f"   • {error}")
    
    def query_audit_logs(self, audit_id: Optional[str] = None,
                        event_type: Optional[str] = None,
                        start_date: Optional[str] = None,
                        end_date: Optional[str] = None,
                        limit: int = 50) -> None:
        """Query audit logs with optional filtering.
        
        Args:
            audit_id: Specific audit ID to search for
            event_type: Filter by event type
            start_date: Start date for filtering (YYYY-MM-DD)
            end_date: End date for filtering (YYYY-MM-DD)
            limit: Maximum number of results to return
        """
        entries = self.logger.get_audit_history(
            audit_id=audit_id,
            event_type=event_type,
            start_date=start_date,
            end_date=end_date
        )
        
        # Sort by timestamp (newest first) and limit results
        entries.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        entries = entries[:limit]
        
        if not entries:
            print("No audit log entries found matching the criteria.")
            return
        
        print(f"Found {len(entries)} audit log entries:")
        print("=" * 80)
        
        for entry in entries:
            timestamp = entry.get('timestamp', 'Unknown')
            event_type = entry.get('event_type', 'Unknown')
            audit_id = entry.get('audit_id', 'Unknown')
            
            print(f"🕒 {timestamp}")
            print(f"📋 Event: {event_type}")
            print(f"🆔 Audit ID: {audit_id}")
            
            if 'requester' in entry:
                print(f"👤 Requester: {entry['requester']}")
            if 'repository_name' in entry:
                print(f"📁 Repository: {entry['repository_name']}")
            if 'team_name' in entry:
                print(f"👥 Team: {entry['team_name']}")
            if 'status' in entry:
                print(f"📊 Status: {entry['status']}")
            
            print("-" * 40)
    
    def generate_compliance_report(self, days: int = 30) -> None:
        """Generate a compliance report for the specified number of days.
        
        Args:
            days: Number of days to include in the report
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        
        entries = self.logger.get_audit_history(
            start_date=start_date_str,
            end_date=end_date_str
        )
        
        # Analyze entries
        stats = {
            'total_events': len(entries),
            'repository_requests': 0,
            'repository_approvals': 0,
            'repository_creations': 0,
            'team_requests': 0,
            'team_approvals': 0,
            'user_access_changes': 0,
            'pending_approvals': 0,
            'rejected_requests': 0
        }
        
        recent_events = []
        
        for entry in entries:
            event_type = entry.get('event_type', '')
            
            if event_type == 'repository_request':
                stats['repository_requests'] += 1
                if entry.get('status') == 'pending_approval':
                    stats['pending_approvals'] += 1
            elif event_type == 'repository_approval':
                stats['repository_approvals'] += 1
                if entry.get('approval_status') == 'rejected':
                    stats['rejected_requests'] += 1
            elif event_type == 'repository_created':
                stats['repository_creations'] += 1
            elif event_type == 'team_request':
                stats['team_requests'] += 1
            elif event_type == 'team_approval':
                stats['team_approvals'] += 1
            elif event_type == 'user_access_change':
                stats['user_access_changes'] += 1
            
            # Collect recent events
            if len(recent_events) < 10:
                recent_events.append(entry)
        
        # Generate report
        print(f"📊 Repository Governance Compliance Report")
        print(f"📅 Period: {start_date_str} to {end_date_str} ({days} days)")
        print("=" * 60)
        
        print(f"📈 Summary Statistics:")
        print(f"   Total governance events: {stats['total_events']}")
        print(f"   Repository requests: {stats['repository_requests']}")
        print(f"   Repository approvals: {stats['repository_approvals']}")
        print(f"   Repository creations: {stats['repository_creations']}")
        print(f"   Team requests: {stats['team_requests']}")
        print(f"   Team approvals: {stats['team_approvals']}")
        print(f"   User access changes: {stats['user_access_changes']}")
        print(f"   Pending approvals: {stats['pending_approvals']}")
        print(f"   Rejected requests: {stats['rejected_requests']}")
        
        if stats['pending_approvals'] > 0:
            print(f"\n⚠️  {stats['pending_approvals']} requests are pending approval")
        
        print(f"\n📋 Recent Activity:")
        for event in recent_events[:5]:
            timestamp = event.get('timestamp', 'Unknown').split('T')[0]
            event_type = event.get('event_type', 'Unknown')
            requester = event.get('requester', 'Unknown')
            print(f"   {timestamp} - {event_type} by {requester}")
    
    def show_config(self) -> None:
        """Display current governance configuration."""
        config = self.validator.config
        
        print("⚙️  Repository Governance Configuration")
        print("=" * 50)
        
        # Show approval groups
        approval_groups = config.get('governance', {}).get('approval_groups', [])
        print("👥 Approval Groups:")
        for group in approval_groups:
            name = group.get('name', 'Unknown')
            required_for = group.get('required_for', [])
            print(f"   • {name}: {', '.join(required_for)}")
        
        # Show naming conventions
        naming = config.get('governance', {}).get('repository_requirements', {}).get('naming_convention', {})
        print(f"\n📝 Naming Convention:")
        print(f"   Prefix required: {naming.get('prefix_required', False)}")
        print(f"   Allowed prefixes: {', '.join(naming.get('allowed_prefixes', []))}")
        print(f"   Max length: {naming.get('max_length', 'Not set')}")
        
        # Show privacy levels
        privacy_levels = config.get('governance', {}).get('repository_requirements', {}).get('privacy_levels', [])
        print(f"\n🔒 Privacy Levels: {', '.join(privacy_levels)}")
        
        # Show data classifications
        classifications = config.get('governance', {}).get('repository_requirements', {}).get('data_classifications', [])
        print(f"📊 Data Classifications: {', '.join(classifications)}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Repository Governance CLI Tool",
        epilog="Examples:\n"
               "  %(prog)s validate-repo request.json\n"
               "  %(prog)s query-logs --event-type repository_request\n"
               "  %(prog)s compliance-report --days 7\n",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Validate repository request command
    validate_repo = subparsers.add_parser('validate-repo', help='Validate a repository request')
    validate_repo.add_argument('request_file', help='JSON file containing repository request data')
    
    # Validate team request command
    validate_team = subparsers.add_parser('validate-team', help='Validate a team request')
    validate_team.add_argument('request_file', help='JSON file containing team request data')
    
    # Query audit logs command
    query_logs = subparsers.add_parser('query-logs', help='Query audit logs')
    query_logs.add_argument('--audit-id', help='Specific audit ID to search for')
    query_logs.add_argument('--event-type', help='Filter by event type')
    query_logs.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    query_logs.add_argument('--end-date', help='End date (YYYY-MM-DD)')
    query_logs.add_argument('--limit', type=int, default=50, help='Maximum results (default: 50)')
    
    # Compliance report command
    compliance = subparsers.add_parser('compliance-report', help='Generate compliance report')
    compliance.add_argument('--days', type=int, default=30, help='Number of days to include (default: 30)')
    
    # Show configuration command
    subparsers.add_parser('show-config', help='Show current governance configuration')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    cli = GovernanceCLI()
    
    try:
        if args.command == 'validate-repo':
            cli.validate_repository_request(args.request_file)
        elif args.command == 'validate-team':
            cli.validate_team_request(args.request_file)
        elif args.command == 'query-logs':
            cli.query_audit_logs(
                audit_id=args.audit_id,
                event_type=args.event_type,
                start_date=args.start_date,
                end_date=args.end_date,
                limit=args.limit
            )
        elif args.command == 'compliance-report':
            cli.generate_compliance_report(args.days)
        elif args.command == 'show-config':
            cli.show_config()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()