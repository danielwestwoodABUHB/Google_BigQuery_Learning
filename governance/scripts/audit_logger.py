#!/usr/bin/env python3
"""
GitHub Repository Governance Audit Logger

This script handles audit logging for repository governance actions including:
- Repository creation requests and approvals
- Team management requests and approvals  
- User access changes
- Policy violations and remediation actions
"""

import json
import datetime
import os
import hashlib
from typing import Dict, Any, Optional

class GovernanceAuditLogger:
    """Handles audit logging for repository governance actions."""
    
    def __init__(self, log_directory: str = "governance/audit-logs"):
        """Initialize the audit logger.
        
        Args:
            log_directory: Directory to store audit log files
        """
        self.log_directory = log_directory
        self.ensure_log_directory()
    
    def ensure_log_directory(self):
        """Ensure the audit log directory exists."""
        os.makedirs(self.log_directory, exist_ok=True)
    
    def generate_audit_id(self, event_type: str, timestamp: str) -> str:
        """Generate a unique audit ID for tracking.
        
        Args:
            event_type: Type of governance event
            timestamp: ISO format timestamp
            
        Returns:
            Unique audit ID
        """
        data = f"{event_type}_{timestamp}_{os.urandom(8).hex()}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]
    
    def log_repository_request(self, request_data: Dict[str, Any]) -> str:
        """Log a repository creation request.
        
        Args:
            request_data: Repository request details
            
        Returns:
            Audit ID for tracking
        """
        timestamp = datetime.datetime.utcnow().isoformat()
        audit_id = self.generate_audit_id("repo_request", timestamp)
        
        audit_entry = {
            "audit_id": audit_id,
            "event_type": "repository_request",
            "timestamp": timestamp,
            "requester": request_data.get("requester"),
            "repository_name": request_data.get("repository_name"),
            "description": request_data.get("description"),
            "privacy_level": request_data.get("privacy_level"),
            "data_classification": request_data.get("data_classification"),
            "business_justification": request_data.get("business_justification"),
            "team_assignments": request_data.get("team_assignments", []),
            "status": "pending_approval",
            "approval_required": True,
            "approvers_required": ["ABB - GitHub Owners"]
        }
        
        self._write_audit_log(audit_entry)
        return audit_id
    
    def log_repository_approval(self, audit_id: str, approval_data: Dict[str, Any]) -> None:
        """Log repository request approval.
        
        Args:
            audit_id: Original audit ID from request
            approval_data: Approval details
        """
        timestamp = datetime.datetime.utcnow().isoformat()
        
        audit_entry = {
            "audit_id": audit_id,
            "event_type": "repository_approval",
            "timestamp": timestamp,
            "approver": approval_data.get("approver"),
            "approval_status": approval_data.get("status"),  # approved/rejected
            "approval_comments": approval_data.get("comments"),
            "security_review_passed": approval_data.get("security_review", False),
            "compliance_review_passed": approval_data.get("compliance_review", False)
        }
        
        self._write_audit_log(audit_entry)
    
    def log_repository_creation(self, audit_id: str, creation_data: Dict[str, Any]) -> None:
        """Log successful repository creation.
        
        Args:
            audit_id: Original audit ID from request
            creation_data: Repository creation details
        """
        timestamp = datetime.datetime.utcnow().isoformat()
        
        audit_entry = {
            "audit_id": audit_id,
            "event_type": "repository_created",
            "timestamp": timestamp,
            "repository_url": creation_data.get("repository_url"),
            "teams_created": creation_data.get("teams_created", []),
            "initial_permissions": creation_data.get("initial_permissions", {}),
            "created_by": creation_data.get("created_by"),
            "status": "completed"
        }
        
        self._write_audit_log(audit_entry)
    
    def log_team_request(self, request_data: Dict[str, Any]) -> str:
        """Log a team management request.
        
        Args:
            request_data: Team request details
            
        Returns:
            Audit ID for tracking
        """
        timestamp = datetime.datetime.utcnow().isoformat()
        audit_id = self.generate_audit_id("team_request", timestamp)
        
        audit_entry = {
            "audit_id": audit_id,
            "event_type": "team_request",
            "timestamp": timestamp,
            "requester": request_data.get("requester"),
            "request_type": request_data.get("request_type"),  # create/modify/add_users/remove_users
            "team_name": request_data.get("team_name"),
            "team_description": request_data.get("team_description"),
            "repository_access": request_data.get("repository_access", []),
            "permission_level": request_data.get("permission_level"),
            "users_to_add": request_data.get("users_to_add", []),
            "users_to_remove": request_data.get("users_to_remove", []),
            "business_justification": request_data.get("business_justification"),
            "status": "pending_approval",
            "approval_required": True
        }
        
        self._write_audit_log(audit_entry)
        return audit_id
    
    def log_team_approval(self, audit_id: str, approval_data: Dict[str, Any]) -> None:
        """Log team request approval.
        
        Args:
            audit_id: Original audit ID from request
            approval_data: Approval details
        """
        timestamp = datetime.datetime.utcnow().isoformat()
        
        audit_entry = {
            "audit_id": audit_id,
            "event_type": "team_approval",
            "timestamp": timestamp,
            "approver": approval_data.get("approver"),
            "approval_status": approval_data.get("status"),
            "approval_comments": approval_data.get("comments"),
            "security_implications_reviewed": approval_data.get("security_reviewed", False)
        }
        
        self._write_audit_log(audit_entry)
    
    def log_user_access_change(self, change_data: Dict[str, Any]) -> str:
        """Log user access changes.
        
        Args:
            change_data: User access change details
            
        Returns:
            Audit ID for tracking
        """
        timestamp = datetime.datetime.utcnow().isoformat()
        audit_id = self.generate_audit_id("user_access", timestamp)
        
        audit_entry = {
            "audit_id": audit_id,
            "event_type": "user_access_change",
            "timestamp": timestamp,
            "changed_by": change_data.get("changed_by"),
            "target_user": change_data.get("target_user"),
            "repository": change_data.get("repository"),
            "team": change_data.get("team"),
            "action": change_data.get("action"),  # added/removed/permission_changed
            "previous_permission": change_data.get("previous_permission"),
            "new_permission": change_data.get("new_permission"),
            "reason": change_data.get("reason")
        }
        
        self._write_audit_log(audit_entry)
        return audit_id
    
    def _write_audit_log(self, audit_entry: Dict[str, Any]) -> None:
        """Write audit entry to log file.
        
        Args:
            audit_entry: Audit entry to write
        """
        date_str = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        log_filename = f"governance-audit-{date_str}.jsonl"
        log_filepath = os.path.join(self.log_directory, log_filename)
        
        with open(log_filepath, "a", encoding="utf-8") as f:
            f.write(json.dumps(audit_entry) + "\n")
    
    def get_audit_history(self, audit_id: Optional[str] = None, 
                         event_type: Optional[str] = None,
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None) -> list:
        """Retrieve audit history with optional filtering.
        
        Args:
            audit_id: Specific audit ID to retrieve
            event_type: Filter by event type
            start_date: Start date for filtering (YYYY-MM-DD)
            end_date: End date for filtering (YYYY-MM-DD)
            
        Returns:
            List of matching audit entries
        """
        matching_entries = []
        
        # Get all log files in date range
        log_files = []
        for filename in os.listdir(self.log_directory):
            if filename.startswith("governance-audit-") and filename.endswith(".jsonl"):
                log_files.append(filename)
        
        for log_file in sorted(log_files):
            log_filepath = os.path.join(self.log_directory, log_file)
            try:
                with open(log_filepath, "r", encoding="utf-8") as f:
                    for line in f:
                        try:
                            entry = json.loads(line.strip())
                            
                            # Apply filters
                            if audit_id and entry.get("audit_id") != audit_id:
                                continue
                            if event_type and entry.get("event_type") != event_type:
                                continue
                            if start_date and entry.get("timestamp", "").split("T")[0] < start_date:
                                continue
                            if end_date and entry.get("timestamp", "").split("T")[0] > end_date:
                                continue
                            
                            matching_entries.append(entry)
                            
                        except json.JSONDecodeError:
                            continue  # Skip malformed entries
            except FileNotFoundError:
                continue  # Skip missing files
        
        return matching_entries


if __name__ == "__main__":
    # Example usage
    logger = GovernanceAuditLogger()
    
    # Example repository request
    repo_request = {
        "requester": "john.doe@company.com",
        "repository_name": "data-analytics-project",
        "description": "Analytics project for business intelligence",
        "privacy_level": "private",
        "data_classification": "internal",
        "business_justification": "Required for quarterly business analysis",
        "team_assignments": [
            {"team": "data-team", "permission": "admin"},
            {"team": "analytics-team", "permission": "push"}
        ]
    }
    
    audit_id = logger.log_repository_request(repo_request)
    print(f"Repository request logged with audit ID: {audit_id}")