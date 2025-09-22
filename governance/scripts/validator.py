#!/usr/bin/env python3
"""
Repository Governance Validator

This script validates repository governance requests against organizational policies
and ensures compliance with naming conventions, security requirements, and approval workflows.
"""

import re
import yaml
import json
from typing import Dict, List, Any, Tuple
from pathlib import Path

class RepositoryGovernanceValidator:
    """Validates repository governance requests against organizational policies."""
    
    def __init__(self, config_path: str = "governance/config/repository-governance-config.yml"):
        """Initialize the validator with configuration.
        
        Args:
            config_path: Path to governance configuration file
        """
        self.config_path = config_path
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load governance configuration from YAML file.
        
        Returns:
            Configuration dictionary
        """
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Governance configuration not found at {self.config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in governance configuration: {e}")
    
    def validate_repository_request(self, request_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate a repository creation request.
        
        Args:
            request_data: Repository request details
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Validate repository name
        name_errors = self._validate_repository_name(request_data.get("repository_name", ""))
        errors.extend(name_errors)
        
        # Validate mandatory fields
        mandatory_errors = self._validate_mandatory_fields(request_data)
        errors.extend(mandatory_errors)
        
        # Validate privacy level
        privacy_errors = self._validate_privacy_level(request_data.get("privacy_level", ""))
        errors.extend(privacy_errors)
        
        # Validate data classification
        classification_errors = self._validate_data_classification(request_data.get("data_classification", ""))
        errors.extend(classification_errors)
        
        # Validate team assignments
        team_errors = self._validate_team_assignments(request_data.get("team_assignments", []))
        errors.extend(team_errors)
        
        return len(errors) == 0, errors
    
    def validate_team_request(self, request_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate a team management request.
        
        Args:
            request_data: Team request details
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Validate team name
        team_name = request_data.get("team_name", "")
        if not team_name:
            errors.append("Team name is required")
        elif not re.match(r'^[a-z0-9\-_]+$', team_name):
            errors.append("Team name must contain only lowercase letters, numbers, hyphens, and underscores")
        
        # Validate request type
        request_type = request_data.get("request_type", "")
        valid_types = ["create", "modify", "add_users", "remove_users", "change_permissions"]
        if request_type not in valid_types:
            errors.append(f"Invalid request type. Must be one of: {', '.join(valid_types)}")
        
        # Validate permission level
        permission_level = request_data.get("permission_level", "")
        valid_permissions = self.config.get("governance", {}).get("team_requirements", {}).get("roles", [])
        if permission_level and permission_level not in valid_permissions:
            errors.append(f"Invalid permission level. Must be one of: {', '.join(valid_permissions)}")
        
        # Validate users format
        users_to_add = request_data.get("users_to_add", [])
        users_to_remove = request_data.get("users_to_remove", [])
        
        for user in users_to_add + users_to_remove:
            if not isinstance(user, str) or not user.startswith("@"):
                errors.append(f"Invalid user format: {user}. Users must be GitHub usernames starting with @")
        
        # Validate business justification
        if not request_data.get("business_justification"):
            errors.append("Business justification is required for team requests")
        
        return len(errors) == 0, errors
    
    def _validate_repository_name(self, name: str) -> List[str]:
        """Validate repository name against naming conventions.
        
        Args:
            name: Repository name to validate
            
        Returns:
            List of validation errors
        """
        errors = []
        
        if not name:
            errors.append("Repository name is required")
            return errors
        
        # Check length
        max_length = self.config.get("governance", {}).get("repository_requirements", {}).get("naming_convention", {}).get("max_length", 50)
        if len(name) > max_length:
            errors.append(f"Repository name exceeds maximum length of {max_length} characters")
        
        # Check prefix requirements
        naming_config = self.config.get("governance", {}).get("repository_requirements", {}).get("naming_convention", {})
        if naming_config.get("prefix_required", False):
            allowed_prefixes = naming_config.get("allowed_prefixes", [])
            if not any(name.startswith(prefix) for prefix in allowed_prefixes):
                errors.append(f"Repository name must start with one of: {', '.join(allowed_prefixes)}")
        
        # Check valid characters
        if not re.match(r'^[a-z0-9\-_]+$', name):
            errors.append("Repository name must contain only lowercase letters, numbers, hyphens, and underscores")
        
        return errors
    
    def _validate_mandatory_fields(self, request_data: Dict[str, Any]) -> List[str]:
        """Validate that all mandatory fields are present.
        
        Args:
            request_data: Request data to validate
            
        Returns:
            List of validation errors
        """
        errors = []
        
        mandatory_fields = self.config.get("governance", {}).get("repository_requirements", {}).get("mandatory_fields", [])
        
        for field in mandatory_fields:
            if not request_data.get(field):
                errors.append(f"Mandatory field '{field}' is missing or empty")
        
        return errors
    
    def _validate_privacy_level(self, privacy_level: str) -> List[str]:
        """Validate privacy level.
        
        Args:
            privacy_level: Privacy level to validate
            
        Returns:
            List of validation errors
        """
        errors = []
        
        valid_levels = self.config.get("governance", {}).get("repository_requirements", {}).get("privacy_levels", [])
        if privacy_level not in valid_levels:
            errors.append(f"Invalid privacy level. Must be one of: {', '.join(valid_levels)}")
        
        return errors
    
    def _validate_data_classification(self, data_classification: str) -> List[str]:
        """Validate data classification.
        
        Args:
            data_classification: Data classification to validate
            
        Returns:
            List of validation errors
        """
        errors = []
        
        valid_classifications = self.config.get("governance", {}).get("repository_requirements", {}).get("data_classifications", [])
        if data_classification not in valid_classifications:
            errors.append(f"Invalid data classification. Must be one of: {', '.join(valid_classifications)}")
        
        return errors
    
    def _validate_team_assignments(self, team_assignments: List[Dict[str, Any]]) -> List[str]:
        """Validate team assignments.
        
        Args:
            team_assignments: List of team assignments to validate
            
        Returns:
            List of validation errors
        """
        errors = []
        
        if not team_assignments:
            errors.append("At least one team assignment is required")
            return errors
        
        valid_roles = self.config.get("governance", {}).get("team_requirements", {}).get("roles", [])
        mandatory_teams = self.config.get("governance", {}).get("team_requirements", {}).get("mandatory_teams", [])
        
        assigned_teams = []
        
        for assignment in team_assignments:
            if not isinstance(assignment, dict):
                errors.append("Team assignments must be objects with 'team' and 'permission' fields")
                continue
            
            team = assignment.get("team", "")
            permission = assignment.get("permission", "")
            
            if not team:
                errors.append("Team name is required in team assignment")
            else:
                assigned_teams.append(team)
            
            if not permission:
                errors.append("Permission level is required in team assignment")
            elif permission not in valid_roles:
                errors.append(f"Invalid permission level '{permission}'. Must be one of: {', '.join(valid_roles)}")
        
        # Check mandatory teams are assigned
        for mandatory_team in mandatory_teams:
            if mandatory_team not in assigned_teams:
                errors.append(f"Mandatory team '{mandatory_team}' must be assigned")
        
        return errors
    
    def requires_approval(self, request_type: str) -> bool:
        """Check if a request type requires approval.
        
        Args:
            request_type: Type of request (repository_creation, team_creation, etc.)
            
        Returns:
            True if approval is required
        """
        approval_groups = self.config.get("governance", {}).get("approval_groups", [])
        
        for group in approval_groups:
            if request_type in group.get("required_for", []):
                return True
        
        return False
    
    def get_approvers(self, request_type: str) -> List[str]:
        """Get list of required approvers for a request type.
        
        Args:
            request_type: Type of request
            
        Returns:
            List of approver group names
        """
        approvers = []
        approval_groups = self.config.get("governance", {}).get("approval_groups", [])
        
        for group in approval_groups:
            if request_type in group.get("required_for", []):
                approvers.append(group.get("name", ""))
        
        return approvers


def main():
    """Example usage of the validator."""
    validator = RepositoryGovernanceValidator()
    
    # Example repository request validation
    repo_request = {
        "repository_name": "data-analytics-project",
        "description": "Analytics project for business intelligence",
        "privacy_level": "private",
        "data_classification": "internal",
        "business_justification": "Required for quarterly business analysis",
        "team_assignments": [
            {"team": "maintainers", "permission": "admin"},
            {"team": "contributors", "permission": "push"}
        ]
    }
    
    is_valid, errors = validator.validate_repository_request(repo_request)
    
    if is_valid:
        print("✓ Repository request is valid")
        
        if validator.requires_approval("repository_creation"):
            approvers = validator.get_approvers("repository_creation")
            print(f"Approval required from: {', '.join(approvers)}")
    else:
        print("✗ Repository request validation failed:")
        for error in errors:
            print(f"  - {error}")


if __name__ == "__main__":
    main()