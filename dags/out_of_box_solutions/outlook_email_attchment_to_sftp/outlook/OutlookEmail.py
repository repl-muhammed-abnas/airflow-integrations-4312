"""
OutlookEmail.py - Refactored Outlook Email Operations Module

This module provides a clean, well-organized interface for Microsoft Graph API email operations.
Supports both personal mailboxes and shared mailbox access.

Key improvements:
- Organized methods into logical groups
- Consistent error handling patterns
- Better support for shared mailbox operations
- Improved documentation
- Type hints for better code clarity
"""
from out_of_box_solutions.outlook_email_attchment_to_sftp.outlook.OutlookConnection import OutlookConnection
import logging
from typing import List, Dict, Optional, Any

logging.basicConfig(level=logging.INFO)

class OutlookEmail:
    """
    Enhanced Outlook email operations class with support for personal and shared mailboxes.

    This class provides methods for:
    - Email retrieval and searching
    - Email status management (read/unread)
    - Attachment handling
    - Folder operations
    - Email drafting and sending
    - Email moving and deletion
    - Shared mailbox access
    """

    def __init__(self, outlook_connection_var_name: str, force_refresh: bool = False, user: str = 'me'):
        """
        Initialize the OutlookEmail instance.

        Args:
            outlook_connection_var_name: Name of the connection configuration
            force_refresh: Whether to force token refresh on initialization
            user: User identifier ('me' for current user or email/ID for shared mailbox)
        """
        self.outlook_connection = OutlookConnection(outlook_connection_var_name)
        self.session = self.outlook_connection.get_connection(force_refresh)
        self.user = user
        self.base_url = self._build_base_url()
        self.category = self.get_categoryId_by_name('marked read by integration')
        self.category_name = "marked read by integration"
        self.all_email_folders = []

    def _build_base_url(self) -> str:
        """
        Build the base URL for Graph API requests based on the user.

        Returns:
            Base URL string
        """
        if self.user == 'me':
            return "https://graph.microsoft.com/v1.0/me"
        else:
            return f"https://graph.microsoft.com/v1.0/users/{self.user}"

    @staticmethod
    def _create_response(status_code: int, status_message: str, response: Any = None) -> Dict[str, Any]:
        """
        Create a standardized response dictionary.

        Args:
            status_code: HTTP status code or custom status code
            status_message: Human-readable status message
            response: The actual response data (can be list, dict, string, etc.)

        Returns:
            Standardized response dictionary
        """
        return {
            "status_code": status_code,
            "status_message": status_message,
            "response": response
        }

    # ==========================================
    # EMAIL RETRIEVAL OPERATIONS
    # ==========================================

    def get_all_emails_for_query(self, query: str) -> Dict[str, Any]:
        """
        Fetch all emails matching a specific query.

        Args:
            query: OData query string
                example: 
                    - "from/emailAddress/address eq 'user@example.com' and isRead eq false and folder eq 'inbox'"

        Returns:
            Standardized response with list of email objects
        """
        url = f"{self.base_url}/messages?$filter={query}"
        response = self.session.get(url)

        if response.status_code == 200:
            messages = response.json().get('value', [])
            logging.info(f"Retrieved {len(messages)} emails for query '{query}' for user '{self.user}'")
            return self._create_response(
                status_code=200,
                status_message=f"Successfully retrieved {len(messages)} emails",
                response=messages
            )
        else:
            logging.error(f"Failed to fetch emails for query '{query}'. Status: {response.status_code}, Response: {response.text}")
            return self._create_response(
                status_code=response.status_code,
                status_message=f"Failed to fetch emails: {response.text}",
                response=[]
            )

    def get_unread_emails_from_folder(self, folder: str = "inbox") -> Dict[str, Any]:
        """
        Fetch unread emails from the specified folder.

        Args:
            folder: Folder name (default: "inbox")

        Returns:
            Standardized response with list of unread email objects
        """
        folder_id = self.get_folderId_by_name(folder)

        if not folder_id:
            return self._create_response(
                status_code=404,
                status_message=f"Folder '{folder}' not found",
                response=[]
            )

        url = f"{self.base_url}/mailFolders/{folder_id}/messages?$filter=isRead eq false"
        response = self.session.get(url)

        if response.status_code == 200:
            messages = response.json().get('value', [])
            logging.info(f"Retrieved {len(messages)} unread emails from folder '{folder}' for user '{self.user}'")
            return self._create_response(
                status_code=200,
                status_message=f"Successfully retrieved {len(messages)} unread emails",
                response=messages
            )
        else:
            logging.error(f"Failed to fetch unread emails from '{folder}'. Status: {response.status_code}, Response: {response.text}")
            return self._create_response(
                status_code=response.status_code,
                status_message=f"Failed to fetch unread emails: {response.text}",
                response=[]
            )

    def get_emails_from_folder(self, folder: str = "inbox", query:str = None) -> Dict[str, Any]:
        """
        Fetch all emails from a specific folder.

        Args:
            folder: Folder name (default: "inbox")
            query: additional filter to retrieve emails
        Returns:
            Standardized response with list of email objects
        """
        folder_id = self.get_folderId_by_name(folder)

        if not folder_id:
            return self._create_response(
                status_code=404,
                status_message=f"Folder '{folder}' not found",
                response=[]
            )

        url = f"{self.base_url}/mailFolders/{folder_id}/messages"
        if query:
            if query.startswith("?"):
                url += query
            else:
                url = f"{url}?{query}"
        logging.info(f"Calling {url}")
        response = self.session.get(url)

        if response.status_code == 200:
            messages = response.json().get('value', [])
            logging.info(f"Retrieved {len(messages)} emails from folder '{folder}' for user '{self.user}'")
            return self._create_response(
                status_code=200,
                status_message=f"Successfully retrieved {len(messages)} emails",
                response=messages
            )
        else:
            logging.error(f"Failed to fetch emails from '{folder}'. Status: {response.status_code}, Response: {response.text}")
            return self._create_response(
                status_code=response.status_code,
                status_message=f"Failed to fetch emails: {response.text}",
                response=[]
            )

    def get_email_details(self, email_id: str) -> Dict[str, Any]:
        """
        Fetch detailed information for a specific email.

        Args:
            email_id: The email message ID

        Returns:
            Standardized response with email details
        """
        url = f"{self.base_url}/messages/{email_id}"
        response = self.session.get(url)

        if response.status_code == 200:
            email_details = response.json()
            logging.info(f"Retrieved email details for ID: {email_id}")
            return self._create_response(
                status_code=200,
                status_message="Successfully retrieved email details",
                response=email_details
            )
        else:
            logging.error(f"Failed to fetch email details for ID: {email_id}. Status: {response.status_code}, Response: {response.text}")
            return self._create_response(
                status_code=response.status_code,
                status_message=f"Failed to fetch email details: {response.text}",
                response=None
            )

    def search_email_in_draft(self, search_param: str, search_param_value: str) -> Dict[str, Any]:
        """
        Search for an email in the Drafts folder.

        Args:
            search_param: Search parameter ('id' or 'subject')
            search_param_value: Value to search for

        Returns:
            Standardized response with draft email object if found

        Raises:
            TypeError: If search_param is not a string
            ValueError: If search_param is not 'id' or 'subject'
        """
        if not isinstance(search_param, str):
            error_msg = f"Expected search params to be of type `str`. Received type {type(search_param)}"
            logging.error(error_msg)
            return self._create_response(
                status_code=400,
                status_message=error_msg,
                response=None
            )

        if search_param not in ['id', 'subject']:
            error_msg = f"Expected search params 'id' or 'subject'. Received '{search_param}'"
            logging.error(error_msg)
            return self._create_response(
                status_code=400,
                status_message=error_msg,
                response=None
            )

        draft_result = self.get_emails_from_folder(folder="drafts")

        if draft_result['status_code'] != 200:
            return draft_result

        draft_emails = draft_result['response']

        for draft in draft_emails:
            if draft.get(search_param) == search_param_value:
                logging.info(f"Email found in draft with {search_param}={search_param_value}")
                return self._create_response(
                    status_code=200,
                    status_message="Draft email found",
                    response=draft
                )

        logging.warning(f"No email found in drafts with {search_param}={search_param_value}")
        return self._create_response(
            status_code=404,
            status_message=f"No draft email found with {search_param}={search_param_value}",
            response=None
        )

    # ==========================================
    # EMAIL STATUS OPERATIONS
    # ==========================================

    def mark_email_as_read(self, email_id: str) -> Dict[str, Any]:
        """
        Mark an email as read.

        Args:
            email_id: The email message ID

        Returns:
            Standardized response dictionary
        """
        url = f"{self.base_url}/messages/{email_id}"
        headers = {'Content-Type': 'application/json'}
        data = {"isRead": True}

        response = self.session.patch(url, headers=headers, json=data)

        if response.status_code == 200:
            logging.info(f"Email {email_id} marked as read")
            return self._create_response(
                status_code=200,
                status_message="Email marked as read successfully",
                response={"email_id": email_id}
            )
        else:
            logging.error(f"Failed to mark email as read. Status: {response.status_code}, Response: {response.text}")
            return self._create_response(
                status_code=response.status_code,
                status_message=f"Failed to mark email as read: {response.text}",
                response=None
            )

    def mark_email_as_unread(self, email_id: str) -> Dict[str, Any]:
        """
        Mark an email as unread.

        Args:
            email_id: The email message ID

        Returns:
            Standardized response dictionary
        """
        url = f"{self.base_url}/messages/{email_id}"
        headers = {'Content-Type': 'application/json'}
        data = {"isRead": False}

        response = self.session.patch(url, headers=headers, json=data)

        if response.status_code == 200:
            logging.info(f"Email {email_id} marked as unread")
            return self._create_response(
                status_code=200,
                status_message="Email marked as unread successfully",
                response={"email_id": email_id}
            )
        else:
            logging.error(f"Failed to mark email as unread. Status: {response.status_code}, Response: {response.text}")
            return self._create_response(
                status_code=response.status_code,
                status_message=f"Failed to mark email as unread: {response.text}",
                response=None
            )

    def mark_email_as_unread_batch(self, email_ids: List[str], log_message:str=None) -> Dict[str, Any]:
        """
        Mark multiple emails as unread in batch.

        Args:
            email_ids: List of email message IDs

        Returns:
            Standardized response dictionary
        """
        headers = {'Content-Type': 'application/json'}
        failed_emails = []
        successful_emails = []
        logging.info(log_message)
        for email_id in email_ids:
            url = f"{self.base_url}/messages/{email_id}"
            data = {"isRead": False}
            response = self.session.patch(url, headers=headers, json=data)

            if response.status_code != 200:
                logging.error(f"Failed to mark email {email_id} as unread. Status: {response.status_code}")
                failed_emails.append(email_id)
            else:
                logging.info(f"Successfully marked email {email_id} as unread. Status: {response.status_code}")
                successful_emails.append(email_id)

        if failed_emails:
            logging.error(f"Failed to mark {len(failed_emails)} emails as unread: {failed_emails}")
            return self._create_response(
                status_code=207,  # Multi-Status
                status_message=f"Marked {len(successful_emails)} emails as unread, {len(failed_emails)} failed",
                response={"successful": successful_emails, "failed": failed_emails}
            )

        logging.info(f"Successfully marked {len(email_ids)} emails as unread")
        return self._create_response(
            status_code=200,
            status_message=f"All {len(email_ids)} emails marked as unread successfully",
            response={"successful": successful_emails, "failed": []}
        )

    # ==========================================
    # ATTACHMENT OPERATIONS
    # ==========================================

    def get_email_attachments(self, email_id: str) -> Dict[str, Any]:
        """
        Fetch all attachments for a specific email.

        Args:
            email_id: The email message ID

        Returns:
            Standardized response with list of attachment objects
        """
        url = f"{self.base_url}/messages/{email_id}/attachments"
        response = self.session.get(url)

        if response.status_code == 200:
            attachments = response.json().get('value', [])
            logging.info(f"Retrieved {len(attachments)} attachments for email {email_id}")
            return self._create_response(
                status_code=200,
                status_message=f"Successfully retrieved {len(attachments)} attachments",
                response=attachments
            )
        else:
            logging.error(f"Failed to fetch attachments. Status: {response.status_code}, Response: {response.text}")
            return self._create_response(
                status_code=response.status_code,
                status_message=f"Failed to fetch attachments: {response.text}",
                response=[]
            )

    def get_attachment_content(self, email_id: str, attachment_id: str) -> Dict[str, Any]:
        """
        Fetch the binary content of a specific attachment.

        Args:
            email_id: The email message ID
            attachment_id: The attachment ID

        Returns:
            Standardized response with attachment content as bytes
        """
        url = f"{self.base_url}/messages/{email_id}/attachments/{attachment_id}/$value"
        response = self.session.get(url)

        if response.status_code == 200:
            logging.info(f"Retrieved attachment content for attachment {attachment_id}")
            return self._create_response(
                status_code=200,
                status_message="Successfully retrieved attachment content",
                response=response.content
            )
        else:
            logging.error(f"Failed to fetch attachment content. Status: {response.status_code}, Response: {response.text}")
            return self._create_response(
                status_code=response.status_code,
                status_message=f"Failed to fetch attachment content: {response.text}",
                response=None
            )

    # ==========================================
    # FOLDER OPERATIONS
    # ==========================================

    def _get_all_folders(self) -> List[Dict]:
        """
        Fetch all mail folders including child folders with pagination.

        Returns:
            List of all folders (flattened structure)
        """
        all_folders = []

        def get_folders_with_pagination(url: str) -> List[Dict]:
            """Helper function to handle pagination for any folder URL"""
            folders = []
            current_url = url

            while current_url:
                response = self.session.get(current_url)

                if response.status_code == 200:
                    data = response.json()
                    batch_folders = data.get('value', [])
                    folders.extend(batch_folders)

                    current_url = data.get('@odata.nextLink')
                    if current_url:
                        logging.info(f"Fetching next page: {len(batch_folders)} folders retrieved so far")
                else:
                    logging.error(f"Failed to fetch folders. Status: {response.status_code}, Response: {response.text}")
                    break

            return folders

        def get_child_folders_recursive(folder_id: str) -> List[Dict]:
            """Recursively get all child folders for a given folder"""
            child_folders = []
            child_url = f"{self.base_url}/mailFolders/{folder_id}/childFolders?$top=100"

            children = get_folders_with_pagination(child_url)
            child_folders.extend(children)

            for child in children:
                grandchildren = get_child_folders_recursive(child['id'])
                child_folders.extend(grandchildren)

            return child_folders

        try:
            # Get all top-level folders with pagination
            top_level_url = f"{self.base_url}/mailFolders?$top=100"
            top_level_folders = get_folders_with_pagination(top_level_url)
            all_folders.extend(top_level_folders)

            logging.info(f"Fetched {len(top_level_folders)} top-level folders for user '{self.user}'")

            # For each top-level folder, get all its child folders recursively
            for folder in top_level_folders:
                child_folders = get_child_folders_recursive(folder['id'])
                all_folders.extend(child_folders)

                if child_folders:
                    logging.info(f"Fetched {len(child_folders)} child folders for '{folder.get('displayName', 'Unknown')}'")

            self.all_email_folders = all_folders
            logging.info(f"Total folders fetched for user '{self.user}': {len(all_folders)}")
            return all_folders

        except Exception as e:
            logging.error(f"Error fetching mail folders: {str(e)}")
            return []

    def _get_all_folders_with_hierarchy(self) -> List[Dict]:
        """
        Fetch all mail folders preserving the hierarchical structure.

        Returns:
            List of folders with nested children
        """
        def get_folders_with_pagination(url: str) -> List[Dict]:
            """Helper function to handle pagination"""
            folders = []
            current_url = url

            while current_url:
                response = self.session.get(current_url)

                if response.status_code == 200:
                    data = response.json()
                    batch_folders = data.get('value', [])
                    folders.extend(batch_folders)
                    current_url = data.get('@odata.nextLink')
                else:
                    logging.error(f"Failed to fetch folders. Status: {response.status_code}")
                    break

            return folders

        def add_children_to_folder(folder: Dict) -> None:
            """Recursively add children to a folder"""
            folder_id = folder['id']
            child_url = f"{self.base_url}/mailFolders/{folder_id}/childFolders?$top=100"

            children = get_folders_with_pagination(child_url)
            folder['children'] = children

            for child in children:
                add_children_to_folder(child)

        try:
            # Get top-level folders
            top_level_url = f"{self.base_url}/mailFolders?$top=100"
            top_level_folders = get_folders_with_pagination(top_level_url)

            # Add children to each top-level folder
            for folder in top_level_folders:
                add_children_to_folder(folder)

            self.all_email_folders = top_level_folders
            logging.info(f"Fetched folder hierarchy with {len(top_level_folders)} top-level folders for user '{self.user}'")
            return top_level_folders

        except Exception as e:
            logging.error(f"Error fetching mail folders: {str(e)}")
            return []

    @staticmethod
    def flatten_folder_hierarchy(folders: List[Dict]) -> List[Dict]:
        """
        Convert hierarchical folder structure to flat list.

        Args:
            folders: List of folders with potential nested children

        Returns:
            Flattened list of folders
        """
        flat_folders = []

        def flatten_recursive(folder_list: List[Dict]) -> None:
            for folder in folder_list:
                folder_copy = {k: v for k, v in folder.items() if k != 'children'}
                flat_folders.append(folder_copy)

                if 'children' in folder:
                    flatten_recursive(folder['children'])

        flatten_recursive(folders)
        return flat_folders

    def get_folderId_by_name(self, folder_name: str) -> Optional[str]:
        """
        Get the folder ID by its display name.

        Args:
            folder_name: The folder display name to search for

        Returns:
            Folder ID if found, None otherwise
        """
        self._get_all_folders()

        for folder in self.all_email_folders:
            if folder['displayName'].lower() == folder_name.lower():
                logging.info(f"Found folder '{folder_name}' with ID: {folder['id']}")
                return folder['id']

        logging.error(f"Folder '{folder_name}' not found for user '{self.user}'")
        raise Exception(f"""Folder '{folder_name}" not found for the user {self.user}""")

    # ==========================================
    # EMAIL DRAFTING AND SENDING
    # ==========================================

    def draft_email(
        self,
        subject: str,
        body: str,
        toRecipients: List[str],
        ccRecipients: List[str] = [],
        bccRecipients: List[str] = [],
        email_id: Optional[str] = None,
        update: bool = False
    ) -> Dict[str, Any]:
        """
        Create or update an email draft.

        Args:
            subject: Email subject
            body: Email body (HTML format)
            toRecipients: List of recipient email addresses
            ccRecipients: List of CC recipient email addresses
            bccRecipients: List of BCC recipient email addresses
            email_id: Email ID for updating existing draft
            update: Whether this is an update operation

        Returns:
            Dictionary with status and draft details
        """
        if update and email_id:
            url = f"{self.base_url}/messages/{email_id}"
        else:
            url = f"{self.base_url}/messages"

        data = {
            "subject": subject,
            "body": {
                "contentType": "HTML",
                "content": body
            },
            "toRecipients": [
                {"emailAddress": {"address": to_recipient}}
                for to_recipient in toRecipients
            ],
            "ccRecipients": [
                {"emailAddress": {"address": cc_recipient}}
                for cc_recipient in ccRecipients
            ],
            "bccRecipients": [
                {"emailAddress": {"address": bcc_recipient}}
                for bcc_recipient in bccRecipients
            ]
        }

        response = self.session.patch(url, json=data) if update else self.session.post(url, json=data)

        if response.status_code in [200, 201]:
            resp_json = response.json()
            logging.info(f"Successfully drafted/updated email with subject '{subject}'. Email ID: {resp_json.get('id')}")
            return self._create_response(
                status_code=response.status_code,
                status_message="Email draft created/updated successfully",
                response=resp_json
            )
        else:
            logging.error(f"Failed to create/update email draft. Status: {response.status_code}, Response: {response.text}")
            return self._create_response(
                status_code=response.status_code,
                status_message=f"Failed to create/update email draft: {response.text}",
                response=None
            )

    def update_draft(
        self,
        create_draft_if_not_found: bool,
        subject: str,
        body: str,
        toRecipients: List[str],
        email_id: Optional[str] = None,
        ccRecipients: List[str] = [],
        bccRecipients: List[str] = []
    ) -> Dict[str, Any]:
        """
        Update an existing draft or create a new one if not found.

        Args:
            create_draft_if_not_found: Whether to create a new draft if not found
            subject: Email subject
            body: Email body (HTML format)
            toRecipients: List of recipient email addresses
            email_id: Optional email ID to search for
            ccRecipients: List of CC recipient email addresses
            bccRecipients: List of BCC recipient email addresses

        Returns:
            Dictionary with operation status and details
        """
        draft_email = None

        if email_id:
            logging.info(f"Searching drafts for email with ID {email_id}")
            draft_result = self.search_email_in_draft('id', email_id)
            if draft_result['status_code'] == 200:
                draft_email = draft_result['response']

        if not draft_email and subject:
            logging.info(f"Searching drafts for email with subject '{subject}'")
            draft_result = self.search_email_in_draft('subject', subject)
            if draft_result['status_code'] == 200:
                draft_email = draft_result['response']

        if not draft_email:
            if create_draft_if_not_found:
                logging.info("No draft found. Creating new draft.")
                if subject and body and toRecipients:
                    return self.draft_email(
                        subject, body, toRecipients, ccRecipients, bccRecipients
                    )
                return self._create_response(
                    status_code=400,
                    status_message="No draft found and mandatory fields missing for new draft creation",
                    response=None
                )
            return self._create_response(
                status_code=404,
                status_message="No draft found and new draft creation skipped",
                response=None
            )

        return self.draft_email(
            subject, body, toRecipients, ccRecipients, bccRecipients,
            draft_email['id'], True
        )

    def send_email(
        self,
        email_id: Optional[str] = None,
        subject: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Send a draft email.

        Args:
            email_id: Draft email ID
            subject: Draft email subject (used if email_id not provided)

        Returns:
            Standardized response dictionary
        """
        draft_email = None

        if email_id:
            logging.info(f"Searching drafts for email with ID {email_id}")
            draft_result = self.search_email_in_draft('id', email_id)
            if draft_result['status_code'] == 200:
                draft_email = draft_result['response']

        if not draft_email and subject:
            logging.warning("Use subject to find draft email is not reliable. Consider using email_id. this will return the first matching subject.")
            logging.info(f"Searching drafts for email with subject '{subject}'")
            draft_result = self.search_email_in_draft('subject', subject)
            if draft_result['status_code'] == 200:
                draft_email = draft_result['response']

        if not draft_email:
            logging.warning("No draft email found with provided details")
            return self._create_response(
                status_code=404,
                status_message="No draft email found with provided details",
                response=None
            )

        url = f"{self.base_url}/messages/{draft_email['id']}/send"
        response = self.session.post(url)

        if response.status_code == 202:
            logging.info(f"Email sent successfully. Subject: '{draft_email['subject']}', ID: {draft_email['id']}")
            return self._create_response(
                status_code=202,
                status_message="Email sent successfully",
                response={"email_id": draft_email['id'], "subject": draft_email['subject']}
            )
        else:
            logging.error(f"Failed to send email. Subject: '{draft_email['subject']}', Status: {response.status_code}")
            return self._create_response(
                status_code=response.status_code,
                status_message=f"Failed to send email: {response.text}",
                response=None
            )

    # ==========================================
    # EMAIL MANAGEMENT OPERATIONS
    # ==========================================

    def delete_email(self, email_id: str, is_draft: bool = False) -> Dict[str, Any]:
        """
        Delete an email or draft.

        Args:
            email_id: The email message ID
            is_draft: Whether the email is in drafts folder

        Returns:
            Standardized response dictionary
        """
        if is_draft:
            url = f"{self.base_url}/mailFolders('drafts')/messages/{email_id}"
        else:
            url = f"{self.base_url}/messages/{email_id}"

        response = self.session.delete(url)

        if response.status_code in [200, 201, 204]:
            logging.info(f"Successfully deleted email. Email ID: {email_id}")
            return self._create_response(
                status_code=200,
                status_message="Email deleted successfully",
                response={"email_id": email_id}
            )
        else:
            logging.error(f"Failed to delete email. Status: {response.status_code}, Response: {response.text}")
            return self._create_response(
                status_code=response.status_code,
                status_message=f"Failed to delete email: {response.text}",
                response=None
            )

    def move_email_to_another_folder(self, email_id: str, folder_id: str) -> Dict[str, Any]:
        """
        Move an email to a different folder.

        Args:
            email_id: The email message ID
            folder_id: Destination folder ID

        Returns:
            Standardized response dictionary
        """
        url = f"{self.base_url}/messages/{email_id}/move"
        data = {"destinationId": folder_id}
        response = self.session.post(url, json=data)

        if response.status_code in [200, 201]:
            resp_json = response.json()
            logging.info(f"Successfully moved email {email_id} to folder {folder_id}")
            return self._create_response(
                status_code=response.status_code,
                status_message="Email moved successfully",
                response=resp_json
            )
        else:
            logging.error(f"Failed to move email. Status: {response.status_code}, Response: {response.text}")
            return self._create_response(
                status_code=response.status_code,
                status_message=f"Failed to move email: {response.text}",
                response=None
            )

    # ==========================================
    # CATEGORY OPERATIONS (EXPERIMENTAL)
    # ==========================================

    def _get_all_categories(self) -> Dict[str, Any]:
        """
        Fetch all Outlook categories.

        Returns:
            Standardized response with list of category objects
        """
        url = "https://graph.microsoft.com/v1.0/me/outlook/masterCategories"
        response = self.session.get(url)

        if response.status_code == 200:
            categories = response.json().get('value', [])
            logging.info(f"Retrieved {len(categories)} categories")
            return self._create_response(
                status_code=200,
                status_message=f"Successfully retrieved {len(categories)} categories",
                response=categories
            )
        else:
            logging.error(f"Failed to fetch categories. Status: {response.status_code}, Response: {response.text}")
            return self._create_response(
                status_code=response.status_code,
                status_message=f"Failed to fetch categories: {response.text}",
                response=[]
            )

    def get_categoryId_by_name(self, category_name: str) -> Optional[str]:
        """
        Get the category ID by its name.

        Note: This feature is currently disabled.

        Args:
            category_name: The category display name

        Returns:
            Category ID if found, None otherwise
        """
        logging.info("Skipping category fetch as it is not working currently.")
        return None

        # Disabled code below
        # categories = self._get_all_categories()
        # for category in categories:
        #     if category['displayName'].lower() == category_name.lower():
        #         return category['id']
        # logging.error(f"Category '{category_name}' not found.")
        # return None

    def update_email_category(self, email_id: str) -> Dict[str, Any]:
        """
        Update the category of an email.

        Note: This feature does not work yet.

        Args:
            email_id: The email message ID

        Returns:
            Standardized response dictionary
        """
        url = f"https://graph.microsoft.com/v1.0/me/messages/{email_id}"
        headers = {'Content-Type': 'application/json'}
        data = {
            "singleValueExtendedProperties": [
                {
                    "id": f"String {{{self.category}}} Name Color",
                    "value": "Green"
                }
            ]
        }
        response = self.session.patch(url, headers=headers, json=data)

        if response.status_code == 200:
            logging.info(f"Email {email_id} updated with category '{self.category_name}'")
            return self._create_response(
                status_code=200,
                status_message="Email category updated successfully",
                response={"email_id": email_id, "category": self.category_name}
            )
        else:
            logging.error(f"Failed to update email category. Status: {response.status_code}, Response: {response.text}")
            return self._create_response(
                status_code=response.status_code,
                status_message=f"Failed to update email category: {response.text}",
                response=None
            )

    # ==========================================
    # SHARED MAILBOX OPERATIONS
    # ==========================================

    def get_shared_mailbox_access(self) -> Dict[str, Any]:
        """
        Get all shared mailboxes and accounts that the current user has access to.

        Returns:
            Standardized response with shared mailbox information
        """
        shared_access = {
            'shared_mailboxes': [],
            'delegated_mailboxes': [],
            'accessible_users': []
        }

        def make_graph_request(url: str) -> List[Dict]:
            """Helper function to make Graph API requests with pagination"""
            all_items = []
            current_url = url

            while current_url:
                response = self.session.get(current_url)

                if response.status_code == 200:
                    data = response.json()
                    items = data.get('value', [])
                    all_items.extend(items)
                    current_url = data.get('@odata.nextLink')
                elif response.status_code == 403:
                    logging.warning(f"Access denied for URL: {url}")
                    break
                else:
                    logging.error(f"Failed request to {url}. Status: {response.status_code}")
                    break

            return all_items

        try:
            # Method 1: Get shared mailboxes (if available in tenant)
            logging.info("Fetching shared mailboxes...")
            try:
                shared_mailboxes_url = "https://graph.microsoft.com/v1.0/users?$filter=mailboxSettings/userPurpose eq 'shared'"
                shared_mailboxes = make_graph_request(shared_mailboxes_url)
                shared_access['shared_mailboxes'] = shared_mailboxes
                logging.info(f"Found {len(shared_mailboxes)} shared mailboxes")
            except Exception as e:
                logging.warning(f"Could not fetch shared mailboxes: {str(e)}")

            # Method 2: Get users and check mailbox access
            logging.info("Checking accessible user mailboxes...")
            users_url = "https://graph.microsoft.com/v1.0/users?$select=id,displayName,mail,userPrincipalName"
            all_users = make_graph_request(users_url)

            accessible_users = []
            for user in all_users:
                user_id = user['id']
                test_url = f"https://graph.microsoft.com/v1.0/users/{user_id}/mailFolders?$top=1"
                response = self.session.get(test_url)

                if response.status_code == 200:
                    accessible_users.append({
                        'id': user['id'],
                        'displayName': user.get('displayName', ''),
                        'mail': user.get('mail', ''),
                        'userPrincipalName': user.get('userPrincipalName', ''),
                        'hasMailboxAccess': True
                    })
                    logging.info(f"Access confirmed for: {user.get('displayName', 'Unknown')}")

            shared_access['accessible_users'] = accessible_users
            logging.info(f"Found {len(accessible_users)} accessible user mailboxes")

            # Method 3: Get delegate information
            logging.info("Fetching delegate information...")
            try:
                delegate_url = "https://graph.microsoft.com/v1.0/me/mailboxSettings"
                response = self.session.get(delegate_url)
                if response.status_code == 200:
                    mailbox_settings = response.json()
                    if 'delegateMeetingMessageDeliveryOptions' in mailbox_settings:
                        shared_access['delegate_settings'] = mailbox_settings
            except Exception as e:
                logging.warning(f"Could not fetch delegate information: {str(e)}")

            return self._create_response(
                status_code=200,
                status_message=f"Successfully retrieved {len(shared_access['accessible_users'])} accessible mailboxes",
                response=shared_access
            )

        except Exception as e:
            logging.error(f"Error getting shared mailbox access: {str(e)}")
            return self._create_response(
                status_code=500,
                status_message=f"Error getting shared mailbox access: {str(e)}",
                response=shared_access
            )

    def get_folders_for_shared_mailbox(self, user_id: str) -> Dict[str, Any]:
        """
        Get all folders for a specific shared mailbox/user.

        Args:
            user_id: The ID or email of the user/shared mailbox

        Returns:
            Standardized response with list of all folders for the specified mailbox
        """
        def get_folders_with_pagination(url: str) -> List[Dict]:
            """Helper function to handle pagination"""
            folders = []
            current_url = url

            while current_url:
                response = self.session.get(current_url)

                if response.status_code == 200:
                    data = response.json()
                    batch_folders = data.get('value', [])
                    folders.extend(batch_folders)
                    current_url = data.get('@odata.nextLink')
                else:
                    logging.error(f"Failed to fetch folders for {user_id}. Status: {response.status_code}")
                    break

            return folders

        def get_child_folders_recursive(folder_id: str, user_id: str) -> List[Dict]:
            """Recursively get all child folders"""
            child_folders = []
            child_url = f"https://graph.microsoft.com/v1.0/users/{user_id}/mailFolders/{folder_id}/childFolders?$top=100"

            children = get_folders_with_pagination(child_url)
            child_folders.extend(children)

            for child in children:
                grandchildren = get_child_folders_recursive(child['id'], user_id)
                child_folders.extend(grandchildren)

            return child_folders

        try:
            all_folders = []

            top_level_url = f"https://graph.microsoft.com/v1.0/users/{user_id}/mailFolders?$top=100"
            top_level_folders = get_folders_with_pagination(top_level_url)
            all_folders.extend(top_level_folders)

            for folder in top_level_folders:
                child_folders = get_child_folders_recursive(folder['id'], user_id)
                all_folders.extend(child_folders)

            logging.info(f"Fetched {len(all_folders)} folders for user {user_id}")
            return self._create_response(
                status_code=200,
                status_message=f"Successfully retrieved {len(all_folders)} folders",
                response=all_folders
            )

        except Exception as e:
            logging.error(f"Error fetching folders for user {user_id}: {str(e)}")
            return self._create_response(
                status_code=500,
                status_message=f"Error fetching folders: {str(e)}",
                response=[]
            )

    def get_all_accessible_folders(self) -> Dict[str, Any]:
        """
        Get folders from all accessible mailboxes (own + shared).

        Returns:
            Standardized response with dictionary organized by mailbox with folder information
        """
        all_mailbox_folders = {}

        try:
            # Get own folders
            logging.info("Fetching own mailbox folders...")
            own_folders = self._get_all_folders()
            all_mailbox_folders['own_mailbox'] = {
                'displayName': 'My Mailbox',
                'folders': own_folders
            }

            # Get shared mailbox access
            shared_result = self.get_shared_mailbox_access()

            if shared_result['status_code'] != 200:
                logging.warning(f"Could not retrieve shared mailbox access: {shared_result['status_message']}")
                shared_access = {'accessible_users': []}
            else:
                shared_access = shared_result['response']

            # Get folders for each accessible user/shared mailbox
            for user in shared_access.get('accessible_users', []):
                user_id = user['id']
                display_name = user['displayName']

                logging.info(f"Fetching folders for shared mailbox: {display_name}")
                folders_result = self.get_folders_for_shared_mailbox(user_id)

                if folders_result['status_code'] == 200:
                    all_mailbox_folders[user_id] = {
                        'displayName': display_name,
                        'mail': user.get('mail', ''),
                        'userPrincipalName': user.get('userPrincipalName', ''),
                        'folders': folders_result['response']
                    }
                else:
                    logging.warning(f"Could not fetch folders for {display_name}: {folders_result['status_message']}")

            # Summary
            total_mailboxes = len(all_mailbox_folders)
            total_folders = sum(len(mailbox['folders']) for mailbox in all_mailbox_folders.values())
            logging.info(f"Total accessible mailboxes: {total_mailboxes}")
            logging.info(f"Total folders across all mailboxes: {total_folders}")

            return self._create_response(
                status_code=200,
                status_message=f"Successfully retrieved folders from {total_mailboxes} mailboxes ({total_folders} total folders)",
                response=all_mailbox_folders
            )

        except Exception as e:
            logging.error(f"Error getting all accessible folders: {str(e)}")
            return self._create_response(
                status_code=500,
                status_message=f"Error getting all accessible folders: {str(e)}",
                response=all_mailbox_folders
            )
