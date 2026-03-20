const CONNECTORS = [

  {
    id: "salesforce",
    name: "Salesforce",
    categories: ["crm", "sales"],
    logo: "/static/images/logos/salesforce.png",

    auth_type: "oauth",
    api_key_label: "Client ID & Secret",

    connect_url: "/connectors/salesforce/connect",
    sync_url: "/connectors/salesforce/sync",
    disconnect_url: "/connectors/salesforce/disconnect",
    status_api: "/api/status/salesforce",
    save_app_url: "/connectors/salesforce/save_app",

    models: [
      { title: "Accounts", desc: "Organization and company records with industry and revenue data." },
      { title: "Contacts", desc: "Individual contact records with email, phone, and job information." },
      { title: "Leads", desc: "Sales lead records with status and conversion tracking." }
    ],

    tables: [
      "salesforce_accounts",
      "salesforce_contacts",
      "salesforce_leads"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Extracts CRM data from Salesforce including accounts, contacts, and leads.",

    data: [
      "Account records",
      "Contact information",
      "Lead tracking",
      "Sales pipeline",
      "Opportunity data"
    ]
  },

  {
    id: "jira",
    name: "Jira",
    categories: ["project_management", "productivity"],
    logo: "https://cdn.simpleicons.org/jira/0052CC",

    auth_type: "api_token",
    api_key_label: "API Token",

    connect_url: "/connectors/jira/connect",
    sync_url: "/connectors/jira/sync",
    disconnect_url: "/connectors/jira/disconnect",
    status_api: "/api/status/jira",
    save_app_url: "/connectors/jira/save_app",

    models: [
      { title: "Issues", desc: "Jira issue tracking records with status, priority, and assignee data." },
      { title: "Projects", desc: "Project records with metadata, leads, and configuration." }
    ],

    tables: [
      "jira_issues",
      "jira_projects"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Extracts project management data from Jira including issues and projects.",

    data: [
      "Issue tracking",
      "Project metadata",
      "Sprint data",
      "Workflow states",
      "Custom fields"
    ]
  },

  {
    id: "zoho_crm",
    name: "Zoho CRM",
    categories: ["crm", "sales"],
    logo: "https://cdn.simpleicons.org/zoho/DE4B39",

    auth_type: "oauth",
    api_key_label: "Client ID & Secret",

    connect_url: "/connectors/zoho_crm/connect",
    sync_url: "/connectors/zoho_crm/sync",
    disconnect_url: "/connectors/zoho_crm/disconnect",
    status_api: "/api/status/zoho_crm",
    save_app_url: "/connectors/zoho_crm/save_app",

    models: [
      { title: "Leads", desc: "Lead records with status, source, and qualification information." },
      { title: "Contacts", desc: "Contact records linked to accounts with communication details." },
      { title: "Deals", desc: "Sales opportunity records with amount, stage, and closing dates." }
    ],

    tables: [
      "zoho_leads",
      "zoho_contacts",
      "zoho_deals"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Extracts CRM data from Zoho including leads, contacts, and deals.",

    data: [
      "Lead management",
      "Contact records",
      "Deal tracking",
      "Sales pipeline",
      "Revenue forecasting"
    ]
  },

  {
    id: "paypal",
    name: "PayPal",
    categories: ["payments", "financial"],
    logo: "https://cdn.simpleicons.org/paypal/003087",

    auth_type: "oauth",
    api_key_label: "Client ID & Secret",

    connect_url: "/connectors/paypal/connect",
    sync_url: "/connectors/paypal/sync",
    disconnect_url: "/connectors/paypal/disconnect",
    status_api: "/api/status/paypal",
    save_app_url: "/connectors/paypal/save_app",

    models: [
      { title: "Transactions", desc: "Transaction history with amounts, fees, and status information." },
      { title: "Payments", desc: "Payment records with payer details and transaction metadata." }
    ],

    tables: [
      "paypal_transactions",
      "paypal_payments"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Extracts payment data from PayPal including transactions and payment history.",

    data: [
      "Transaction history",
      "Payment records",
      "Fee tracking",
      "Payer information",
      "Currency conversion"
    ]
  },

  {
    id: "asana",
    name: "Asana",
    categories: ["project_management", "productivity"],
    logo: "https://cdn.simpleicons.org/asana/F06A6A",

    auth_type: "api_token",
    api_key_label: "Personal Access Token",

    connect_url: "/connectors/asana/connect",
    sync_url: "/connectors/asana/sync",
    disconnect_url: "/connectors/asana/disconnect",
    status_api: "/api/status/asana",
    save_app_url: "/connectors/asana/save_app",

    models: [
      { title: "Tasks", desc: "Task records with completion status, assignee, and project information." },
      { title: "Projects", desc: "Project records with workspace and archive status." }
    ],

    tables: [
      "asana_tasks",
      "asana_projects"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Extracts tasks and projects from Asana workspaces.",

    data: [
      "Task tracking",
      "Project metadata",
      "Assignee information",
      "Due dates",
      "Completion status"
    ]
  },

  {
    id: "sendgrid",
    name: "SendGrid",
    categories: ["email", "marketing"],
    logo: "/static/images/logos/sendgrid.png",

    auth_type: "api_key",
    api_key_label: "SendGrid API Key",

    connect_url: "/connectors/sendgrid/connect",
    sync_url: "/connectors/sendgrid/sync",
    disconnect_url: "/connectors/sendgrid/disconnect",
    status_api: "/api/status/sendgrid",
    save_app_url: "/connectors/sendgrid/save_app",

    models: [
      { title: "Messages", desc: "Email message activity with opens, clicks, and delivery status." },
      { title: "Stats", desc: "Email statistics aggregated by day with delivery metrics." }
    ],

    tables: [
      "sendgrid_messages",
      "sendgrid_stats"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Extracts email messages and statistics from SendGrid.",

    data: [
      "Email activity",
      "Delivery stats",
      "Open rates",
      "Click tracking",
      "Bounce metrics"
    ]
  },

  {
    id: "mixpanel",
    name: "Mixpanel",
    categories: ["analytics", "product"],
    logo: "https://cdn.simpleicons.org/mixpanel/7856FF",

    auth_type: "api_secret",
    api_key_label: "Mixpanel API Secret",

    connect_url: "/connectors/mixpanel/connect",
    sync_url: "/connectors/mixpanel/sync",
    disconnect_url: "/connectors/mixpanel/disconnect",
    status_api: "/api/status/mixpanel",
    save_app_url: "/connectors/mixpanel/save_app",

    models: [
      { title: "Events", desc: "Product analytics events with user properties and device information." },
      { title: "Users", desc: "User profiles with engagement and demographic data." }
    ],

    tables: [
      "mixpanel_events",
      "mixpanel_users"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Extracts events and user profiles from Mixpanel analytics.",

    data: [
      "Event tracking",
      "User profiles",
      "Behavioral data",
      "Device information",
      "Geographic data"
    ]
  },

  {
    id: "monday",
    name: "Monday.com",
    categories: ["project_management", "productivity"],
    logo: "/static/images/logos/monday.png",

    auth_type: "api_token",
    api_key_label: "API Token",

    connect_url: "/connectors/monday/connect",
    sync_url: "/connectors/monday/sync",
    disconnect_url: "/connectors/monday/disconnect",
    status_api: "/api/status/monday",
    save_app_url: "/connectors/monday/save_app",

    models: [
      { title: "Boards", desc: "Board records with metadata, state, and configuration." },
      { title: "Items", desc: "Work items with column values, state, and creator information." },
      { title: "Users", desc: "User records with roles and permissions." }
    ],

    tables: [
      "monday_boards",
      "monday_items",
      "monday_users"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Extracts boards, items, and users from Monday.com workspaces.",

    data: [
      "Board metadata",
      "Work items",
      "Column values",
      "User information",
      "Workflow state"
    ]
  },

  {
    id: "clickup",
    name: "ClickUp",
    categories: ["project_management", "productivity"],
    logo: "https://cdn.simpleicons.org/clickup/7B68EE",

    auth_type: "api_token",
    api_key_label: "API Token",

    connect_url: "/connectors/clickup/connect",
    sync_url: "/connectors/clickup/sync",
    disconnect_url: "/connectors/clickup/disconnect",
    status_api: "/api/status/clickup",
    save_app_url: "/connectors/clickup/save_app",

    models: [
      { title: "Teams", desc: "Workspace teams with configuration and members." },
      { title: "Lists", desc: "Task lists with hierarchy and metadata." },
      { title: "Tasks", desc: "Task records with status, priority, and assignments." }
    ],

    tables: [
      "clickup_teams",
      "clickup_lists",
      "clickup_tasks"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Extracts tasks, lists, and teams from ClickUp workspaces.",

    data: [
      "Task tracking",
      "List hierarchy",
      "Team metadata",
      "Task status and priority",
      "Due dates and assignments"
    ]
  },

  {
    id: "helpscout",
    name: "Help Scout",
    categories: ["support", "customer_service"],
    logo: "https://cdn.simpleicons.org/helpscout/1292EE",

    auth_type: "api_key",
    api_key_label: "API Key",

    connect_url: "/connectors/helpscout/connect",
    sync_url: "/connectors/helpscout/sync",
    disconnect_url: "/connectors/helpscout/disconnect",
    status_api: "/api/status/helpscout",
    save_app_url: "/connectors/helpscout/save_app",

    models: [
      { title: "Mailboxes", desc: "Support mailboxes with email addresses and settings." },
      { title: "Conversations", desc: "Customer conversations with status and metadata." },
      { title: "Customers", desc: "Customer records with contact information." }
    ],

    tables: [
      "helpscout_mailboxes",
      "helpscout_conversations",
      "helpscout_customers"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Extracts conversations, customers, and mailboxes from HelpScout.",

    data: [
      "Conversation history",
      "Customer records",
      "Mailbox configuration",
      "Support metrics",
      "Ticket status"
    ]
  },

  {
    id: "notion",
    name: "Notion",
    categories: ["productivity", "documentation"],
    logo: "/static/images/logos/notion.png",

    auth_type: "api_key",
    api_key_label: "Notion Integration Token",

    connect_url: "/connectors/notion/connect",
    sync_url: "/connectors/notion/sync",
    disconnect_url: "/connectors/notion/disconnect",
    status_api: "/api/status/notion",
    save_app_url: "/connectors/notion/save_app",

    models: [
      { title: "Pages", desc: "Shared Notion pages and page metadata." },
      { title: "Databases", desc: "Database containers and schema payloads." },
      { title: "Blocks", desc: "Child blocks discovered under synced parents." }
    ],

    tables: [
      "notion_pages",
      "notion_databases",
      "notion_blocks"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Extracts Notion pages, databases, and blocks through an integration token.",

    data: [
      "Page content",
      "Database entries",
      "Block hierarchies",
      "Timestamps",
      "Object metadata"
    ]
  },

  {
    id: "airtable",
    name: "Airtable",
    categories: ["productivity", "database"],
    logo: "/static/images/logos/airtable.png",

    auth_type: "api_key",
    api_key_label: "Airtable Personal Access Token",

    connect_url: "/connectors/airtable/connect",
    sync_url: "/connectors/airtable/sync",
    disconnect_url: "/connectors/airtable/disconnect",
    status_api: "/api/status/airtable",
    save_app_url: "/connectors/airtable/save_app",

    models: [
      { title: "Tables", desc: "Base table metadata and schema details." },
      { title: "Records", desc: "Rows extracted from the configured Airtable table." }
    ],

    tables: [
      "airtable_tables",
      "airtable_records"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Extracts Airtable base metadata and table records through a personal access token.",

    data: [
      "Base tables",
      "Table records",
      "Field payloads",
      "Created timestamps",
      "Object metadata"
    ]
  },

  {
    id: "hubspot",
    name: "HubSpot",
    categories: ["crm", "sales", "marketing"],
    logo: "https://cdn.simpleicons.org/hubspot/FF7A59",

    auth_type: "api_key",
    api_key_label: "HubSpot Private App Token",

    connect_url: "/connectors/hubspot/connect",
    sync_url: "/connectors/hubspot/sync",
    disconnect_url: "/connectors/hubspot/disconnect",
    status_api: "/api/status/hubspot",
    save_app_url: "/connectors/hubspot/save_app",

    models: [
      { title: "Contacts", desc: "CRM contacts with profile and company fields." },
      { title: "Companies", desc: "Company records with firmographic metadata." },
      { title: "Deals", desc: "Pipeline deals with amount, stage, and close details." }
    ],

    tables: [
      "hubspot_contacts",
      "hubspot_companies",
      "hubspot_deals"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Extracts HubSpot CRM contacts, companies, and deals through a private app token.",

    data: [
      "Contact profiles",
      "Company records",
      "Deals and stages",
      "Update timestamps",
      "CRM object metadata"
    ]
  },

  {
    id: "shopify",
    name: "Shopify",
    categories: ["ecommerce"],
    logo: "https://cdn.simpleicons.org/shopify/95BF47",

    auth_type: "api_key",
    api_key_label: "Shopify Admin API Access Token",

    connect_url: "/connectors/shopify/connect",
    sync_url: "/connectors/shopify/sync",
    disconnect_url: "/connectors/shopify/disconnect",
    status_api: "/api/status/shopify",
    save_app_url: "/connectors/shopify/save_app",

    models: [
      { title: "Products", desc: "Store products, variants, and inventory details." },
      { title: "Orders", desc: "Customer orders, transactions, and fulfillment status." },
      { title: "Customers", desc: "Customer profiles, contact info, and lifetime value." }
    ],

    tables: [
      "shopify_products",
      "shopify_orders",
      "shopify_customers"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Extracts products, orders, and customers from your Shopify store through the Admin API.",

    data: [
      "Product catalog",
      "Order history",
      "Customer profiles",
      "Timestamps",
      "Object metadata"
    ]
  },

  {
    id: "zendesk",
    name: "Zendesk",
    categories: ["customer-service", "support"],
    logo: "https://cdn.simpleicons.org/zendesk/03363D",

    auth_type: "api_key",
    api_key_label: "Zendesk API Token",

    connect_url: "/connectors/zendesk/connect",
    sync_url: "/connectors/zendesk/sync",
    disconnect_url: "/connectors/zendesk/disconnect",
    status_api: "/api/status/zendesk",
    save_app_url: "/connectors/zendesk/save_app",

    models: [
      { title: "Tickets", desc: "Support tickets, comments, and tags." },
      { title: "Users", desc: "Customer profiles and agent details." },
      { title: "Organizations", desc: "Company groupings and shared data." }
    ],

    tables: [
      "zendesk_tickets",
      "zendesk_users",
      "zendesk_organizations"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Extracts tickets, users, and organizations from your Zendesk instance.",

    data: [
      "Ticket details",
      "Comments & events",
      "User profiles",
      "Organization metadata",
      "Timestamps"
    ]
  },

  {
    id: "intercom",
    name: "Intercom",
    categories: ["customer-service", "messaging"],
    logo: "https://cdn.simpleicons.org/intercom/1F8DED",

    auth_type: "api_key",
    api_key_label: "Intercom Access Token",

    connect_url: "/connectors/intercom/connect",
    sync_url: "/connectors/intercom/sync",
    disconnect_url: "/connectors/intercom/disconnect",
    status_api: "/api/status/intercom",
    save_app_url: "/connectors/intercom/save_app",

    models: [
      { title: "Contacts", desc: "User and lead profiles with attributes." },
      { title: "Conversations", desc: "Support chat history and transcripts." },
      { title: "Companies", desc: "Business accounts and firmographic data." }
    ],

    tables: [
      "intercom_contacts",
      "intercom_conversations",
      "intercom_companies"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Syncs contacts, conversations, and company data from Intercom.",

    data: [
      "Contact profiles",
      "Chat transcripts",
      "Company records",
      "Custom attributes",
      "Event timestamps"
    ]
  },

  {
    id: "mailchimp",
    name: "Mailchimp",
    categories: ["marketing", "email"],
    logo: "https://cdn.simpleicons.org/mailchimp/FFE01B",

    auth_type: "api_key",
    api_key_label: "Mailchimp API Key",

    connect_url: "/connectors/mailchimp/connect",
    sync_url: "/connectors/mailchimp/sync",
    disconnect_url: "/connectors/mailchimp/disconnect",
    status_api: "/api/status/mailchimp",
    save_app_url: "/connectors/mailchimp/save_app",

    models: [
      { title: "Lists", desc: "Audiences and segment metadata." },
      { title: "Members", desc: "Subscriber profiles and activity." },
      { title: "Campaigns", desc: "Email campaign reports and metrics." }
    ],

    tables: [
      "mailchimp_lists",
      "mailchimp_members",
      "mailchimp_campaigns"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Extracts audiences, members, and campaign reports from Mailchimp.",

    data: [
      "Subscriber lists",
      "Email addresses",
      "Campaign stats",
      "Open/click rates",
      "Member activity"
    ]
  },

  {
    id: "twilio",
    name: "Twilio",
    categories: ["communication", "sms", "voice"],
    logo: "/static/images/logos/twilio.png",

    auth_type: "api_key",
    api_key_label: "Twilio Auth Token",

    connect_url: "/connectors/twilio/connect",
    sync_url: "/connectors/twilio/sync",
    disconnect_url: "/connectors/twilio/disconnect",
    status_api: "/api/status/twilio",
    save_app_url: "/connectors/twilio/save_app",

    models: [
      { title: "Messages", desc: "SMS and WhatsApp message logs." },
      { title: "Calls", desc: "Voice call logs and metadata." },
      { title: "Recordings", desc: "Call recordings and transcription info." }
    ],

    tables: [
      "twilio_messages",
      "twilio_calls",
      "twilio_recordings"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Syncs messages, call logs, and recordings from your Twilio account.",

    data: [
      "Message logs",
      "Call details",
      "Recording links",
      "Price & status",
      "Timestamps"
    ]
  },

  {
    id: "pipedrive",
    name: "Pipedrive",
    categories: ["crm", "sales"],
    logo: "/static/images/logos/pipedrive.png",

    auth_type: "api_key",
    api_key_label: "Pipedrive API Token",

    connect_url: "/connectors/pipedrive/connect",
    sync_url: "/connectors/pipedrive/sync",
    disconnect_url: "/connectors/pipedrive/disconnect",
    status_api: "/api/status/pipedrive",
    save_app_url: "/connectors/pipedrive/save_app",

    models: [
      { title: "Deals", desc: "Sales deals with value, status, and pipeline information." },
      { title: "Persons", desc: "Contact records with email, phone, and organization links." },
      { title: "Organizations", desc: "Company records with addresses and contact counts." }
    ],

    tables: [
      "pipedrive_deals",
      "pipedrive_persons",
      "pipedrive_organizations"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Extracts CRM data from Pipedrive including deals, contacts, and organizations.",

    data: [
      "Deal pipeline data",
      "Contact information",
      "Organization records",
      "Activity timestamps",
      "Owner assignments"
    ]
  },

  {
    id: "freshdesk",
    name: "Freshdesk",
    categories: ["support", "helpdesk"],
    logo: "/static/images/logos/freshdesk.png",

    auth_type: "api_key",
    api_key_label: "Freshdesk API Key",

    connect_url: "/connectors/freshdesk/connect",
    sync_url: "/connectors/freshdesk/sync",
    disconnect_url: "/connectors/freshdesk/disconnect",
    status_api: "/api/status/freshdesk",
    save_app_url: "/connectors/freshdesk/save_app",

    models: [
      { title: "Tickets", desc: "Support tickets with status, priority, and assignee details." },
      { title: "Contacts", desc: "Customer contact records with email and phone information." },
      { title: "Companies", desc: "Company accounts with domain and description data." }
    ],

    tables: [
      "freshdesk_tickets",
      "freshdesk_contacts",
      "freshdesk_companies"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Extracts support data from Freshdesk including tickets, contacts, and companies.",

    data: [
      "Support tickets",
      "Contact records",
      "Company accounts",
      "Status tracking",
      "Priority levels"
    ]
  },

  {
    id: "klaviyo",
    name: "Klaviyo",
    categories: ["marketing", "email"],
    logo: "/static/images/logos/klaviyo.png",

    auth_type: "api_key",
    api_key_label: "Klaviyo Private API Key",

    connect_url: "/connectors/klaviyo/connect",
    sync_url: "/connectors/klaviyo/sync",
    disconnect_url: "/connectors/klaviyo/disconnect",
    status_api: "/api/status/klaviyo",
    save_app_url: "/connectors/klaviyo/save_app",

    models: [
      { title: "Profiles", desc: "Customer profiles with email, phone, and demographic data." },
      { title: "Events", desc: "Tracking events with timestamps and profile associations." },
      { title: "Lists", desc: "Email list segments with creation and update metadata." }
    ],

    tables: [
      "klaviyo_profiles",
      "klaviyo_events",
      "klaviyo_lists"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Extracts marketing data from Klaviyo including profiles, events, and lists.",

    data: [
      "Customer profiles",
      "Event tracking",
      "Email lists",
      "Engagement data",
      "Timestamps"
    ]
  },

  {
    id: "amplitude",
    name: "Amplitude",
    categories: ["analytics", "product"],
    logo: "/static/images/logos/amplitude.png",

    auth_type: "api_key",
    api_key_label: "Amplitude API Key",

    connect_url: "/connectors/amplitude/connect",
    sync_url: "/connectors/amplitude/sync",
    disconnect_url: "/connectors/amplitude/disconnect",
    status_api: "/api/status/amplitude",
    save_app_url: "/connectors/amplitude/save_app",

    models: [
      { title: "Events", desc: "Product analytics events with user, device, and location data." },
      { title: "Users", desc: "Unique user records with platform and geographic information." },
      { title: "Sessions", desc: "User session data with duration and device details." }
    ],

    tables: [
      "amplitude_events",
      "amplitude_users",
      "amplitude_sessions"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Extracts product analytics from Amplitude including events, users, and sessions.",

    data: [
      "Event streams",
      "User behavior",
      "Session data",
      "Device information",
      "Geographic data"
    ]
  },

  {
    id: "slack",
    name: "Slack",
    categories: ["communication"],
    logo: "/static/images/logos/slack.png",

    auth_type: "api_key",
    api_key_label: "Slack Bot Token",

    connect_url: "/connectors/slack/connect",
    sync_url: "/connectors/slack/sync",
    disconnect_url: "/connectors/slack/disconnect",
    status_api: "/api/status/slack",
    save_app_url: "/connectors/slack/save_app",

    models: [
      { title: "Channels", desc: "Public and private channels." },
      { title: "Messages", desc: "Message history across channels." },
      { title: "Users", desc: "Workspace members and bots." }
    ],

    tables: [
      "slack_channels",
      "slack_messages",
      "slack_users"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Extracts and syncs communication data from your Slack workspace.",

    data: [
      "Channel details",
      "Message threads",
      "User profiles",
      "Timestamps",
      "Message types"
    ]
  },

  /* ================= GOOGLE ================= */

  {
    id: "gmail",
    name: "Gmail",
    categories: ["google"],
    logo: "/static/images/logos/gmail.png",

    auth_type: "oauth",

    connect_url: "/connectors/gmail/connect",
    sync_url: "/connectors/gmail/sync",
    dashboard: "/dashboard/gmail",

    long_description: `
Connect and analyze your Gmail account to understand communication
patterns, customer interactions, and productivity trends.
All metadata and message headers are securely synchronized.
  `,

    steps: [
      { title: "Authorize Google", desc: "Login and grant Gmail read access." },
      { title: "Secure Token Storage", desc: "OAuth tokens stored in database." },
      { title: "Initial Sync", desc: "All email metadata is fetched." }
    ],

    tables: [
      "google_gmail_profile",
      "google_gmail_labels",
      "google_gmail_messages",
      "google_gmail_message_details"
    ],

    erd: "/static/images/erd/gmail_erd.png",

    description: "Collects email metadata and communication patterns.",

    data: [
      "Inbox messages",
      "Senders & recipients",
      "Subjects",
      "Threads",
      "Snippets"
    ]
  },

  {
    id: "drive",
    name: "Google Drive",
    categories: ["google", "cloud"],
    logo: "/static/images/logos/drive.png",

    auth_type: "oauth",

    connect_url: "/connectors/drive/connect",
    sync_url: "/connectors/drive/sync",
    dashboard: "/dashboard/drive",

    long_description: `
Indexes all files and folders in Google Drive and provides
visibility into document usage, storage patterns, and ownership.
  `,

    steps: [
      { title: "Authorize Drive", desc: "Grant Drive metadata access." },
      { title: "Scan Files", desc: "All folders and files indexed." },
      { title: "Delta Sync", desc: "Future changes tracked." }
    ],

    tables: [
      "google_drive_files",
      "google_drive_folders"
    ],

    erd: "/static/images/erd/drive_erd.png",

    description: "Indexes cloud files and folder structures.",

    data: [
      "File names",
      "Folder hierarchy",
      "File sizes",
      "Owners",
      "Timestamps"
    ]
  },

  {
    id: "calendar",
    name: "Google Calendar",
    categories: ["google"],
    logo: "/static/images/logos/calendar.png",

    auth_type: "oauth",

    connect_url: "/connectors/calendar/connect",
    sync_url: "/connectors/calendar/sync",
    dashboard: "/dashboard/calendar",

    long_description: `
Synchronizes calendar events to analyze meetings,
availability, and scheduling patterns.
  `,

    steps: [
      { title: "Authorize Calendar", desc: "Grant calendar access." },
      { title: "Fetch Calendars", desc: "All calendars loaded." },
      { title: "Sync Events", desc: "Events stored with metadata." }
    ],

    tables: [
      "google_calendar_list",
      "google_calendar_events"
    ],

    erd: "/static/images/erd/calendar_erd.png",

    description: "Tracks meeting schedules and event timelines.",

    data: [
      "Event titles",
      "Attendees",
      "Schedules",
      "Recurring events",
      "Locations"
    ]
  },

  {
    id: "sheets",
    name: "Google Sheets",
    categories: ["google"],
    logo: "/static/images/logos/sheets.png",

    auth_type: "oauth",

    connect_url: "/connectors/sheets/connect",
    sync_url: "/connectors/sheets/sync",
    dashboard: "/dashboard/sheets",

    long_description: `
Extracts spreadsheet structures and values
to support reporting, finance tracking, and KPI analysis.
  `,

    steps: [
      { title: "Authorize Sheets", desc: "Grant Sheets access." },
      { title: "Fetch Spreadsheets", desc: "All sheets discovered." },
      { title: "Read Values", desc: "Cell ranges downloaded." }
    ],

    tables: [
      "google_sheets",
      "google_sheet_values"
    ],

    erd: "/static/images/erd/sheets_erd.png",

    description: "Extracts spreadsheet data and metrics.",

    data: [
      "Sheet metadata",
      "Cell values",
      "Formulas",
      "Row data",
      "Timestamps"
    ]
  },

  {
    id: "forms",
    name: "Google Forms",
    categories: ["google"],
    logo: "/static/images/logos/forms.png",

    auth_type: "oauth",

    connect_url: "/connectors/forms/connect",
    sync_url: "/connectors/forms/sync",
    dashboard: "/dashboard/forms",

    long_description: `
Collects form structures and user responses
for surveys, feedback systems, and analytics pipelines.
  `,

    steps: [
      { title: "Authorize Forms", desc: "Grant Forms access." },
      { title: "Fetch Forms", desc: "All owned forms loaded." },
      { title: "Sync Responses", desc: "Responses normalized." }
    ],

    tables: [
      "google_forms",
      "google_form_responses"
    ],

    erd: "/static/images/erd/forms_erd.png",

    description: "Collects survey responses and form structures.",

    data: [
      "Questions",
      "Responses",
      "Submission time",
      "User metadata",
      "Form schema"
    ]
  },

  {
    id: "contacts",
    name: "Google Contacts",
    categories: ["google"],
    logo: "/static/images/logos/contacts.png",

    auth_type: "oauth",

    connect_url: "/connectors/contacts/connect",
    sync_url: "/connectors/contacts/sync",
    dashboard: "/dashboard/contacts",

    long_description: `
Synchronizes personal and business contacts
to enrich CRM and customer databases.
  `,

    steps: [
      { title: "Authorize Contacts", desc: "Grant People API access." },
      { title: "Fetch Profiles", desc: "All contacts retrieved." },
      { title: "Normalize Data", desc: "Fields standardized." }
    ],

    tables: [
      "google_contacts_persons"
    ],

    erd: "/static/images/erd/contacts_erd.png",

    description: "Synchronizes contact and CRM information.",

    data: [
      "Names",
      "Emails",
      "Phone numbers",
      "Organizations",
      "Addresses"
    ]
  },

  {
    id: "tasks",
    name: "Google Tasks",
    categories: ["google"],
    logo: "/static/images/logos/tasks.png",

    auth_type: "oauth",

    connect_url: "/connectors/tasks/connect",
    sync_url: "/connectors/tasks/sync",
    dashboard: "/dashboard/tasks",

    long_description: `
Tracks task lists and individual tasks
to analyze productivity and completion trends.
  `,

    steps: [
      { title: "Authorize Tasks", desc: "Grant Tasks API access." },
      { title: "Fetch Lists", desc: "Task lists loaded." },
      { title: "Sync Items", desc: "Task items stored." }
    ],

    tables: [
      "google_tasks_lists",
      "google_tasks_items"
    ],

    erd: "/static/images/erd/tasks_erd.png",

    description: "Tracks personal productivity tasks.",

    data: [
      "Task lists",
      "Task status",
      "Due dates",
      "Completion state",
      "Updates"
    ]
  },

  {
    id: "ga4",
    name: "Google Analytics (GA4)",
    categories: ["google", "analytics"],
    logo: "/static/images/logos/googleanalytics.png",

    auth_type: "oauth",

    connect_url: "/connectors/ga4/connect",
    sync_url: "/connectors/ga4/sync",
    dashboard: "/dashboard/ga4",

    long_description: `
Collects GA4 analytics reports for understanding
user behavior, traffic sources, and conversions.
  `,

    steps: [
      { title: "Authorize Analytics", desc: "Grant GA4 access." },
      { title: "Select Property", desc: "Choose analytics property." },
      { title: "Generate Reports", desc: "Metrics synchronized." }
    ],

    tables: [
      "ga4_website_overview",
      "ga4_devices",
      "ga4_locations",
      "ga4_traffic_sources",
      "ga4_events"
    ],

    erd: "/static/images/erd/ga4_erd.png",

    description: "Analyzes website traffic and user behavior.",

    data: [
      "Sessions",
      "Page views",
      "Bounce rate",
      "Traffic sources",
      "Conversions"
    ]
  },

  {
    id: "search-console",
    name: "Search Console",
    categories: ["google", "analytics"],
    logo: "/static/images/logos/searchconsole.png",

    auth_type: "oauth",

    connect_url: "/connectors/search-console/connect",
    sync_url: "/connectors/search-console/sync",
    dashboard: "/dashboard/searchconsole",

    long_description: `
Tracks keyword rankings and SEO performance
using Google Search Console reports.
  `,

    steps: [
      { title: "Authorize GSC", desc: "Grant webmaster access." },
      { title: "Select Site", desc: "Choose verified site." },
      { title: "Sync Queries", desc: "SEO data fetched." }
    ],

    tables: [
      "google_search_console"
    ],

    erd: "/static/images/erd/searchconsole_erd.png",

    description: "Monitors website SEO performance.",

    data: [
      "Search queries",
      "Impressions",
      "Clicks",
      "Ranking",
      "Index status"
    ]
  },

  {
    id: "youtube",
    name: "YouTube",
    categories: ["google", "video", "social"],
    logo: "/static/images/logos/youtube.png",

    auth_type: "oauth",

    connect_url: "/connectors/youtube/connect",
    sync_url: "/connectors/youtube/sync",
    dashboard: "/dashboard/youtube",

    long_description: `
Collects channel and video analytics
to evaluate audience engagement and growth.
  `,

    steps: [
      { title: "Authorize YouTube", desc: "Grant YouTube read access." },
      { title: "Fetch Channel", desc: "Channel metadata loaded." },
      { title: "Sync Videos", desc: "Videos and comments stored." }
    ],

    tables: [
      "google_youtube_channels",
      "google_youtube_videos",
      "google_youtube_comments",
      "google_youtube_state"
    ],

    erd: "/static/images/erd/youtube_erd.png",

    description: "Tracks channel and video engagement.",

    data: [
      "Views",
      "Subscribers",
      "Likes",
      "Comments",
      "Watch time"
    ]
  },

  {
    id: "trends",
    name: "Google Trends",
    categories: ["google"],
    logo: "/static/images/logos/googletrends.png",

    auth_type: "none",

    connect_url: null,
    sync_url: "/connectors/trends/sync",
    dashboard: "/dashboard/trends",

    long_description: `
Tracks keyword popularity and interest-over-time trends
to identify seasonal patterns and emerging topics.
Uses Pytrends for data collection.
  `,

    steps: [
      { title: "Configure Keywords", desc: "Select keywords to track." },
      { title: "Start Sync", desc: "Trigger Trends ingestion." },
      { title: "Time-Series Storage", desc: "Interest values saved." }
    ],

    tables: [
      "google_trends_interest",
      "google_trends_related",
      "google_trends_state"
    ],

    erd: "/static/images/erd/trends_erd.png",

    description: "Provides keyword popularity trends.",

    data: [
      "Search volume",
      "Regional trends",
      "Keywords",
      "Time series",
      "Topics"
    ]
  },

  {
    id: "news",
    name: "Google News",
    categories: ["google", "content"],
    logo: "/static/images/logos/googlenews.png",

    auth_type: "none",

    connect_url: null,
    sync_url: "/connectors/news/sync",
    dashboard: "/dashboard/news",

    long_description: `
Aggregates news articles using public RSS feeds
to monitor brand mentions and trending topics.
  `,

    steps: [
      { title: "Configure Keywords", desc: "Define news queries." },
      { title: "Fetch Feeds", desc: "RSS feeds parsed." },
      { title: "Store Articles", desc: "Metadata indexed." }
    ],

    tables: [
      "google_news_articles",
      "google_news_state"
    ],

    erd: "/static/images/erd/news_erd.png",

    description: "Monitors news and media mentions.",

    data: [
      "Articles",
      "Sources",
      "Publish time",
      "Topics",
      "Publishers"
    ]
  },

  {
    id: "books",
    name: "Google Books",
    categories: ["google", "content"],
    logo: "/static/images/logos/googlebooks.png",

    auth_type: "api_key",

    connect_url: null,
    sync_url: "/connectors/books/sync",
    dashboard: "/dashboard/books",

    long_description: `
Collects bibliographic metadata using Google Books API
to build searchable digital libraries.
  `,

    steps: [
      { title: "Configure API Key", desc: "Set Books API key." },
      { title: "Run Queries", desc: "Search by author/ISBN." },
      { title: "Store Volumes", desc: "Metadata saved." }
    ],

    tables: [
      "google_books_volumes",
      "google_books_state"
    ],

    erd: "/static/images/erd/books_erd.png",

    description: "Accesses book catalog metadata.",

    data: [
      "Titles",
      "Authors",
      "ISBN",
      "Descriptions",
      "Categories"
    ]
  },

  {
    id: "webfonts",
    name: "Google Webfonts",
    categories: ["google"],
    logo: "/static/images/logos/webfonts.png",

    auth_type: "api_key",

    connect_url: null,
    sync_url: "/connectors/webfonts/sync",
    dashboard: "/dashboard/webfonts",

    long_description: `
Indexes Google Fonts catalog for design systems
and typography optimization.
  `,

    steps: [
      { title: "Configure API Key", desc: "Set Webfonts API key." },
      { title: "Fetch Catalog", desc: "Fonts downloaded." },
      { title: "Cache Data", desc: "Metadata stored." }
    ],

    tables: [
      "google_webfonts_fonts"
    ],

    erd: "/static/images/erd/webfonts_erd.png",

    description: "Indexes font families and assets.",

    data: [
      "Font families",
      "Variants",
      "Subsets",
      "URLs",
      "Metadata"
    ]
  },

  {
    id: "pagespeed",
    name: "PageSpeed",
    categories: ["google", "analytics"],
    logo: "/static/images/logos/pagespeed.png",

    auth_type: "api_key",

    connect_url: null,
    sync_url: "/connectors/pagespeed/sync",
    dashboard: "/dashboard/pagespeed",

    long_description: `
Measures website performance using Lighthouse reports
to optimize SEO and loading speed.
  `,

    steps: [
      { title: "Enter URL", desc: "Provide site URL." },
      { title: "Run Audit", desc: "PageSpeed analysis executed." },
      { title: "Store Results", desc: "Scores saved." }
    ],

    tables: [
      "google_pagespeed"
    ],

    erd: "/static/images/erd/pagespeed_erd.png",

    description: "Measures website performance metrics.",

    data: [
      "Load time",
      "SEO score",
      "Accessibility",
      "Performance",
      "Best practices"
    ]
  },

  {
    id: "gcs",
    name: "Google Cloud Storage",
    categories: ["google", "cloud"],
    logo: "/static/images/logos/gcs.png",

    auth_type: "oauth",

    connect_url: "/connectors/gcs/connect",
    sync_url: "/connectors/gcs/sync",
    dashboard: "/dashboard/gcs",

    long_description: `
Analyzes cloud storage usage, bucket metadata,
and object-level information.
  `,

    steps: [
      { title: "Authorize GCS", desc: "Grant storage access." },
      { title: "Fetch Buckets", desc: "Buckets enumerated." },
      { title: "Sync Objects", desc: "Files indexed." }
    ],

    tables: [
      "google_gcs_buckets",
      "google_gcs_objects"
    ],

    erd: "/static/images/erd/gcs_erd.png",

    description: "Analyzes cloud storage usage.",

    data: [
      "Buckets",
      "Objects",
      "Sizes",
      "Types",
      "Update times"
    ]
  },

  {
    id: "bigquery",
    name: "Google BigQuery",
    categories: ["data_warehouse", "google"],
    logo: "https://cdn.simpleicons.org/googlebigquery/4285F4",

    auth_type: "service_account",

    route: "/connectors/bigquery",
    connect_url: "/connectors/bigquery/connect",
    sync_url: "/connectors/bigquery/sync",
    dashboard: "/dashboard/bigquery",

    long_description: `
Google BigQuery is the central destination warehouse for Segmento. It receives unified rows
from all source connectors via the sync engine and stores them in per-source tables inside
your configured dataset.
    `,

    steps: [
      { title: "Provide Service Account", desc: "Paste your BigQuery service account JSON key." },
      { title: "Validate Dataset", desc: "Segmento checks dataset access and metadata permissions." },
      { title: "Start Warehouse Sync", desc: "Source connectors stream unified rows into BigQuery tables." }
    ],

    tables: [
      "facebook_ads",
      "github_repos",
      "socialinsider_posts"
    ],

    erd: "/static/images/erd/gcs_erd.png",

    description: "Primary analytics warehouse destination powered by BigQuery streaming and load jobs.",

    data: [
      "Per-source fact tables",
      "Unified uid and source identifiers",
      "Raw JSON payloads",
      "Normalized metrics and attributes",
      "Incremental sync timestamps"
    ]
  },

  {
    id: "classroom",
    name: "Google Classroom",
    categories: ["google"],
    logo: "/static/images/logos/classroom.png",

    auth_type: "oauth",

    connect_url: "/connectors/classroom/connect",
    sync_url: "/connectors/classroom/sync",
    dashboard: "/dashboard/classroom",

    long_description: `
Collects classroom data for education analytics
including courses, assignments, and submissions.
  `,

    steps: [
      { title: "Authorize Classroom", desc: "Grant education scopes." },
      { title: "Load Courses", desc: "Courses fetched." },
      { title: "Sync Submissions", desc: "Student data stored." }
    ],

    tables: [
      "google_classroom_courses",
      "google_classroom_teachers",
      "google_classroom_students",
      "google_classroom_announcements",
      "google_classroom_coursework",
      "google_classroom_submissions"
    ],

    erd: "/static/images/erd/classroom_erd.png",

    description: "Collects education activity data.",

    data: [
      "Courses",
      "Students",
      "Assignments",
      "Submissions",
      "Announcements"
    ]
  },

  {
    id: "factcheck",
    name: "Fact Check",
    categories: ["google", "content"],
    logo: "/static/images/logos/factcheck.png",

    auth_type: "api_key",

    connect_url: null,
    sync_url: "http://localhost:4000/google/sync/factcheck",
    dashboard: "/dashboard/factcheck",

    long_description: `
Retrieves verified claims to monitor misinformation
and validate public statements.
  `,

    steps: [
      { title: "Configure API Key", desc: "Set FactCheck API key." },
      { title: "Run Queries", desc: "Claims searched." },
      { title: "Store Reviews", desc: "Ratings saved." }
    ],

    tables: [
      "google_factcheck_claims",
      "google_factcheck_state"
    ],

    erd: "/static/images/erd/factcheck_erd.png",

    description: "Verifies claims and misinformation.",

    data: [
      "Claims",
      "Sources",
      "Ratings",
      "Reviews",
      "Publishers"
    ]
  },

  /* ================= META ================= */
  {
    id: "facebook",
    name: "Facebook Pages",
    categories: ["meta", "social"],
    logo: "/static/images/logos/facebook.png",

    auth_type: "oauth",

    connect_url: "http://localhost:4000/connectors/facebook/connect",
    sync_url: "http://localhost:4000/connectors/facebook/sync",
    dashboard: "/dashboard/facebook",

    long_description: `
Connect and analyze your Facebook Page to understand audience engagement,
content performance, reactions, comments, and daily insights.
All page data is securely synchronized using OAuth authorization.
  `,

    steps: [
      { title: "Provide App Credentials", desc: "Enter Facebook App ID and App Secret." },
      { title: "Authorize Access", desc: "Grant pages permissions via OAuth." },
      { title: "Initial Sync", desc: "Page posts, comments, reactions and insights are fetched." }
    ],

    tables: [
      "facebook_pages_metadata",
      "facebook_page_posts",
      "facebook_post_comments",
      "facebook_reactions",
      "facebook_page_insights"
    ],

    erd: "/static/images/erd/facebook_pages_erd.png",

    description: "Collects page metadata, posts, comments, reactions and insights.",

    data: [
      "Page information",
      "Posts & stories",
      "Comments",
      "Reactions summary",
      "Daily insights metrics"
    ]
  },

  /* ================= META ADS ================= */
  {
    id: "facebook_ads",
    name: "Facebook Ads",
    categories: ["meta", "ads"],
    logo: "/static/images/logos/facebook.png",

    auth_type: "oauth",

    connect_url: "http://localhost:4000/connectors/facebook_ads/connect",
    sync_url: "http://localhost:4000/connectors/facebook_ads/sync",
    dashboard: "/dashboard/facebook_ads",

    long_description: `
Connect and analyze your Facebook Ad Account to monitor campaign
performance, ad sets, creatives, and advertising insights.
All advertising data is securely synchronized using OAuth authorization.
  `,

    steps: [
      { title: "Provide App Credentials", desc: "Enter Facebook App ID and App Secret." },
      { title: "Authorize Access", desc: "Grant ads_read and ads_management permissions via OAuth." },
      { title: "Initial Sync", desc: "Ad Accounts, Campaigns, Ads, Creatives and Insights are fetched." }
    ],

    tables: [
      "facebook_ad_accounts",
      "facebook_ad_campaigns",
      "facebook_ad_sets",
      "facebook_ads",
      "facebook_ad_creatives",
      "facebook_ads_insights"
    ],

    erd: "/static/images/erd/facebook_ads_erd.png",

    description: "Collects ad accounts, campaigns, ad sets, ads, creatives and performance insights.",

    data: [
      "Ad account metadata",
      "Campaign structure",
      "Ad sets configuration",
      "Ad creatives",
      "Performance insights (spend, impressions, clicks, CTR, CPC, CPM, reach)"
    ]
  },

  {
    id: "instagram",
    name: "Instagram",
    categories: ["meta", "social"],
    logo: "/static/images/logos/instagram.png",

    auth_type: "oauth",

    connect_url: "http://localhost:4000/instagram/connect",
    sync_url: "http://localhost:4000/connectors/instagram/sync",
    dashboard: "/dashboard/instagram",

    long_description: `
Connect your Instagram Business account through Meta Graph API to sync
media, captions, engagement counters, and publishing timestamps.
OAuth access is scoped and securely stored.
  `,

    steps: [
      { title: "Provide App Credentials", desc: "Enter Meta App ID and App Secret." },
      { title: "Authorize Access", desc: "Grant Instagram and page scopes via OAuth." },
      { title: "Initial Sync", desc: "Instagram media and engagement fields are fetched." }
    ],

    tables: [
      "instagram_media",
      "instagram_media_metrics"
    ],

    erd: "/static/images/erd/facebook_pages_erd.png",

    description: "Collects Instagram business media and engagement metrics.",

    data: [
      "Media posts",
      "Captions",
      "Permalinks",
      "Like counts",
      "Comment counts"
    ]
  },

  {
    id: "tiktok",
    name: "TikTok Business",
    categories: ["video", "ads"],
    logo: "/static/images/logos/tiktok.png",

    auth_type: "oauth",

    connect_url: "http://localhost:4000/connectors/tiktok/connect",
    sync_url: "http://localhost:4000/connectors/tiktok/sync",
    dashboard: "/dashboard/tiktok",

    long_description: `
Connect your TikTok Business account to ingest campaigns, ads,
and integrated ad reports with secure OAuth token management.
  `,

    steps: [
      { title: "Provide App Credentials", desc: "Enter TikTok client key, client secret and advertiser ID." },
      { title: "Authorize Access", desc: "Complete OAuth authorization to grant business data access." },
      { title: "Run Sync", desc: "Campaign, ad and integrated report data are fetched and stored." }
    ],

    tables: [
      "tiktok_campaigns",
      "tiktok_ads",
      "tiktok_reports"
    ],

    erd: "/static/images/logos/tiktok.png",

    description: "Collects TikTok Business campaigns, ads and performance reports.",

    data: [
      "Campaign metadata",
      "Ad metadata",
      "Spend and impressions",
      "Clicks and CTR",
      "Daily report metrics"
    ]
  },

  {
    id: "taboola",
    name: "Taboola Backstage",
    categories: ["ads"],
    logo: "/static/images/logos/taboola.png",

    auth_type: "oauth",

    connect_url: "http://localhost:4000/connectors/taboola/connect",
    sync_url: "http://localhost:4000/connectors/taboola/sync",
    dashboard: "/dashboard/taboola",

    long_description: `
Connect your Taboola Backstage API credentials to ingest campaign performance,
top campaign content, and publisher revenue reports with token caching.
  `,

    steps: [
      { title: "Provide API Credentials", desc: "Enter Taboola client ID, client secret and account ID." },
      { title: "Validate Connection", desc: "Client credentials token is generated and allowed accounts are verified." },
      { title: "Run Sync", desc: "Campaign, ad-level content and publisher revenue reports are ingested." }
    ],

    tables: [
      "taboola_campaign_reports",
      "taboola_ads",
      "taboola_publisher_revenue"
    ],

    erd: "/static/images/logos/taboola.png",

    description: "Collects Taboola campaign, content, and publisher revenue metrics.",

    data: [
      "Impressions and clicks",
      "Spend, CPC, CPM, CTR",
      "Conversions and ROAS",
      "Top campaign content",
      "Publisher revenue KPIs"
    ]
  },

  {
    id: "outbrain",
    name: "Outbrain Amplify",
    categories: ["ads"],
    logo: "/static/images/logos/outbrain.png",

    auth_type: "oauth",

    connect_url: "http://localhost:4000/connectors/outbrain/connect",
    sync_url: "http://localhost:4000/connectors/outbrain/sync",
    dashboard: "/dashboard/outbrain",

    long_description: `
Connect your Outbrain Amplify credentials to ingest marketer accounts,
campaign reports, promoted content performance, and periodic campaign metrics.
  `,

    steps: [
      { title: "Provide Credentials", desc: "Enter Outbrain username, password and default marketer ID." },
      { title: "Validate Token", desc: "OB-TOKEN-V1 is generated and marketer access is validated." },
      { title: "Run Sync", desc: "Campaign and promoted content reports are ingested." }
    ],

    tables: [
      "outbrain_marketers",
      "outbrain_campaign_reports",
      "outbrain_ads"
    ],

    erd: "/static/images/logos/outbrain.png",

    description: "Collects Outbrain marketers, campaigns and promoted content performance.",

    data: [
      "Marketer accounts",
      "Campaign metrics",
      "Promoted content metrics",
      "Periodic breakdown metrics",
      "Clicks, spend and conversions"
    ]
  },

  {
    id: "similarweb",
    name: "SimilarWeb",
    categories: ["analytics"],
    logo: "/static/images/logos/similarweb.png",

    auth_type: "api_key",

    connect_url: "http://localhost:4000/connectors/similarweb/connect",
    sync_url: "http://localhost:4000/connectors/similarweb/sync",
    dashboard: "/dashboard/similarweb",

    long_description: `
Connect SimilarWeb using your API key to ingest website traffic and engagement,
marketing channel shares, social traffic sources, and search keyword analytics.
  `,

    steps: [
      { title: "Provide API Key", desc: "Enter SimilarWeb API key and domain." },
      { title: "Validate Access", desc: "API key is validated against SimilarWeb endpoint." },
      { title: "Run Sync", desc: "Domain overview, channels, referrals and keywords are ingested." }
    ],

    tables: [
      "similarweb_domain_overview",
      "similarweb_traffic_sources",
      "similarweb_referrals",
      "similarweb_search_keywords"
    ],

    erd: "/static/images/logos/similarweb.png",

    description: "Collects SimilarWeb domain analytics and traffic source metrics.",

    data: [
      "Visits and engagement",
      "Channel traffic shares",
      "Social source shares",
      "Search keywords",
      "CPC and traffic share"
    ]
  },

  {
    id: "x",
    name: "X (Twitter)",
    categories: ["social"],
    logo: "/static/images/logos/x.png",

    auth_type: "oauth",

    connect_url: "http://localhost:4000/connectors/x/connect",
    sync_url: "http://localhost:4000/connectors/x/sync",
    dashboard: "/dashboard/x",

    long_description: `
Connect your X account using OAuth 2.0 and ingest profile, follower,
and recent tweet data with secure token refresh support.
  `,

    steps: [
      { title: "Provide App Credentials", desc: "Enter X client ID and client secret." },
      { title: "Authorize Access", desc: "Complete OAuth 2.0 authorization." },
      { title: "Run Sync", desc: "Profile, followers and recent tweets are ingested." }
    ],

    tables: [
      "x_users",
      "x_tweets"
    ],

    erd: "/static/images/logos/x.png",

    description: "Collects X user profiles, followers and recent tweets.",

    data: [
      "User profile fields",
      "Follower accounts",
      "Recent tweets",
      "Engagement metrics",
      "Incremental sync state"
    ]
  },

  {
    id: "linkedin",
    name: "LinkedIn Marketing",
    categories: ["social", "ads"],
    logo: "/static/images/logos/linkedin.png",

    auth_type: "oauth",

    connect_url: "http://localhost:4000/connectors/linkedin/connect",
    sync_url: "http://localhost:4000/connectors/linkedin/sync",
    dashboard: "/dashboard/linkedin",

    long_description: `
Connect your LinkedIn Marketing account using OAuth 2.0 and ingest
ad accounts, campaigns, creatives, and ad analytics with refresh-token support.
  `,

    steps: [
      { title: "Provide App Credentials", desc: "Enter LinkedIn client ID and client secret." },
      { title: "Authorize Access", desc: "Complete LinkedIn OAuth 2.0 authorization." },
      { title: "Run Sync", desc: "Ad accounts, campaigns, creatives, and analytics are ingested." }
    ],

    tables: [
      "linkedin_ad_accounts",
      "linkedin_campaigns",
      "linkedin_creatives",
      "linkedin_ad_analytics"
    ],

    erd: "/static/images/logos/linkedin.png",

    description: "Collects LinkedIn ad accounts, campaigns, creatives and performance analytics.",

    data: [
      "Ad account metadata",
      "Campaign configuration",
      "Creative metadata",
      "Impressions and clicks",
      "Cost metrics by pivot"
    ]
  },

  /* ================= DEV ================= */

  {
    id: "github",
    name: "GitHub",
    categories: ["developer", "git"],
    logo: "/static/images/logos/github.png",

    auth_type: "oauth",

    connect_url: "http://localhost:4000/github/connect",
    sync_url: "http://localhost:4000/github/sync/repos",
    dashboard: "/dashboard/github",

    long_description: `
Analyzes repositories, commits, and issues
to provide engineering productivity insights.
  `,

    steps: [
      { title: "Authorize GitHub", desc: "Grant repository access." },
      { title: "Fetch Repos", desc: "Repositories loaded." },
      { title: "Sync Activity", desc: "Commits and issues stored." }
    ],

    tables: [
      "github_repos",
      "github_commits",
      "github_issues",
      "github_state"
    ],

    erd: "/static/images/erd/github_erd.png",

    description: "Analyzes repositories and code activity.",

    data: [
      "Repositories",
      "Commits",
      "Issues",
      "Stars",
      "Contributors"
    ]
  },

  {
    id: "gitlab",
    name: "GitLab",
    categories: ["developer", "git"],
    logo: "/static/images/logos/gitlab.png",

    auth_type: "oauth",

    connect_url: "http://localhost:4000/gitlab/connect",
    sync_url: "http://localhost:4000/gitlab/sync/projects",
    dashboard: "/dashboard/gitlab",

    long_description: `
Tracks GitLab projects, pipelines, and merge requests
for DevOps and workflow analytics.
  `,

    steps: [
      { title: "Authorize GitLab", desc: "Grant project access." },
      { title: "Load Projects", desc: "Projects discovered." },
      { title: "Sync Pipelines", desc: "Workflow data stored." }
    ],

    tables: [
      "gitlab_projects",
      "gitlab_commits",
      "gitlab_issues",
      "gitlab_merge_requests",
      "gitlab_state"
    ],

    erd: "/static/images/erd/gitlab_erd.png",

    description: "Tracks projects and development workflows.",

    data: [
      "Projects",
      "Commits",
      "Issues",
      "Merge requests",
      "Pipelines"
    ]
  },

  {
    id: "devto",
    name: "Dev.to",
    categories: ["developer", "content"],
    logo: "/static/images/logos/devto.png",

    auth_type: "api_key",

    connect_url: null,
    sync_url: "http://localhost:4000/devto/sync/articles",
    dashboard: "/dashboard/devto",

    long_description: `
Monitors developer articles, reactions, and engagement
on Dev.to to analyze content performance and trends.
  `,

    steps: [
      { title: "Configure API Key", desc: "Set Dev.to API key." },
      { title: "Fetch Articles", desc: "Articles downloaded." },
      { title: "Sync Engagement", desc: "Reactions and comments stored." }
    ],

    tables: [
      "devto_articles",
      "devto_comments",
      "devto_reactions",
      "devto_state"
    ],

    erd: "/static/images/erd/devto_erd.png",

    description: "Monitors developer content performance.",

    data: [
      "Articles",
      "Likes",
      "Comments",
      "Tags",
      "Authors"
    ]
  },

  {
    id: "stackoverflow",
    name: "StackOverflow",
    categories: ["developer"],
    logo: "/static/images/logos/stackoverflow.png",

    auth_type: "api_key",

    connect_url: null,
    sync_url: "http://localhost:4000/stackoverflow/sync/questions",
    dashboard: "/dashboard/stackoverflow",

    long_description: `
Tracks questions, answers, and voting patterns
to analyze developer knowledge trends.
  `,

    steps: [
      { title: "Configure API Key", desc: "Set StackExchange key." },
      { title: "Fetch Questions", desc: "Questions loaded." },
      { title: "Sync Answers", desc: "Answers and votes stored." }
    ],

    tables: [
      "stack_questions",
      "stack_answers",
      "stack_users",
      "stack_state"
    ],

    erd: "/static/images/erd/stack_erd.png",

    description: "Tracks Q&A trends and developer activity.",

    data: [
      "Questions",
      "Answers",
      "Votes",
      "Tags",
      "Users"
    ]
  },

  {
    id: "hackernews",
    name: "HackerNews",
    categories: ["developer", "content"],
    logo: "/static/images/logos/hackernews.png",

    auth_type: "none",

    connect_url: null,
    sync_url: "http://localhost:4000/hackernews/sync",
    dashboard: "/dashboard/hackernews",

    long_description: `
Monitors Hacker News stories and discussions
to track technology and startup trends.
  `,

    steps: [
      { title: "Start Sync", desc: "Trigger HN crawler." },
      { title: "Fetch Stories", desc: "Top stories loaded." },
      { title: "Store Comments", desc: "Threads indexed." }
    ],

    tables: [
      "hackernews_stories",
      "hackernews_comments",
      "hackernews_state"
    ],

    erd: "/static/images/erd/hackernews_erd.png",

    description: "Monitors tech news discussions.",

    data: [
      "Stories",
      "Comments",
      "Scores",
      "Authors",
      "Links"
    ]
  },

  {
    id: "nvd",
    name: "NVD",
    categories: ["developer", "security"],
    logo: "/static/images/logos/nvd.png",

    auth_type: "api_key",

    connect_url: null,
    sync_url: "http://localhost:4000/nvd/sync",
    dashboard: "/dashboard/nvd",

    long_description: `
Tracks CVE vulnerability disclosures
from the National Vulnerability Database
for security monitoring.
  `,

    steps: [
      { title: "Configure API Key", desc: "Set NVD API key." },
      { title: "Fetch CVEs", desc: "Vulnerability data loaded." },
      { title: "Update State", desc: "Sync progress stored." }
    ],

    tables: [
      "nvd_cves",
      "nvd_products",
      "nvd_state"
    ],

    erd: "/static/images/erd/nvd_erd.png",

    description: "Tracks vulnerability disclosures.",

    data: [
      "CVEs",
      "Severity",
      "Products",
      "Versions",
      "Patches"
    ]
  },


  /* ================= SOCIAL ================= */

  {
    id: "reddit",
    name: "Reddit",
    categories: ["social"],
    logo: "/static/images/logos/reddit.png",

    auth_type: "oauth",

    connect_url: "http://localhost:4000/reddit/connect",
    sync_url: "http://localhost:4000/reddit/sync",
    dashboard: "/dashboard/reddit",

    long_description: `
Analyzes subreddit activity, posts, and comments
to measure community engagement and sentiment.
  `,

    steps: [
      { title: "Authorize Reddit", desc: "Grant account access." },
      { title: "Fetch Subreddits", desc: "Subscribed subs loaded." },
      { title: "Sync Posts", desc: "Posts and comments stored." }
    ],

    tables: [
      "reddit_accounts",
      "reddit_profiles",
      "reddit_posts",
      "reddit_comments",
      "reddit_state"
    ],

    erd: "/static/images/erd/reddit_erd.png",

    description: "Analyzes community engagement.",

    data: [
      "Posts",
      "Comments",
      "Upvotes",
      "Subreddits",
      "Users"
    ]
  },

  {
    id: "discord",
    name: "Discord",
    categories: ["social", "messaging"],
    logo: "/static/images/logos/discord.png",

    auth_type: "bot",

    connect_url: null,
    sync_url: "http://localhost:4000/discord/sync/guilds",
    dashboard: "/dashboard/discord",

    long_description: `
Tracks Discord server messages and activity
using bot-based integration.
  `,

    steps: [
      { title: "Add Bot", desc: "Invite bot to server." },
      { title: "Authorize Permissions", desc: "Grant read access." },
      { title: "Sync Messages", desc: "Channel data stored." }
    ],

    tables: [
      "discord_guilds",
      "discord_channels",
      "discord_messages",
      "discord_state"
    ],

    erd: "/static/images/erd/discord_erd.png",

    description: "Tracks server activity and engagement.",

    data: [
      "Messages",
      "Members",
      "Channels",
      "Reactions",
      "Roles"
    ]
  },

  {
    id: "telegram",
    name: "Telegram",
    categories: ["social", "messaging"],
    logo: "/static/images/logos/telegram.png",

    auth_type: "bot_token",

    connect_url: null,
    sync_url: "http://localhost:4000/telegram/sync/channel",
    dashboard: "/dashboard/telegram",

    long_description: `
Collects Telegram channel messages and engagement
using bot token authentication.
  `,

    steps: [
      { title: "Create Bot", desc: "Generate Telegram bot token." },
      { title: "Join Channel", desc: "Add bot to channel." },
      { title: "Sync Messages", desc: "Messages indexed." }
    ],

    tables: [
      "telegram_channels",
      "telegram_messages",
      "telegram_state"
    ],

    erd: "/static/images/erd/telegram_erd.png",

    description: "Collects channel analytics.",

    data: [
      "Messages",
      "Views",
      "Subscribers",
      "Forwards",
      "Reactions"
    ]
  },

  {
    id: "tumblr",
    name: "Tumblr",
    categories: ["social"],
    logo: "/static/images/logos/tumblr.png",

    auth_type: "oauth",

    connect_url: "http://localhost:4000/tumblr/connect",
    sync_url: "http://localhost:4000/tumblr/sync",
    dashboard: "/dashboard/tumblr",

    long_description: `
Tracks Tumblr blogs, posts, and engagement
to analyze content reach.
  `,

    steps: [
      { title: "Authorize Tumblr", desc: "Grant blog access." },
      { title: "Fetch Blogs", desc: "User blogs loaded." },
      { title: "Sync Posts", desc: "Posts and likes stored." }
    ],

    tables: [
      "tumblr_blogs",
      "tumblr_posts",
      "tumblr_notes",
      "tumblr_state"
    ],

    erd: "/static/images/erd/tumblr_erd.png",

    description: "Tracks blog engagement.",

    data: [
      "Posts",
      "Likes",
      "Reblogs",
      "Tags",
      "Authors"
    ]
  },

  {
    id: "medium",
    name: "Medium",
    categories: ["social", "content"],
    logo: "/static/images/logos/medium.png",

    auth_type: "oauth",

    connect_url: "http://localhost:4000/medium/connect",
    sync_url: "http://localhost:4000/medium/sync",
    dashboard: "/dashboard/medium",

    long_description: `
Analyzes Medium articles, followers, and engagement
to measure content impact.
  `,

    steps: [
      { title: "Authorize Medium", desc: "Grant account access." },
      { title: "Fetch Publications", desc: "Publications loaded." },
      { title: "Sync Articles", desc: "Stories indexed." }
    ],

    tables: [
      "medium_users",
      "medium_posts",
      "medium_stats",
      "medium_state"
    ],

    erd: "/static/images/erd/medium_erd.png",

    description: "Analyzes article reach.",

    data: [
      "Reads",
      "Claps",
      "Responses",
      "Followers",
      "Tags"
    ]
  },

  {
    id: "mastodon",
    name: "Mastodon",
    categories: ["social"],
    logo: "/static/images/logos/mastodon.png",

    auth_type: "oauth",

    connect_url: "http://localhost:4000/mastodon/connect",
    sync_url: "http://localhost:4000/mastodon/sync",
    dashboard: "/dashboard/mastodon",

    long_description: `
Tracks federated social interactions across Mastodon
instances for decentralized social analytics.
  `,

    steps: [
      { title: "Authorize Instance", desc: "Login to Mastodon server." },
      { title: "Fetch Timeline", desc: "Timelines loaded." },
      { title: "Sync Toots", desc: "Posts and boosts stored." }
    ],

    tables: [
      "mastodon_accounts",
      "mastodon_statuses",
      "mastodon_favourites",
      "mastodon_state"
    ],

    erd: "/static/images/erd/mastodon_erd.png",

    description: "Tracks federated social activity.",

    data: [
      "Toots",
      "Boosts",
      "Favorites",
      "Users",
      "Instances"
    ]
  },

  {
    id: "lemmy",
    name: "Lemmy",
    categories: ["social", "community"],
    logo: "/static/images/logos/lemmy.png",

    auth_type: "none",

    connect_url: null,
    sync_url: "http://localhost:4000/lemmy/sync",
    dashboard: "/dashboard/lemmy",

    long_description: `
Analyzes federated Lemmy communities and discussions
to understand decentralized social engagement.
  `,

    steps: [
      { title: "Configure Instance", desc: "Select Lemmy instance." },
      { title: "Fetch Communities", desc: "Communities loaded." },
      { title: "Sync Posts", desc: "Posts and votes stored." }
    ],

    tables: [
      "lemmy_communities",
      "lemmy_posts",
      "lemmy_comments",
      "lemmy_votes",
      "lemmy_state"
    ],

    erd: "/static/images/erd/lemmy_erd.png",

    description: "Analyzes federated communities.",

    data: [
      "Posts",
      "Communities",
      "Votes",
      "Users",
      "Comments"
    ]
  },

  {
    id: "pinterest",
    name: "Pinterest",
    categories: ["social"],
    logo: "/static/images/logos/pinterest.png",

    auth_type: "oauth",

    connect_url: "http://localhost:4000/pinterest/connect",
    sync_url: "http://localhost:4000/pinterest/sync",
    dashboard: "/dashboard/pinterest",

    long_description: `
Tracks Pinterest boards and pins to analyze
visual content performance and trends.
  `,

    steps: [
      { title: "Authorize Pinterest", desc: "Grant board access." },
      { title: "Fetch Boards", desc: "Boards loaded." },
      { title: "Sync Pins", desc: "Pins indexed." }
    ],

    tables: [
      "pinterest_tokens",
      "pinterest_boards",
      "pinterest_pins",
      "pinterest_state"
    ],

    erd: "/static/images/erd/pinterest_erd.png",

    description: "Tracks pins and boards.",

    data: [
      "Boards",
      "Pins",
      "Saves",
      "Views",
      "Links"
    ]
  },

  {
    id: "twitch",
    name: "Twitch",
    categories: ["video"],
    logo: "/static/images/logos/twitch.png",

    auth_type: "oauth",

    connect_url: "http://localhost:4000/twitch/connect",
    sync_url: "http://localhost:4000/twitch/sync",
    dashboard: "/dashboard/twitch",

    long_description: `
Monitors Twitch channels, streams, and chat activity
for live streaming analytics.
  `,

    steps: [
      { title: "Authorize Twitch", desc: "Grant channel access." },
      { title: "Fetch Streams", desc: "Stream metadata loaded." },
      { title: "Sync Chats", desc: "Chat messages stored." }
    ],

    tables: [
      "twitch_channels",
      "twitch_streams",
      "twitch_chats",
      "twitch_clips",
      "twitch_state"
    ],

    erd: "/static/images/erd/twitch_erd.png",

    description: "Monitors live streaming analytics.",

    data: [
      "Streams",
      "Viewers",
      "Followers",
      "Chats",
      "Clips"
    ]
  },

  {
    id: "peertube",
    name: "PeerTube",
    categories: ["video"],
    logo: "/static/images/logos/peertube.png",

    auth_type: "none",

    connect_url: null,
    sync_url: "http://localhost:4000/peertube/sync",
    dashboard: "/dashboard/peertube",

    long_description: `
Tracks decentralized video content across PeerTube
instances for federated media analytics.
  `,

    steps: [
      { title: "Configure Instance", desc: "Select PeerTube server." },
      { title: "Fetch Channels", desc: "Channels loaded." },
      { title: "Sync Videos", desc: "Videos indexed." }
    ],

    tables: [
      "peertube_channels",
      "peertube_videos",
      "peertube_comments",
      "peertube_state"
    ],

    erd: "/static/images/erd/peertube_erd.png",

    description: "Tracks decentralized video content.",

    data: [
      "Videos",
      "Views",
      "Channels",
      "Comments",
      "Tags"
    ]
  },


  /* ================= OTHER ================= */

  {
    id: "openstreetmap",
    name: "OpenStreetMap",
    categories: ["maps"],
    logo: "/static/images/logos/openstreetmap.png",

    auth_type: "none",

    connect_url: null,
    sync_url: "http://localhost:4000/osm/sync",
    dashboard: "/dashboard/openstreetmap",

    long_description: `
Collects and analyzes geospatial data from
OpenStreetMap for mapping and location services.
  `,

    steps: [
      { title: "Configure Region", desc: "Select geographic area." },
      { title: "Fetch Map Data", desc: "OSM data downloaded." },
      { title: "Normalize GeoData", desc: "Coordinates processed." }
    ],

    tables: [
      "osm_roads",
      "osm_buildings",
      "osm_pois",
      "osm_regions",
      "osm_state"
    ],

    erd: "/static/images/erd/osm_erd.png",

    description: "Collects geospatial map data.",

    data: [
      "Roads",
      "Buildings",
      "POIs",
      "Coordinates",
      "Regions"
    ]
  },

  {
    id: "wikipedia",
    name: "Wikipedia",
    categories: ["content"],
    logo: "/static/images/logos/wikipedia.png",

    auth_type: "none",

    connect_url: null,
    sync_url: "http://localhost:4000/wikipedia/sync",
    dashboard: "/dashboard/wikipedia",

    long_description: `
Analyzes Wikipedia articles and revisions
to track knowledge updates and editor activity.
  `,

    steps: [
      { title: "Start Sync", desc: "Trigger Wikipedia crawler." },
      { title: "Fetch Articles", desc: "Pages loaded." },
      { title: "Track Revisions", desc: "Edits indexed." }
    ],

    tables: [
      "wikipedia_pages",
      "wikipedia_revisions",
      "wikipedia_editors",
      "wikipedia_state"
    ],

    erd: "/static/images/erd/wikipedia_erd.png",

    description: "Analyzes encyclopedia content.",

    data: [
      "Articles",
      "Revisions",
      "Editors",
      "Categories",
      "Links"
    ]
  },

  {
    id: "producthunt",
    name: "ProductHunt",
    categories: ["content", "community"],
    logo: "/static/images/logos/producthunt.png",

    auth_type: "api_key",

    connect_url: null,
    sync_url: "http://localhost:4000/producthunt/sync",
    dashboard: "/dashboard/producthunt",

    long_description: `
Tracks new product launches, votes, and makers
on Product Hunt for startup analytics.
  `,

    steps: [
      { title: "Configure API Token", desc: "Set Product Hunt token." },
      { title: "Fetch Products", desc: "Daily launches loaded." },
      { title: "Sync Votes", desc: "Engagement stored." }
    ],

    tables: [
      "producthunt_products",
      "producthunt_votes",
      "producthunt_users",
      "producthunt_state"
    ],

    erd: "/static/images/erd/producthunt_erd.png",

    description: "Tracks startup launches.",

    data: [
      "Products",
      "Votes",
      "Comments",
      "Makers",
      "Categories"
    ]
  },

  {
    id: "discourse",
    name: "Discourse",
    categories: ["community"],
    logo: "/static/images/logos/discourse.png",

    auth_type: "api_key",

    connect_url: null,
    sync_url: "http://localhost:4000/discourse/sync",
    dashboard: "/dashboard/discourse",

    long_description: `
Analyzes forum topics, posts, and user engagement
from Discourse communities.
  `,

    steps: [
      { title: "Configure API Key", desc: "Set Discourse key." },
      { title: "Fetch Topics", desc: "Topics loaded." },
      { title: "Sync Posts", desc: "Posts indexed." }
    ],

    tables: [
      "discourse_topics",
      "discourse_posts",
      "discourse_users",
      "discourse_state"
    ],

    erd: "/static/images/erd/discourse_erd.png",

    description: "Analyzes forum discussions.",

    data: [
      "Topics",
      "Posts",
      "Users",
      "Tags",
      "Categories"
    ]
  },

  {
    id: "whatsapp",
    name: "WhatsApp",
    categories: ["meta", "messaging"],
    route: "/connectors/whatsapp",
    logo: "/static/images/logos/whatsapp.png",
    tables: [
      "whatsapp_business_accounts",
      "whatsapp_phone_numbers",
      "whatsapp_message_templates",
      "whatsapp_conversation_analytics",
      "whatsapp_message_insights"
    ],
    description: "Connect and analyze WhatsApp Business accounts, phone numbers, and messaging insights.",
    long_description: `
    Integrate with WhatsApp Cloud API to sync business accounts, phone numbers, 
    message templates, and conversation analytics directly to your warehouse.
  `,
    steps: [
      { title: "Provide Credentials", desc: "Enter Access Token, WABA ID, and Phone ID." },
      { title: "Establish Connection", desc: "Validate credentials with Meta Graph API." },
      { title: "Automated Sync", desc: "Messaging metrics and analytics are synchronized." }
    ],
    data: [
      "Business accounts",
      "Phone numbers",
      "Message templates",
      "Conversation analytics",
      "Messaging insights"
    ]
  },

  // ================================================================
  // CHARTBEAT CONNECTOR
  // ================================================================
  {
    id: "chartbeat",
    name: "Chartbeat",
    categories: ["analytics", "content"],
    logo: "/static/images/logos/chartbeat.png",

    auth_type: "api_key",

    route: "/connectors/chartbeat",
    connect_url: "http://localhost:4000/connectors/chartbeat/connect",
    sync_url: "http://localhost:4000/connectors/chartbeat/sync",
    dashboard: "/dashboard/chartbeat",

    long_description: `
Connect Chartbeat using your API Key to ingest real-time top pages,
historical page engagement analytics, recurring query results, and
video engagement metrics directly into your data warehouse.
    `,

    steps: [
      { title: "Provide API Key", desc: "Enter your Chartbeat API Key and site domain." },
      { title: "Validate Access", desc: "Credentials are validated against the Chartbeat live endpoint." },
      { title: "Run Sync", desc: "Real-time pages, engagement reports, and video data are ingested." }
    ],

    tables: [
      "chartbeat_top_pages",
      "chartbeat_page_engagement",
      "chartbeat_video_engagement"
    ],

    erd: "/static/images/logos/chartbeat.png",

    description: "Collects real-time audience analytics and historical engagement data from Chartbeat.",

    data: [
      "Real-time top pages & concurrents",
      "Page views, uniques & engaged time",
      "Author, section & device breakdowns",
      "Referrer type analytics",
      "Video plays, loads & play rate"
    ]
  },

  {
    id: "socialinsider",
    name: "Social Insider",
    categories: ["social", "analytics"],
    logo: "https://www.socialinsider.io/favicon.ico",

    auth_type: "api_key",

    route: "/connectors/socialinsider",
    connect_url: "http://localhost:4000/connectors/socialinsider/connect",
    sync_url: "http://localhost:4000/connectors/socialinsider/sync",

    long_description: `
Connect Social Insider using your API Key to ingest social media posts, 
engagement metrics, and detailed profile insights across various platforms 
directly into your data warehouse.
    `,

    steps: [
      { title: "Provide API Key", desc: "Enter your Social Insider API Key, platform, and handle." },
      { title: "Validate Access", desc: "Validate access to the specified profile via Social Insider API." },
      { title: "Run Sync", desc: "Social media posts and profile insights are ingested into your warehouse." }
    ],

    tables: [
      "socialinsider_posts",
      "socialinsider_profile_insights"
    ],

    description: "Fetches social media posts and profile insights across multiple platforms using Social Insider.",

    data: [
      "Post engagement & reach",
      "Content type analytics",
      "Follower growth & demographics",
      "Industry benchmarks",
      "Historical social data"
    ]
  },

  {
    id: "aws_rds",
    name: "AWS RDS",
    categories: ["cloud", "database"],
    logo: "/static/images/logos/aws_rds.png",

    auth_type: "credentials",

    route: "/connectors/aws_rds",
    connect_url: "/connectors/aws_rds/connect",
    sync_url: "/connectors/aws_rds/sync",
    dashboard: "/dashboard/aws_rds",

    long_description: `
Connect directly to your Amazon RDS or Aurora database instance.
Segmento automatically discovers all tables, extracts rows in
configurable batches, and streams the data into your destination
warehouse. Supports MySQL, MariaDB, Aurora MySQL, PostgreSQL,
and Aurora PostgreSQL engines.
    `,

    steps: [
      { title: "Enter Credentials", desc: "Provide your RDS endpoint, engine, port, database name, username, and password." },
      { title: "Validate Connection", desc: "Segmento opens a live connection and runs a lightweight ping against your instance." },
      { title: "Account Linked", desc: "All tables are discovered automatically and rows are synced to your warehouse." }
    ],

    tables: [
      "aws_rds_sync (mirrors source tables dynamically)"
    ],

    description: "Extracts all tables from an Amazon RDS or Aurora instance and loads them into your warehouse.",

    data: [
      "All user tables (auto-discovered)",
      "Full row extraction with batch pagination",
      "MySQL, MariaDB, Aurora MySQL support",
      "PostgreSQL, Aurora PostgreSQL support",
      "Incremental and historical sync modes"
    ]
  },

  {
    id: "stripe",
    name: "Stripe",
    categories: ["finance", "payments"],
    logo: "/static/images/logos/stripe.png",

    auth_type: "credentials",

    route: "/connectors/stripe",
    connect_url: "/connectors/stripe/connect",
    sync_url: "/connectors/stripe/sync",
    dashboard: "/dashboard/stripe",

    long_description: `
Connect Stripe using your Secret API Key. Segmento validates access to your Stripe account,
then paginates through customers, charges, subscriptions, and products and streams normalized
JSON rows into your configured warehouse destination.
    `,

    steps: [
      { title: "Enter Secret Key", desc: "Provide your Stripe Secret API Key." },
      { title: "Validate Access", desc: "Segmento verifies the key against the Stripe REST API." },
      { title: "Run Sync", desc: "Customers, charges, subscriptions, and products are extracted." }
    ],

    tables: [
      "stripe_customers",
      "stripe_charges",
      "stripe_subscriptions",
      "stripe_products"
    ],

    description: "Extracts Stripe billing and catalog data and loads normalized JSON rows into your warehouse.",

    data: [
      "Customers",
      "Charges",
      "Subscriptions",
      "Products",
      "Normalized JSON payloads"
    ]
  },

  {
    id: "dynamodb",
    name: "AWS DynamoDB",
    categories: ["cloud", "database"],
    logo: "/static/images/logos/dynamodb.png",

    auth_type: "credentials",

    route: "/connectors/dynamodb",
    connect_url: "/connectors/dynamodb/connect",
    sync_url: "/connectors/dynamodb/sync",
    dashboard: "/dashboard/dynamodb",

    long_description: `
Connect directly to Amazon DynamoDB using AWS credentials and a target region.
Segmento lists all tables in the region, scans items page by page, converts
each record into normalized JSON, and streams both table metadata and item
data into your configured warehouse destination.
    `,

    steps: [
      { title: "Enter Credentials", desc: "Provide your AWS Access Key, AWS Secret Key, and region." },
      { title: "Validate Access", desc: "Segmento verifies DynamoDB access by listing tables in the selected region." },
      { title: "Run Sync", desc: "Table metadata and scanned item payloads are pushed into your warehouse." }
    ],

    tables: [
      "dynamodb_tables",
      "dynamodb_data"
    ],

    description: "Scans DynamoDB tables and items from a selected AWS region and loads them into your warehouse.",

    data: [
      "DynamoDB table metadata",
      "Scanned table items with pagination",
      "Primary-key based record identifiers",
      "Normalized JSON payloads",
      "Incremental and historical sync modes"
    ]
  },

  {
    id: "looker",
    name: "Looker",
    categories: ["analytics", "bi"],
    logo: "https://cdn.simpleicons.org/looker/4285F4",

    auth_type: "api_key",
    api_key_label: "Looker API3 Credentials",

    connect_url: "/connectors/looker/connect",
    sync_url: "/connectors/looker/sync",
    disconnect_url: "/connectors/looker/disconnect",
    status_api: "/api/status/looker",
    save_app_url: "/connectors/looker/save_app",

    models: [{ title: "Users", desc: "User records" }, { title: "Dashboards", desc: "Dashboards" }, { title: "Looks", desc: "Saved looks" }],

    tables: [
      "looker_users",
      "looker_dashboards",
      "looker_looks"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Connect your Looker instance to extract users, dashboards, and looks into your destination warehouse.",

    data: ["Dashboards", "Looks", "User records"]
  },

  {
    id: "superset",
    name: "Apache Superset",
    categories: ["analytics", "bi"],
    logo: "https://cdn.simpleicons.org/apachesuperset/20A7C9",

    auth_type: "api_key",
    api_key_label: "Superset Credentials",

    connect_url: "/connectors/superset/connect",
    sync_url: "/connectors/superset/sync",
    disconnect_url: "/connectors/superset/disconnect",
    status_api: "/api/status/superset",
    save_app_url: "/connectors/superset/save_app",

    models: [{ title: "Dashboards", desc: "Dashboards" }, { title: "Charts", desc: "Charts" }, { title: "Datasets", desc: "Datasets" }],

    tables: [
      "superset_dashboards",
      "superset_charts",
      "superset_datasets"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Connect your Superset instance to extract dashboards, charts, and datasets into your destination warehouse.",

    data: ["Dashboards", "Charts", "Datasets"]
  },

  {
    id: "azure_blob",
    name: "Azure Blob Storage",
    categories: ["storage", "data"],
    logo: "/static/images/logos/azureblob.png",

    auth_type: "api_key",
    api_key_label: "Azure Connection String",

    connect_url: "/connectors/azure_blob/connect",
    sync_url: "/connectors/azure_blob/sync",
    disconnect_url: "/connectors/azure_blob/disconnect",
    status_api: "/api/status/azure_blob",
    save_app_url: "/connectors/azure_blob/save_app",

    models: [{ title: "Files", desc: "Blob files" }],

    tables: [
      "azure_blob_files"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Connect Azure Blob Storage to map and extract file metadata.",

    data: ["Blob files", "File sizes", "Timestamps"]
  },

  {
    id: "datadog",
    name: "Datadog",
    categories: ["monitoring", "analytics"],
    logo: "https://cdn.simpleicons.org/datadog/632CA6",

    auth_type: "api_key",
    api_key_label: "Datadog API and App Keys",

    connect_url: "/connectors/datadog/connect",
    sync_url: "/connectors/datadog/sync",
    disconnect_url: "/connectors/datadog/disconnect",
    status_api: "/api/status/datadog",
    save_app_url: "/connectors/datadog/save_app",

    models: [{ title: "Events", desc: "Datadog events" }],

    tables: [
      "datadog_events"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Connect to your Datadog instance to extract events, metrics, and logs into your destination warehouse.",

    data: ["Events", "Logs", "Metrics"]
  },

  {
    id: "okta",
    name: "Okta",
    categories: ["security", "identity"],
    logo: "https://cdn.simpleicons.org/okta/007DC1",

    auth_type: "api_key",
    api_key_label: "Okta API Token",

    route: "/connectors/okta",
    connect_url: "/connectors/okta/connect",
    sync_url: "/connectors/okta/sync",
    disconnect_url: "/connectors/okta/disconnect",
    status_api: "/api/status/okta",
    save_app_url: "/connectors/okta/save_app",

    models: [
      {title: "Users", desc: "Okta user accounts and profile data"},
      {title: "Groups", desc: "Okta groups and memberships"},
      {title: "Apps", desc: "Integrated application assignments"}
    ],

    tables: [
      "okta_users",
      "okta_groups",
      "okta_apps"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Connect your Okta tenant to extract users, groups, and application data for identity analytics and compliance reporting.",

    data: ["Users", "Groups", "Apps", "Login metadata", "Profile attributes"]
  },

  {
    id: "auth0",
    name: "Auth0",
    categories: ["security", "identity"],
    logo: "https://cdn.simpleicons.org/auth0/EB5424",

    auth_type: "api_key",
    api_key_label: "Auth0 Management API Token",

    route: "/connectors/auth0",
    connect_url: "/connectors/auth0/connect",
    sync_url: "/connectors/auth0/sync",
    disconnect_url: "/connectors/auth0/disconnect",
    status_api: "/api/status/auth0",
    save_app_url: "/connectors/auth0/save_app",

    models: [
      {title: "Users", desc: "Auth0 user accounts and profile data"},
      {title: "Roles", desc: "Authentication roles and permissions"},
      {title: "Logs", desc: "Authentication event logs"}
    ],

    tables: [
      "auth0_users",
      "auth0_roles",
      "auth0_logs"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Connect your Auth0 tenant to extract user accounts, roles, and authentication logs into your destination warehouse.",

    data: ["Users", "Roles", "Logs", "Login counts", "Last login timestamps"]
  },

  {
    id: "cloudflare",
    name: "Cloudflare",
    categories: ["infrastructure", "networking"],
    logo: "https://cdn.simpleicons.org/cloudflare/F38020",

    auth_type: "api_key",
    api_key_label: "Cloudflare API Token",

    route: "/connectors/cloudflare",
    connect_url: "/connectors/cloudflare/connect",
    sync_url: "/connectors/cloudflare/sync",
    disconnect_url: "/connectors/cloudflare/disconnect",
    status_api: "/api/status/cloudflare",
    save_app_url: "/connectors/cloudflare/save_app",

    models: [
      {title: "Zones", desc: "Cloudflare DNS zones and domains"},
      {title: "DNS Records", desc: "DNS record entries across all zones"},
      {title: "Analytics", desc: "Web traffic and analytics data"}
    ],

    tables: [
      "cloudflare_zones",
      "cloudflare_dns_records",
      "cloudflare_analytics"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Connect your Cloudflare account to extract zones, DNS records, and web analytics into your destination warehouse.",

    data: ["Zones", "DNS Records", "Traffic Analytics", "Threat counts", "Bandwidth metrics"]
  },

  {
    id: "sentry",
    name: "Sentry",
    categories: ["monitoring", "engineering"],
    logo: "https://cdn.simpleicons.org/sentry/362D59",

    auth_type: "api_key",
    api_key_label: "Sentry Auth Token",

    route: "/connectors/sentry",
    connect_url: "/connectors/sentry/connect",
    sync_url: "/connectors/sentry/sync",
    disconnect_url: "/connectors/sentry/disconnect",
    status_api: "/api/status/sentry",
    save_app_url: "/connectors/sentry/save_app",

    models: [
      {title: "Projects", desc: "Sentry monitored projects"},
      {title: "Issues", desc: "Error issues and exceptions"},
      {title: "Events", desc: "Individual error event occurrences"}
    ],

    tables: [
      "sentry_projects",
      "sentry_issues",
      "sentry_events"
    ],

    erd: "/static/images/empty_erd.png",

    description: "Connect your Sentry organization to extract projects, issues, and error events for engineering analytics and release quality tracking.",

    data: ["Projects", "Issues", "Events", "Error levels", "Stack traces"]
  },

];
