const CONNECTORS = [

/* ================= GOOGLE ================= */

{
  id:"gmail",
  name:"Gmail",
  category:"google",
  logo:"/static/images/logos/gmail.png",

  auth_type:"oauth",

  connect_url:"http://localhost:4000/google/connect",
  sync_url:"http://localhost:4000/google/sync/gmail",
  dashboard:"/dashboard/gmail",

  long_description:`
Connect and analyze your Gmail account to understand communication
patterns, customer interactions, and productivity trends.
All metadata and message headers are securely synchronized.
  `,

  steps:[
    {title:"Authorize Google",desc:"Login and grant Gmail read access."},
    {title:"Secure Token Storage",desc:"OAuth tokens stored in database."},
    {title:"Initial Sync",desc:"All email metadata is fetched."}
  ],

  tables:[
    "google_gmail_profile",
    "google_gmail_labels",
    "google_gmail_messages",
    "google_gmail_message_details"
  ],

  erd:"/static/images/erd/gmail_erd.png",

  description:"Collects email metadata and communication patterns.",

  data:[
    "Inbox messages",
    "Senders & recipients",
    "Subjects",
    "Threads",
    "Snippets"
  ]
},

{
  id:"drive",
  name:"Google Drive",
  category:"google",
  logo:"/static/images/logos/drive.png",

  auth_type:"oauth",

  connect_url:"http://localhost:4000/google/connect",
  sync_url:"http://localhost:4000/google/sync/drive",
  dashboard:"/dashboard/drive",

  long_description:`
Indexes all files and folders in Google Drive and provides
visibility into document usage, storage patterns, and ownership.
  `,

  steps:[
    {title:"Authorize Drive",desc:"Grant Drive metadata access."},
    {title:"Scan Files",desc:"All folders and files indexed."},
    {title:"Delta Sync",desc:"Future changes tracked."}
  ],

  tables:[
    "google_drive_files",
    "google_drive_folders"
  ],

  erd:"/static/images/erd/drive_erd.png",

  description:"Indexes cloud files and folder structures.",

  data:[
    "File names",
    "Folder hierarchy",
    "File sizes",
    "Owners",
    "Timestamps"
  ]
},

{
  id:"calendar",
  name:"Google Calendar",
  category:"google",
  logo:"/static/images/logos/calendar.png",

  auth_type:"oauth",

  connect_url:"http://localhost:4000/google/connect",
  sync_url:"http://localhost:4000/google/sync/calendar",
  dashboard:"/dashboard/calendar",

  long_description:`
Synchronizes calendar events to analyze meetings,
availability, and scheduling patterns.
  `,

  steps:[
    {title:"Authorize Calendar",desc:"Grant calendar access."},
    {title:"Fetch Calendars",desc:"All calendars loaded."},
    {title:"Sync Events",desc:"Events stored with metadata."}
  ],

  tables:[
    "google_calendar_list",
    "google_calendar_events"
  ],

  erd:"/static/images/erd/calendar_erd.png",

  description:"Tracks meeting schedules and event timelines.",

  data:[
    "Event titles",
    "Attendees",
    "Schedules",
    "Recurring events",
    "Locations"
  ]
},

{
  id:"sheets",
  name:"Google Sheets",
  category:"google",
  logo:"/static/images/logos/sheets.png",

  auth_type:"oauth",

  connect_url:"http://localhost:4000/google/connect",
  sync_url:"http://localhost:4000/google/sync/sheets",
  dashboard:"/dashboard/sheets",

  long_description:`
Extracts spreadsheet structures and values
to support reporting, finance tracking, and KPI analysis.
  `,

  steps:[
    {title:"Authorize Sheets",desc:"Grant Sheets access."},
    {title:"Fetch Spreadsheets",desc:"All sheets discovered."},
    {title:"Read Values",desc:"Cell ranges downloaded."}
  ],

  tables:[
    "google_sheets",
    "google_sheet_values"
  ],

  erd:"/static/images/erd/sheets_erd.png",

  description:"Extracts spreadsheet data and metrics.",

  data:[
    "Sheet metadata",
    "Cell values",
    "Formulas",
    "Row data",
    "Timestamps"
  ]
},

{
  id:"forms",
  name:"Google Forms",
  category:"google",
  logo:"/static/images/logos/forms.png",

  auth_type:"oauth",

  connect_url:"http://localhost:4000/google/connect",
  sync_url:"http://localhost:4000/google/sync/forms",
  dashboard:"/dashboard/forms",

  long_description:`
Collects form structures and user responses
for surveys, feedback systems, and analytics pipelines.
  `,

  steps:[
    {title:"Authorize Forms",desc:"Grant Forms access."},
    {title:"Fetch Forms",desc:"All owned forms loaded."},
    {title:"Sync Responses",desc:"Responses normalized."}
  ],

  tables:[
    "google_forms",
    "google_form_responses"
  ],

  erd:"/static/images/erd/forms_erd.png",

  description:"Collects survey responses and form structures.",

  data:[
    "Questions",
    "Responses",
    "Submission time",
    "User metadata",
    "Form schema"
  ]
},

{
  id:"contacts",
  name:"Google Contacts",
  category:"google",
  logo:"/static/images/logos/contacts.png",

  auth_type:"oauth",

  connect_url:"http://localhost:4000/google/connect",
  sync_url:"http://localhost:4000/google/sync/contacts",
  dashboard:"/dashboard/contacts",

  long_description:`
Synchronizes personal and business contacts
to enrich CRM and customer databases.
  `,

  steps:[
    {title:"Authorize Contacts",desc:"Grant People API access."},
    {title:"Fetch Profiles",desc:"All contacts retrieved."},
    {title:"Normalize Data",desc:"Fields standardized."}
  ],

  tables:[
    "google_contacts_persons"
  ],

  erd:"/static/images/erd/contacts_erd.png",

  description:"Synchronizes contact and CRM information.",

  data:[
    "Names",
    "Emails",
    "Phone numbers",
    "Organizations",
    "Addresses"
  ]
},

{
  id:"tasks",
  name:"Google Tasks",
  category:"google",
  logo:"/static/images/logos/tasks.png",

  auth_type:"oauth",

  connect_url:"http://localhost:4000/google/connect",
  sync_url:"http://localhost:4000/google/sync/tasks",
  dashboard:"/dashboard/tasks",

  long_description:`
Tracks task lists and individual tasks
to analyze productivity and completion trends.
  `,

  steps:[
    {title:"Authorize Tasks",desc:"Grant Tasks API access."},
    {title:"Fetch Lists",desc:"Task lists loaded."},
    {title:"Sync Items",desc:"Task items stored."}
  ],

  tables:[
    "google_tasks_lists",
    "google_tasks_items"
  ],

  erd:"/static/images/erd/tasks_erd.png",

  description:"Tracks personal productivity tasks.",

  data:[
    "Task lists",
    "Task status",
    "Due dates",
    "Completion state",
    "Updates"
  ]
},

{
  id:"ga4",
  name:"Google Analytics (GA4)",
  category:"google",
  logo:"/static/images/logos/googleanalytics.png",

  auth_type:"oauth",

  connect_url:"http://localhost:4000/google/connect",
  sync_url:"http://localhost:4000/google/sync/ga4",
  dashboard:"/dashboard/ga4",

  long_description:`
Collects GA4 analytics reports for understanding
user behavior, traffic sources, and conversions.
  `,

  steps:[
    {title:"Authorize Analytics",desc:"Grant GA4 access."},
    {title:"Select Property",desc:"Choose analytics property."},
    {title:"Generate Reports",desc:"Metrics synchronized."}
  ],

  tables:[
    "ga4_website_overview",
    "ga4_devices",
    "ga4_locations",
    "ga4_traffic_sources",
    "ga4_events"
  ],

  erd:"/static/images/erd/ga4_erd.png",

  description:"Analyzes website traffic and user behavior.",

  data:[
    "Sessions",
    "Page views",
    "Bounce rate",
    "Traffic sources",
    "Conversions"
  ]
},

{
  id:"search-console",
  name:"Search Console",
  category:"google",
  logo:"/static/images/logos/searchconsole.png",

  auth_type:"oauth",

  connect_url:"http://localhost:4000/google/connect",
  sync_url:"http://localhost:4000/google/sync/searchconsole",
  dashboard:"/dashboard/searchconsole",

  long_description:`
Tracks keyword rankings and SEO performance
using Google Search Console reports.
  `,

  steps:[
    {title:"Authorize GSC",desc:"Grant webmaster access."},
    {title:"Select Site",desc:"Choose verified site."},
    {title:"Sync Queries",desc:"SEO data fetched."}
  ],

  tables:[
    "google_search_console"
  ],

  erd:"/static/images/erd/searchconsole_erd.png",

  description:"Monitors website SEO performance.",

  data:[
    "Search queries",
    "Impressions",
    "Clicks",
    "Ranking",
    "Index status"
  ]
},

{
  id:"youtube",
  name:"YouTube",
  category:"google",
  logo:"/static/images/logos/youtube.png",

  auth_type:"oauth",

  connect_url:"http://localhost:4000/google/connect",
  sync_url:"http://localhost:4000/google/sync/youtube",
  dashboard:"/dashboard/youtube",

  long_description:`
Collects channel and video analytics
to evaluate audience engagement and growth.
  `,

  steps:[
    {title:"Authorize YouTube",desc:"Grant YouTube read access."},
    {title:"Fetch Channel",desc:"Channel metadata loaded."},
    {title:"Sync Videos",desc:"Videos and comments stored."}
  ],

  tables:[
    "google_youtube_channels",
    "google_youtube_videos",
    "google_youtube_comments",
    "google_youtube_state"
  ],

  erd:"/static/images/erd/youtube_erd.png",

  description:"Tracks channel and video engagement.",

  data:[
    "Views",
    "Subscribers",
    "Likes",
    "Comments",
    "Watch time"
  ]
},

{
  id:"trends",
  name:"Google Trends",
  category:"google",
  logo:"/static/images/logos/googletrends.png",

  auth_type:"none",

  connect_url:null,
  sync_url:"http://localhost:4000/google/sync/trends",
  dashboard:"/dashboard/trends",

  long_description:`
Tracks keyword popularity and interest-over-time trends
to identify seasonal patterns and emerging topics.
Uses Pytrends for data collection.
  `,

  steps:[
    {title:"Configure Keywords",desc:"Select keywords to track."},
    {title:"Start Sync",desc:"Trigger Trends ingestion."},
    {title:"Time-Series Storage",desc:"Interest values saved."}
  ],

  tables:[
    "google_trends_interest",
    "google_trends_related",
    "google_trends_state"
  ],

  erd:"/static/images/erd/trends_erd.png",

  description:"Provides keyword popularity trends.",

  data:[
    "Search volume",
    "Regional trends",
    "Keywords",
    "Time series",
    "Topics"
  ]
},

{
  id:"news",
  name:"Google News",
  category:"google",
  logo:"/static/images/logos/googlenews.png",

  auth_type:"none",

  connect_url:null,
  sync_url:"http://localhost:4000/google/sync/news",
  dashboard:"/dashboard/news",

  long_description:`
Aggregates news articles using public RSS feeds
to monitor brand mentions and trending topics.
  `,

  steps:[
    {title:"Configure Keywords",desc:"Define news queries."},
    {title:"Fetch Feeds",desc:"RSS feeds parsed."},
    {title:"Store Articles",desc:"Metadata indexed."}
  ],

  tables:[
    "google_news_articles",
    "google_news_state"
  ],

  erd:"/static/images/erd/news_erd.png",

  description:"Monitors news and media mentions.",

  data:[
    "Articles",
    "Sources",
    "Publish time",
    "Topics",
    "Publishers"
  ]
},

{
  id:"books",
  name:"Google Books",
  category:"google",
  logo:"/static/images/logos/googlebooks.png",

  auth_type:"api_key",

  connect_url:null,
  sync_url:"http://localhost:4000/google/sync/books",
  dashboard:"/dashboard/books",

  long_description:`
Collects bibliographic metadata using Google Books API
to build searchable digital libraries.
  `,

  steps:[
    {title:"Configure API Key",desc:"Set Books API key."},
    {title:"Run Queries",desc:"Search by author/ISBN."},
    {title:"Store Volumes",desc:"Metadata saved."}
  ],

  tables:[
    "google_books_volumes",
    "google_books_state"
  ],

  erd:"/static/images/erd/books_erd.png",

  description:"Accesses book catalog metadata.",

  data:[
    "Titles",
    "Authors",
    "ISBN",
    "Descriptions",
    "Categories"
  ]
},

{
  id:"webfonts",
  name:"Google Webfonts",
  category:"google",
  logo:"/static/images/logos/webfonts.png",

  auth_type:"api_key",

  connect_url:null,
  sync_url:"http://localhost:4000/google/sync/webfonts",
  dashboard:"/dashboard/webfonts",

  long_description:`
Indexes Google Fonts catalog for design systems
and typography optimization.
  `,

  steps:[
    {title:"Configure API Key",desc:"Set Webfonts API key."},
    {title:"Fetch Catalog",desc:"Fonts downloaded."},
    {title:"Cache Data",desc:"Metadata stored."}
  ],

  tables:[
    "google_webfonts_fonts"
  ],

  erd:"/static/images/erd/webfonts_erd.png",

  description:"Indexes font families and assets.",

  data:[
    "Font families",
    "Variants",
    "Subsets",
    "URLs",
    "Metadata"
  ]
},

{
  id:"pagespeed",
  name:"PageSpeed",
  category:"google",
  logo:"/static/images/logos/pagespeed.png",

  auth_type:"api_key",

  connect_url:null,
  sync_url:"http://localhost:4000/google/sync/pagespeed",
  dashboard:"/dashboard/pagespeed",

  long_description:`
Measures website performance using Lighthouse reports
to optimize SEO and loading speed.
  `,

  steps:[
    {title:"Enter URL",desc:"Provide site URL."},
    {title:"Run Audit",desc:"PageSpeed analysis executed."},
    {title:"Store Results",desc:"Scores saved."}
  ],

  tables:[
    "google_pagespeed"
  ],

  erd:"/static/images/erd/pagespeed_erd.png",

  description:"Measures website performance metrics.",

  data:[
    "Load time",
    "SEO score",
    "Accessibility",
    "Performance",
    "Best practices"
  ]
},

{
  id:"gcs",
  name:"Google Cloud Storage",
  category:"google",
  logo:"/static/images/logos/gcs.png",

  auth_type:"oauth",

  connect_url:"http://localhost:4000/google/connect",
  sync_url:"http://localhost:4000/google/sync/gcs",
  dashboard:"/dashboard/gcs",

  long_description:`
Analyzes cloud storage usage, bucket metadata,
and object-level information.
  `,

  steps:[
    {title:"Authorize GCS",desc:"Grant storage access."},
    {title:"Fetch Buckets",desc:"Buckets enumerated."},
    {title:"Sync Objects",desc:"Files indexed."}
  ],

  tables:[
    "google_gcs_buckets",
    "google_gcs_objects"
  ],

  erd:"/static/images/erd/gcs_erd.png",

  description:"Analyzes cloud storage usage.",

  data:[
    "Buckets",
    "Objects",
    "Sizes",
    "Types",
    "Update times"
  ]
},

{
  id:"classroom",
  name:"Google Classroom",
  category:"google",
  logo:"/static/images/logos/classroom.png",

  auth_type:"oauth",

  connect_url:"http://localhost:4000/google/connect",
  sync_url:"http://localhost:4000/google/sync/classroom",
  dashboard:"/dashboard/classroom",

  long_description:`
Collects classroom data for education analytics
including courses, assignments, and submissions.
  `,

  steps:[
    {title:"Authorize Classroom",desc:"Grant education scopes."},
    {title:"Load Courses",desc:"Courses fetched."},
    {title:"Sync Submissions",desc:"Student data stored."}
  ],

  tables:[
    "google_classroom_courses",
    "google_classroom_teachers",
    "google_classroom_students",
    "google_classroom_announcements",
    "google_classroom_coursework",
    "google_classroom_submissions"
  ],

  erd:"/static/images/erd/classroom_erd.png",

  description:"Collects education activity data.",

  data:[
    "Courses",
    "Students",
    "Assignments",
    "Submissions",
    "Announcements"
  ]
},

{
  id:"factcheck",
  name:"Fact Check",
  category:"google",
  logo:"/static/images/logos/factcheck.png",

  auth_type:"api_key",

  connect_url:null,
  sync_url:"http://localhost:4000/google/sync/factcheck",
  dashboard:"/dashboard/factcheck",

  long_description:`
Retrieves verified claims to monitor misinformation
and validate public statements.
  `,

  steps:[
    {title:"Configure API Key",desc:"Set FactCheck API key."},
    {title:"Run Queries",desc:"Claims searched."},
    {title:"Store Reviews",desc:"Ratings saved."}
  ],

  tables:[
    "google_factcheck_claims",
    "google_factcheck_state"
  ],

  erd:"/static/images/erd/factcheck_erd.png",

  description:"Verifies claims and misinformation.",

  data:[
    "Claims",
    "Sources",
    "Ratings",
    "Reviews",
    "Publishers"
  ]
},

/* ================= META ================= */
{
  id:"facebook",
  name:"Facebook Pages",
  category:"social",
  logo:"/static/images/logos/facebook.png",

  auth_type:"oauth",

  connect_url:"http://localhost:4000/connectors/facebook/connect",
  sync_url:"http://localhost:4000/connectors/facebook/sync",
  dashboard:"/dashboard/facebook",

  long_description:`
Connect and analyze your Facebook Page to understand audience engagement,
content performance, reactions, comments, and daily insights.
All page data is securely synchronized using OAuth authorization.
  `,

  steps:[
    {title:"Provide App Credentials",desc:"Enter Facebook App ID and App Secret."},
    {title:"Authorize Access",desc:"Grant pages permissions via OAuth."},
    {title:"Initial Sync",desc:"Page posts, comments, reactions and insights are fetched."}
  ],

  tables:[
    "facebook_pages_metadata",
    "facebook_page_posts",
    "facebook_post_comments",
    "facebook_reactions",
    "facebook_page_insights"
  ],

  erd:"/static/images/erd/facebook_pages_erd.png",

  description:"Collects page metadata, posts, comments, reactions and insights.",

  data:[
    "Page information",
    "Posts & stories",
    "Comments",
    "Reactions summary",
    "Daily insights metrics"
  ]
},

/* ================= META ADS ================= */
{
  id:"facebook_ads",
  name:"Facebook Ads",
  category:"social",
  logo:"/static/images/logos/facebook.png",

  auth_type:"oauth",

  connect_url:"http://localhost:4000/connectors/facebook_ads/connect",
  sync_url:"http://localhost:4000/connectors/facebook_ads/sync",
  dashboard:"/dashboard/facebook_ads",

  long_description:`
Connect and analyze your Facebook Ad Account to monitor campaign
performance, ad sets, creatives, and advertising insights.
All advertising data is securely synchronized using OAuth authorization.
  `,

  steps:[
    {title:"Provide App Credentials",desc:"Enter Facebook App ID and App Secret."},
    {title:"Authorize Access",desc:"Grant ads_read and ads_management permissions via OAuth."},
    {title:"Initial Sync",desc:"Ad Accounts, Campaigns, Ads, Creatives and Insights are fetched."}
  ],

  tables:[
    "facebook_ad_accounts",
    "facebook_ad_campaigns",
    "facebook_ad_sets",
    "facebook_ads",
    "facebook_ad_creatives",
    "facebook_ads_insights"
  ],

  erd:"/static/images/erd/facebook_ads_erd.png",

  description:"Collects ad accounts, campaigns, ad sets, ads, creatives and performance insights.",

  data:[
    "Ad account metadata",
    "Campaign structure",
    "Ad sets configuration",
    "Ad creatives",
    "Performance insights (spend, impressions, clicks, CTR, CPC, CPM, reach)"
  ]
},

/* ================= DEV ================= */

{
  id:"github",
  name:"GitHub",
  category:"dev",
  logo:"/static/images/logos/github.png",

  auth_type:"oauth",

  connect_url:"http://localhost:4000/github/connect",
  sync_url:"http://localhost:4000/github/sync/repos",
  dashboard:"/dashboard/github",

  long_description:`
Analyzes repositories, commits, and issues
to provide engineering productivity insights.
  `,

  steps:[
    {title:"Authorize GitHub",desc:"Grant repository access."},
    {title:"Fetch Repos",desc:"Repositories loaded."},
    {title:"Sync Activity",desc:"Commits and issues stored."}
  ],

  tables:[
    "github_repos",
    "github_commits",
    "github_issues",
    "github_state"
  ],

  erd:"/static/images/erd/github_erd.png",

  description:"Analyzes repositories and code activity.",

  data:[
    "Repositories",
    "Commits",
    "Issues",
    "Stars",
    "Contributors"
  ]
},

{
  id:"gitlab",
  name:"GitLab",
  category:"dev",
  logo:"/static/images/logos/gitlab.png",

  auth_type:"oauth",

  connect_url:"http://localhost:4000/gitlab/connect",
  sync_url:"http://localhost:4000/gitlab/sync/projects",
  dashboard:"/dashboard/gitlab",

  long_description:`
Tracks GitLab projects, pipelines, and merge requests
for DevOps and workflow analytics.
  `,

  steps:[
    {title:"Authorize GitLab",desc:"Grant project access."},
    {title:"Load Projects",desc:"Projects discovered."},
    {title:"Sync Pipelines",desc:"Workflow data stored."}
  ],

  tables:[
    "gitlab_projects",
    "gitlab_commits",
    "gitlab_issues",
    "gitlab_merge_requests",
    "gitlab_state"
  ],

  erd:"/static/images/erd/gitlab_erd.png",

  description:"Tracks projects and development workflows.",

  data:[
    "Projects",
    "Commits",
    "Issues",
    "Merge requests",
    "Pipelines"
  ]
},

{
  id:"devto",
  name:"Dev.to",
  category:"dev",
  logo:"/static/images/logos/devto.png",

  auth_type:"api_key",

  connect_url:null,
  sync_url:"http://localhost:4000/devto/sync/articles",
  dashboard:"/dashboard/devto",

  long_description:`
Monitors developer articles, reactions, and engagement
on Dev.to to analyze content performance and trends.
  `,

  steps:[
    {title:"Configure API Key",desc:"Set Dev.to API key."},
    {title:"Fetch Articles",desc:"Articles downloaded."},
    {title:"Sync Engagement",desc:"Reactions and comments stored."}
  ],

  tables:[
    "devto_articles",
    "devto_comments",
    "devto_reactions",
    "devto_state"
  ],

  erd:"/static/images/erd/devto_erd.png",

  description:"Monitors developer content performance.",

  data:[
    "Articles",
    "Likes",
    "Comments",
    "Tags",
    "Authors"
  ]
},

{
  id:"stackoverflow",
  name:"StackOverflow",
  category:"dev",
  logo:"/static/images/logos/stackoverflow.png",

  auth_type:"api_key",

  connect_url:null,
  sync_url:"http://localhost:4000/stackoverflow/sync/questions",
  dashboard:"/dashboard/stackoverflow",

  long_description:`
Tracks questions, answers, and voting patterns
to analyze developer knowledge trends.
  `,

  steps:[
    {title:"Configure API Key",desc:"Set StackExchange key."},
    {title:"Fetch Questions",desc:"Questions loaded."},
    {title:"Sync Answers",desc:"Answers and votes stored."}
  ],

  tables:[
    "stack_questions",
    "stack_answers",
    "stack_users",
    "stack_state"
  ],

  erd:"/static/images/erd/stack_erd.png",

  description:"Tracks Q&A trends and developer activity.",

  data:[
    "Questions",
    "Answers",
    "Votes",
    "Tags",
    "Users"
  ]
},

{
  id:"hackernews",
  name:"HackerNews",
  category:"dev",
  logo:"/static/images/logos/hackernews.png",

  auth_type:"none",

  connect_url:null,
  sync_url:"http://localhost:4000/hackernews/sync",
  dashboard:"/dashboard/hackernews",

  long_description:`
Monitors Hacker News stories and discussions
to track technology and startup trends.
  `,

  steps:[
    {title:"Start Sync",desc:"Trigger HN crawler."},
    {title:"Fetch Stories",desc:"Top stories loaded."},
    {title:"Store Comments",desc:"Threads indexed."}
  ],

  tables:[
    "hackernews_stories",
    "hackernews_comments",
    "hackernews_state"
  ],

  erd:"/static/images/erd/hackernews_erd.png",

  description:"Monitors tech news discussions.",

  data:[
    "Stories",
    "Comments",
    "Scores",
    "Authors",
    "Links"
  ]
},

{
  id:"nvd",
  name:"NVD",
  category:"dev",
  logo:"/static/images/logos/nvd.png",

  auth_type:"api_key",

  connect_url:null,
  sync_url:"http://localhost:4000/nvd/sync",
  dashboard:"/dashboard/nvd",

  long_description:`
Tracks CVE vulnerability disclosures
from the National Vulnerability Database
for security monitoring.
  `,

  steps:[
    {title:"Configure API Key",desc:"Set NVD API key."},
    {title:"Fetch CVEs",desc:"Vulnerability data loaded."},
    {title:"Update State",desc:"Sync progress stored."}
  ],

  tables:[
    "nvd_cves",
    "nvd_products",
    "nvd_state"
  ],

  erd:"/static/images/erd/nvd_erd.png",

  description:"Tracks vulnerability disclosures.",

  data:[
    "CVEs",
    "Severity",
    "Products",
    "Versions",
    "Patches"
  ]
},


/* ================= SOCIAL ================= */

{
  id:"reddit",
  name:"Reddit",
  category:"social",
  logo:"/static/images/logos/reddit.png",

  auth_type:"oauth",

  connect_url:"http://localhost:4000/reddit/connect",
  sync_url:"http://localhost:4000/reddit/sync",
  dashboard:"/dashboard/reddit",

  long_description:`
Analyzes subreddit activity, posts, and comments
to measure community engagement and sentiment.
  `,

  steps:[
    {title:"Authorize Reddit",desc:"Grant account access."},
    {title:"Fetch Subreddits",desc:"Subscribed subs loaded."},
    {title:"Sync Posts",desc:"Posts and comments stored."}
  ],

  tables:[
    "reddit_accounts",
    "reddit_profiles",
    "reddit_posts",
    "reddit_comments",
    "reddit_state"
  ],

  erd:"/static/images/erd/reddit_erd.png",

  description:"Analyzes community engagement.",

  data:[
    "Posts",
    "Comments",
    "Upvotes",
    "Subreddits",
    "Users"
  ]
},

{
  id:"discord",
  name:"Discord",
  category:"social",
  logo:"/static/images/logos/discord.png",

  auth_type:"bot",

  connect_url:null,
  sync_url:"http://localhost:4000/discord/sync/guilds",
  dashboard:"/dashboard/discord",

  long_description:`
Tracks Discord server messages and activity
using bot-based integration.
  `,

  steps:[
    {title:"Add Bot",desc:"Invite bot to server."},
    {title:"Authorize Permissions",desc:"Grant read access."},
    {title:"Sync Messages",desc:"Channel data stored."}
  ],

  tables:[
    "discord_guilds",
    "discord_channels",
    "discord_messages",
    "discord_state"
  ],

  erd:"/static/images/erd/discord_erd.png",

  description:"Tracks server activity and engagement.",

  data:[
    "Messages",
    "Members",
    "Channels",
    "Reactions",
    "Roles"
  ]
},

{
  id:"telegram",
  name:"Telegram",
  category:"social",
  logo:"/static/images/logos/telegram.png",

  auth_type:"bot_token",

  connect_url:null,
  sync_url:"http://localhost:4000/telegram/sync/channel",
  dashboard:"/dashboard/telegram",

  long_description:`
Collects Telegram channel messages and engagement
using bot token authentication.
  `,

  steps:[
    {title:"Create Bot",desc:"Generate Telegram bot token."},
    {title:"Join Channel",desc:"Add bot to channel."},
    {title:"Sync Messages",desc:"Messages indexed."}
  ],

  tables:[
    "telegram_channels",
    "telegram_messages",
    "telegram_state"
  ],

  erd:"/static/images/erd/telegram_erd.png",

  description:"Collects channel analytics.",

  data:[
    "Messages",
    "Views",
    "Subscribers",
    "Forwards",
    "Reactions"
  ]
},

{
  id:"tumblr",
  name:"Tumblr",
  category:"social",
  logo:"/static/images/logos/tumblr.png",

  auth_type:"oauth",

  connect_url:"http://localhost:4000/tumblr/connect",
  sync_url:"http://localhost:4000/tumblr/sync",
  dashboard:"/dashboard/tumblr",

  long_description:`
Tracks Tumblr blogs, posts, and engagement
to analyze content reach.
  `,

  steps:[
    {title:"Authorize Tumblr",desc:"Grant blog access."},
    {title:"Fetch Blogs",desc:"User blogs loaded."},
    {title:"Sync Posts",desc:"Posts and likes stored."}
  ],

  tables:[
    "tumblr_blogs",
    "tumblr_posts",
    "tumblr_notes",
    "tumblr_state"
  ],

  erd:"/static/images/erd/tumblr_erd.png",

  description:"Tracks blog engagement.",

  data:[
    "Posts",
    "Likes",
    "Reblogs",
    "Tags",
    "Authors"
  ]
},

{
  id:"medium",
  name:"Medium",
  category:"social",
  logo:"/static/images/logos/medium.png",

  auth_type:"oauth",

  connect_url:"http://localhost:4000/medium/connect",
  sync_url:"http://localhost:4000/medium/sync",
  dashboard:"/dashboard/medium",

  long_description:`
Analyzes Medium articles, followers, and engagement
to measure content impact.
  `,

  steps:[
    {title:"Authorize Medium",desc:"Grant account access."},
    {title:"Fetch Publications",desc:"Publications loaded."},
    {title:"Sync Articles",desc:"Stories indexed."}
  ],

  tables:[
    "medium_users",
    "medium_posts",
    "medium_stats",
    "medium_state"
  ],

  erd:"/static/images/erd/medium_erd.png",

  description:"Analyzes article reach.",

  data:[
    "Reads",
    "Claps",
    "Responses",
    "Followers",
    "Tags"
  ]
},

{
  id:"mastodon",
  name:"Mastodon",
  category:"social",
  logo:"/static/images/logos/mastodon.png",

  auth_type:"oauth",

  connect_url:"http://localhost:4000/mastodon/connect",
  sync_url:"http://localhost:4000/mastodon/sync",
  dashboard:"/dashboard/mastodon",

  long_description:`
Tracks federated social interactions across Mastodon
instances for decentralized social analytics.
  `,

  steps:[
    {title:"Authorize Instance",desc:"Login to Mastodon server."},
    {title:"Fetch Timeline",desc:"Timelines loaded."},
    {title:"Sync Toots",desc:"Posts and boosts stored."}
  ],

  tables:[
    "mastodon_accounts",
    "mastodon_statuses",
    "mastodon_favourites",
    "mastodon_state"
  ],

  erd:"/static/images/erd/mastodon_erd.png",

  description:"Tracks federated social activity.",

  data:[
    "Toots",
    "Boosts",
    "Favorites",
    "Users",
    "Instances"
  ]
},

{
  id:"lemmy",
  name:"Lemmy",
  category:"social",
  logo:"/static/images/logos/lemmy.png",

  auth_type:"none",

  connect_url:null,
  sync_url:"http://localhost:4000/lemmy/sync",
  dashboard:"/dashboard/lemmy",

  long_description:`
Analyzes federated Lemmy communities and discussions
to understand decentralized social engagement.
  `,

  steps:[
    {title:"Configure Instance",desc:"Select Lemmy instance."},
    {title:"Fetch Communities",desc:"Communities loaded."},
    {title:"Sync Posts",desc:"Posts and votes stored."}
  ],

  tables:[
    "lemmy_communities",
    "lemmy_posts",
    "lemmy_comments",
    "lemmy_votes",
    "lemmy_state"
  ],

  erd:"/static/images/erd/lemmy_erd.png",

  description:"Analyzes federated communities.",

  data:[
    "Posts",
    "Communities",
    "Votes",
    "Users",
    "Comments"
  ]
},

{
  id:"pinterest",
  name:"Pinterest",
  category:"social",
  logo:"/static/images/logos/pinterest.png",

  auth_type:"oauth",

  connect_url:"http://localhost:4000/pinterest/connect",
  sync_url:"http://localhost:4000/pinterest/sync",
  dashboard:"/dashboard/pinterest",

  long_description:`
Tracks Pinterest boards and pins to analyze
visual content performance and trends.
  `,

  steps:[
    {title:"Authorize Pinterest",desc:"Grant board access."},
    {title:"Fetch Boards",desc:"Boards loaded."},
    {title:"Sync Pins",desc:"Pins indexed."}
  ],

  tables:[
    "pinterest_tokens",
    "pinterest_boards",
    "pinterest_pins",
    "pinterest_state"
  ],

  erd:"/static/images/erd/pinterest_erd.png",

  description:"Tracks pins and boards.",

  data:[
    "Boards",
    "Pins",
    "Saves",
    "Views",
    "Links"
  ]
},

{
  id:"twitch",
  name:"Twitch",
  category:"video",
  logo:"/static/images/logos/twitch.png",

  auth_type:"oauth",

  connect_url:"http://localhost:4000/twitch/connect",
  sync_url:"http://localhost:4000/twitch/sync",
  dashboard:"/dashboard/twitch",

  long_description:`
Monitors Twitch channels, streams, and chat activity
for live streaming analytics.
  `,

  steps:[
    {title:"Authorize Twitch",desc:"Grant channel access."},
    {title:"Fetch Streams",desc:"Stream metadata loaded."},
    {title:"Sync Chats",desc:"Chat messages stored."}
  ],

  tables:[
    "twitch_channels",
    "twitch_streams",
    "twitch_chats",
    "twitch_clips",
    "twitch_state"
  ],

  erd:"/static/images/erd/twitch_erd.png",

  description:"Monitors live streaming analytics.",

  data:[
    "Streams",
    "Viewers",
    "Followers",
    "Chats",
    "Clips"
  ]
},

{
  id:"peertube",
  name:"PeerTube",
  category:"video",
  logo:"/static/images/logos/peertube.png",

  auth_type:"none",

  connect_url:null,
  sync_url:"http://localhost:4000/peertube/sync",
  dashboard:"/dashboard/peertube",

  long_description:`
Tracks decentralized video content across PeerTube
instances for federated media analytics.
  `,

  steps:[
    {title:"Configure Instance",desc:"Select PeerTube server."},
    {title:"Fetch Channels",desc:"Channels loaded."},
    {title:"Sync Videos",desc:"Videos indexed."}
  ],

  tables:[
    "peertube_channels",
    "peertube_videos",
    "peertube_comments",
    "peertube_state"
  ],

  erd:"/static/images/erd/peertube_erd.png",

  description:"Tracks decentralized video content.",

  data:[
    "Videos",
    "Views",
    "Channels",
    "Comments",
    "Tags"
  ]
},


/* ================= OTHER ================= */

{
  id:"openstreetmap",
  name:"OpenStreetMap",
  category:"other",
  logo:"/static/images/logos/openstreetmap.png",

  auth_type:"none",

  connect_url:null,
  sync_url:"http://localhost:4000/osm/sync",
  dashboard:"/dashboard/openstreetmap",

  long_description:`
Collects and analyzes geospatial data from
OpenStreetMap for mapping and location services.
  `,

  steps:[
    {title:"Configure Region",desc:"Select geographic area."},
    {title:"Fetch Map Data",desc:"OSM data downloaded."},
    {title:"Normalize GeoData",desc:"Coordinates processed."}
  ],

  tables:[
    "osm_roads",
    "osm_buildings",
    "osm_pois",
    "osm_regions",
    "osm_state"
  ],

  erd:"/static/images/erd/osm_erd.png",

  description:"Collects geospatial map data.",

  data:[
    "Roads",
    "Buildings",
    "POIs",
    "Coordinates",
    "Regions"
  ]
},

{
  id:"wikipedia",
  name:"Wikipedia",
  category:"other",
  logo:"/static/images/logos/wikipedia.png",

  auth_type:"none",

  connect_url:null,
  sync_url:"http://localhost:4000/wikipedia/sync",
  dashboard:"/dashboard/wikipedia",

  long_description:`
Analyzes Wikipedia articles and revisions
to track knowledge updates and editor activity.
  `,

  steps:[
    {title:"Start Sync",desc:"Trigger Wikipedia crawler."},
    {title:"Fetch Articles",desc:"Pages loaded."},
    {title:"Track Revisions",desc:"Edits indexed."}
  ],

  tables:[
    "wikipedia_pages",
    "wikipedia_revisions",
    "wikipedia_editors",
    "wikipedia_state"
  ],

  erd:"/static/images/erd/wikipedia_erd.png",

  description:"Analyzes encyclopedia content.",

  data:[
    "Articles",
    "Revisions",
    "Editors",
    "Categories",
    "Links"
  ]
},

{
  id:"producthunt",
  name:"ProductHunt",
  category:"other",
  logo:"/static/images/logos/producthunt.png",

  auth_type:"api_key",

  connect_url:null,
  sync_url:"http://localhost:4000/producthunt/sync",
  dashboard:"/dashboard/producthunt",

  long_description:`
Tracks new product launches, votes, and makers
on Product Hunt for startup analytics.
  `,

  steps:[
    {title:"Configure API Token",desc:"Set Product Hunt token."},
    {title:"Fetch Products",desc:"Daily launches loaded."},
    {title:"Sync Votes",desc:"Engagement stored."}
  ],

  tables:[
    "producthunt_products",
    "producthunt_votes",
    "producthunt_users",
    "producthunt_state"
  ],

  erd:"/static/images/erd/producthunt_erd.png",

  description:"Tracks startup launches.",

  data:[
    "Products",
    "Votes",
    "Comments",
    "Makers",
    "Categories"
  ]
},

{
  id:"discourse",
  name:"Discourse",
  category:"other",
  logo:"/static/images/logos/discourse.png",

  auth_type:"api_key",

  connect_url:null,
  sync_url:"http://localhost:4000/discourse/sync",
  dashboard:"/dashboard/discourse",

  long_description:`
Analyzes forum topics, posts, and user engagement
from Discourse communities.
  `,

  steps:[
    {title:"Configure API Key",desc:"Set Discourse key."},
    {title:"Fetch Topics",desc:"Topics loaded."},
    {title:"Sync Posts",desc:"Posts indexed."}
  ],

  tables:[
    "discourse_topics",
    "discourse_posts",
    "discourse_users",
    "discourse_state"
  ],

  erd:"/static/images/erd/discourse_erd.png",

  description:"Analyzes forum discussions.",

  data:[
    "Topics",
    "Posts",
    "Users",
    "Tags",
    "Categories"
  ]
},

];