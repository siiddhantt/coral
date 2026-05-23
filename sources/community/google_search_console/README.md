# Google Search Console

**Version:** 0.1.0  
**Backend:** HTTP  
**Tables:** 7  
**Base URL:** `https://www.googleapis.com/webmasters/v3`  

Query Google Search Console for verified properties (sites) and search performance metrics (clicks, impressions, CTR, position) filtered by date range and grouped by dimensions such as date, query, page, country, or device. The performance tables require a `encoded_site_url`, `start_date`, and `end_date` filter, making them useful for combining with other analytics sources or extracting specific SEO snapshots.

## Tables

| Table | Description | Required filters | Optional filters |
| :--- | :--- | :--- | :--- |
| `sites` | Lists all verified properties accessible to the user. | | |
| `search_performance` | Aggregate metrics without dimension grouping. | `encoded_site_url`, `start_date`, `end_date` | |
| `performance_by_date` | Metrics grouped by date. | `encoded_site_url`, `start_date`, `end_date` | |
| `performance_by_query` | Metrics grouped by search query (keyword). | `encoded_site_url`, `start_date`, `end_date` | |
| `performance_by_page` | Metrics grouped by landing page URL. | `encoded_site_url`, `start_date`, `end_date` | |
| `performance_by_country` | Metrics grouped by country (ISO 3166-1 alpha-3). | `encoded_site_url`, `start_date`, `end_date` | |
| `performance_by_device` | Metrics grouped by device type (MOBILE, DESKTOP, TABLET). | `encoded_site_url`, `start_date`, `end_date` | |

## Authentication

Google Search Console requires an OAuth 2.0 access token with the `https://www.googleapis.com/auth/webmasters.readonly` scope. 

1. Go to the [Google Cloud Console](https://console.cloud.google.com/).
2. Create a new project or select an existing one.
3. Enable the **Google Search Console API** in the API library.
4. Navigate to **APIs & Services > Credentials** and click **Create Credentials > OAuth client ID**.
5. Select **Desktop app** as the application type.
6. Note down the **Client ID** and **Client Secret**.
7. In Coral, run the interactive setup command below and provide the Client ID and Secret when prompted.

## Install

Add the source interactively:
```sh
coral source add --interactive --file sources/community/google_search_console/manifest.yaml
```

Lint the manifest to verify syntax:
```sh
coral source lint sources/community/google_search_console/manifest.yaml
```

Run test queries defined in the manifest:
```sh
coral source test --file sources/community/google_search_console/manifest.yaml
```

## Example Queries

### List available properties
Start by retrieving the `encoded_site_url` values, which are required for performance queries.

```sql
SELECT site_url, encoded_site_url, permission_level
FROM google_search_console.sites;
```

### Total search performance for a month
Fetch the aggregate clicks, impressions, CTR, and average position for January 2025.

```sql
SELECT clicks, impressions, ctr, position
FROM google_search_console.search_performance
WHERE encoded_site_url = 'https%3A%2F%2Fexample.com%2F'
  AND start_date = '2025-01-01'
  AND end_date = '2025-01-31';
```

### Top 50 search queries by clicks
Find the keywords driving the most traffic.

```sql
SELECT query, clicks, impressions, position
FROM google_search_console.performance_by_query
WHERE encoded_site_url = 'https%3A%2F%2Fexample.com%2F'
  AND start_date = '2025-01-01'
  AND end_date = '2025-01-31'
ORDER BY clicks DESC
LIMIT 50;
```

### Daily performance trends
Retrieve daily metrics to plot traffic trends.

```sql
SELECT date, clicks, impressions, ctr
FROM google_search_console.performance_by_date
WHERE encoded_site_url = 'https%3A%2F%2Fexample.com%2F'
  AND start_date = '2025-01-01'
  AND end_date = '2025-01-31'
ORDER BY date ASC;
```

### Traffic by device type
Break down search performance by Mobile, Desktop, and Tablet.

```sql
SELECT device, clicks, impressions, ctr
FROM google_search_console.performance_by_device
WHERE encoded_site_url = 'https%3A%2F%2Fexample.com%2F'
  AND start_date = '2025-01-01'
  AND end_date = '2025-01-31';
```

## Notes

* The `encoded_site_url` filter value must be URL-encoded (e.g., `https%3A%2F%2Fexample.com%2F` or `sc-domain%3Aexample.com`). You can discover the exact `encoded_site_url` string for a property by querying the `sites` table.
* The API returns up to 25,000 rows per request. Pagination is handled automatically by Coral using the `startRow` offset mechanism.
* The API may omit rows where data is zero (e.g., a date with no clicks or impressions).
* Start and end dates must be provided in `YYYY-MM-DD` format.

## Limitations

* Due to API constraints, querying multiple dimensions at once (e.g., `date` AND `query`) is not supported out of the box in this source structure, as each dimension requires a separate table definition.
* This source supports read-only operations. Modifying sitemaps or managing property verification is not supported.
