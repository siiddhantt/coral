# Reddit

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 7
**Base URL:** `https://www.reddit.com`

Query public Reddit posts, comments, user activity, and keyword search through
Reddit JSON endpoints. This source is read-only and does not require
authentication.

```bash
coral source add --file sources/community/reddit/manifest.yaml
coral source test reddit
```

## Tables

| Table | Description | Required filters |
| --- | --- | --- |
| `subreddit_hot` | Hot posts from a subreddit | `subreddit` |
| `subreddit_new` | Newest posts from a subreddit | `subreddit` |
| `subreddit_top` | Top posts from a subreddit | `subreddit` |
| `search_posts` | Search public Reddit posts globally or in one subreddit | `q` |
| `user_posts` | Public posts submitted by a user | `username` |
| `user_comments` | Public comments written by a user | `username` |
| `post_comments` | Top-level comments for a post | `subreddit`, `post_id` |

## Quick Start

List hot posts from a subreddit:

```bash
coral sql "
  SELECT title, author, score, num_comments, permalink
  FROM reddit.subreddit_hot
  WHERE subreddit = 'LocalLLaMA'
  LIMIT 25
"
```

List newest posts:

```bash
coral sql "
  SELECT title, author, score, created_utc, permalink
  FROM reddit.subreddit_new
  WHERE subreddit = 'redditdev'
  LIMIT 25
"
```

Search all of Reddit:

```bash
coral sql "
  SELECT title, subreddit, author, score, permalink
  FROM reddit.search_posts
  WHERE q = 'open source agents'
  LIMIT 25
"
```

Search inside one subreddit:

```bash
coral sql "
  SELECT title, subreddit, author, score, permalink
  FROM reddit.search_posts
  WHERE q = 'vector database'
    AND subreddit = 'LocalLLaMA'
  LIMIT 25
"
```

Get comments from a post:

```bash
coral sql "
  SELECT author, score, body, created_utc
  FROM reddit.post_comments
  WHERE subreddit = 'redditdev'
    AND post_id = '<post_id>'
  LIMIT 100
"
```

List public posts by a user:

```bash
coral sql "
  SELECT title, subreddit, score, permalink
  FROM reddit.user_posts
  WHERE username = '<username>'
  LIMIT 25
"
```

List public comments by a user:

```bash
coral sql "
  SELECT body, subreddit, score, permalink
  FROM reddit.user_comments
  WHERE username = '<username>'
  LIMIT 25
"
```

## Common Columns

Post tables expose these commonly useful columns:

| Column | Description |
| --- | --- |
| `id` | Reddit post ID without the `t3_` prefix |
| `fullname` | Reddit fullname, usually prefixed with `t3_` |
| `title` | Post title |
| `subreddit` | Subreddit name without the `r/` prefix |
| `author` | Reddit username |
| `score` | Current Reddit score |
| `upvote_ratio` | Upvote ratio when Reddit provides it |
| `num_comments` | Number of comments on the post |
| `permalink` | Relative Reddit permalink |
| `url` | Linked URL or Reddit URL |
| `selftext` | Text body for self posts |
| `created_utc` | Creation time as a UTC timestamp |
| `raw` | Raw Reddit listing child JSON |

Comment tables expose:

| Column | Description |
| --- | --- |
| `id` | Reddit comment ID without the `t1_` prefix |
| `fullname` | Reddit fullname, usually prefixed with `t1_` |
| `body` | Comment text |
| `author` | Reddit username |
| `score` | Current comment score |
| `parent_id` | Parent post or comment fullname |
| `permalink` | Relative Reddit permalink |
| `created_utc` | Creation time as a UTC timestamp |
| `raw` | Raw Reddit listing child JSON |

## Notes And Limitations

- This source uses public Reddit JSON endpoints and does not authenticate.
- Private subreddits, saved posts, inbox data, moderation queues, and votes are
  not available in this version.
- Reddit may rate-limit public requests. Keep exploratory queries small with
  `LIMIT`.
- `post_comments` returns top-level comments and includes Reddit `more`
  placeholders as rows with `kind = 'more'`; nested replies are available in the
  `replies` JSON column.
- Subreddit filters should not include the `r/` prefix. Use `LocalLLaMA`, not
  `r/LocalLLaMA`.
- Username filters should not include the `u/` prefix.

## Useful Queries

Find product mentions:

```bash
coral sql "
  SELECT title, subreddit, author, score, permalink
  FROM reddit.search_posts
  WHERE q = 'RasmalAI bug OR error OR pricing'
  LIMIT 50
"
```

Watch launch sentiment:

```bash
coral sql "
  SELECT title, subreddit, score, num_comments, created_utc, permalink
  FROM reddit.search_posts
  WHERE q = 'Coral SQL'
    AND sort = 'new'
  LIMIT 50
"
```

Find highly discussed posts:

```bash
coral sql "
  SELECT title, author, score, num_comments, permalink
  FROM reddit.subreddit_top
  WHERE subreddit = 'LocalLLaMA'
    AND t = 'week'
  ORDER BY num_comments DESC
  LIMIT 25
"
```

## AI And Operational Intelligence Use Cases

Reddit can be useful for:

- product mention monitoring
- launch sentiment tracking
- competitor research
- community trend detection
- public bug-report discovery
- incident chatter monitoring
- feeding AI agents real user language and pain points

Example workflow:

```text
Reddit mentions
+ GitHub issues
+ Stripe customer state
+ PostHog product events
= community and customer intelligence brief
```

