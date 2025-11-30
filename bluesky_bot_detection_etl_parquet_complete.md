# Bluesky Bot Detection ETL (Parquet + DuckDB) - Complete

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              INGESTION                                      │
│                                                                             │
│  Firehose Consumer (Python)                                                 │
│       │                                                                     │
│       ▼                                                                     │
│  data/                                                                      │
│  ├── events/                                                                │
│  │   ├── 2025-01-15.parquet                                                │
│  │   ├── 2025-01-16.parquet                                                │
│  │   └── ...                                                                │
│  ├── posts/                                                                 │
│  │   ├── 2025-01-15.parquet                                                │
│  │   └── ...                                                                │
│  ├── follows/                                                               │
│  │   └── current.parquet        (updated incrementally)                    │
│  └── accounts/                                                              │
│      └── current.parquet        (updated periodically via DID resolution)  │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           BATCH PROCESSING                                  │
│                                                                             │
│  Python scripts using DuckDB to query parquet files                         │
│                                                                             │
│  Daily:                                                                     │
│    1_fingerprint.py  → data/content_shares/YYYY-MM-DD.parquet              │
│    2_coordination.py → data/coordination_events/YYYY-MM-DD.parquet         │
│    3_features.py     → data/features/YYYY-MM-DD.parquet                    │
│    4_cluster.py      → data/clusters/YYYY-MM-DD.parquet                    │
│                                                                             │
│  Weekly:                                                                    │
│    5_embeddings.py   → data/embeddings/YYYY-MM-DD.parquet                  │
│    6_network.py      → data/network_features/YYYY-MM-DD.parquet            │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              OUTPUT                                         │
│                                                                             │
│  data/clusters/YYYY-MM-DD.parquet                                          │
│  Jupyter notebooks for exploration                                          │
│  Optional: simple Flask/FastAPI for queries                                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Directory Structure

```
project/
├── ingest/
│   └── firehose.py           # Firehose consumer, writes daily parquet
├── jobs/
│   ├── 1_fingerprint.py      # Content fingerprinting
│   ├── 2_coordination.py     # Coordination detection
│   ├── 3_features.py         # Feature computation (daily)
│   ├── 4_cluster.py          # Clustering
│   ├── 5_embeddings.py       # Embedding generation (weekly)
│   └── 6_network.py          # Network feature computation (weekly)
├── data/
│   ├── events/               # Daily event parquet files
│   ├── posts/                # Daily post parquet files
│   ├── follows/              # Follow graph (current state)
│   ├── accounts/             # Account metadata
│   ├── content_shares/       # Daily fingerprinted content
│   ├── coordination_events/  # Daily coordination pairs
│   ├── features/             # Daily feature snapshots
│   ├── embeddings/           # Weekly embedding snapshots
│   ├── network_features/     # Weekly network metrics
│   └── clusters/             # Daily cluster assignments
├── lib/
│   ├── fingerprint.py        # URL/text normalization utilities
│   └── features.py           # Feature computation helpers
└── notebooks/
    └── explore.ipynb         # Investigation notebook
```

---

## Data Schemas

### Raw Data (from Ingestion)

#### events/{date}.parquet

Captures all interactions from the firehose.

| Column | Type | Description |
|--------|------|-------------|
| event_type | string | 'post', 'repost', 'like', 'reply', 'quote', 'follow', 'block' |
| actor_did | string | Who performed the action |
| target_did | string | Target account (for repost/like/reply/follow/block) |
| target_post_uri | string | Target post (for repost/like/reply/quote) |
| timestamp | timestamp[us] | Event time |

#### posts/{date}.parquet

Full post content for text analysis.

| Column | Type | Description |
|--------|------|-------------|
| post_uri | string | Post identifier (at://did/app.bsky.feed.post/rkey) |
| author_did | string | Author DID |
| text | string | Post text content |
| text_length | int | Character count |
| reply_parent | string | Parent post URI if this is a reply |
| reply_root | string | Thread root post URI if this is a reply |
| quote_uri | string | Quoted post URI if this is a quote-post |
| embed_type | string | 'images', 'external', 'record', 'recordWithMedia', null |
| urls | list[string] | URLs extracted from text and embeds |
| mentions | list[string] | @mentioned DIDs |
| hashtags | list[string] | #hashtags |
| langs | list[string] | Detected languages |
| has_images | bool | Whether post contains images |
| image_count | int | Number of images |
| created_at | timestamp[us] | Post creation time |

#### follows/current.parquet

Current state of the follow graph (updated incrementally).

| Column | Type | Description |
|--------|------|-------------|
| follower_did | string | Account doing the following |
| following_did | string | Account being followed |
| created_at | timestamp[us] | When the follow was created |

#### accounts/current.parquet

Account metadata (updated periodically via DID resolution).

| Column | Type | Description |
|--------|------|-------------|
| did | string | Account DID |
| handle | string | Current handle |
| display_name | string | Display name |
| bio | string | Profile bio/description |
| bio_length | int | Bio character count |
| has_avatar | bool | Whether account has avatar |
| has_banner | bool | Whether account has banner image |
| created_at | timestamp[us] | Account creation time (from DID document) |
| first_seen | timestamp[us] | First seen in firehose |
| last_active | timestamp[us] | Last activity timestamp |
| follower_count | int | Number of followers (if available) |
| following_count | int | Number following (if available) |
| post_count | int | Total posts (if available) |
| pds_host | string | PDS endpoint hostname |
| updated_at | timestamp[us] | Last metadata refresh |

---

### Computed Data (from Batch Jobs)

#### content_shares/{date}.parquet

Fingerprinted content for coordination detection.

| Column | Type | Description |
|--------|------|-------------|
| actor_did | string | Who shared the content |
| content_type | string | 'url', 'repost', 'quote', 'text', 'image' |
| fingerprint | string | Hash of normalized content |
| content_raw | string | Original content (URL, text snippet, post URI) |
| post_uri | string | The post containing this share |
| created_at | timestamp[us] | When shared |

#### coordination_events/{date}.parquet

Pairs of accounts sharing same content within time window.

| Column | Type | Description |
|--------|------|-------------|
| fingerprint | string | Content fingerprint |
| content_type | string | Type of coordinated content |
| content_raw | string | Original content for inspection |
| account_a | string | First account (lexicographically) |
| account_b | string | Second account |
| time_a | timestamp[us] | First account's share time |
| time_b | timestamp[us] | Second account's share time |
| time_delta_sec | int | Seconds between shares |

#### features/{date}.parquet

All computed features per account.

| Column | Type | Description |
|--------|------|-------------|
| **Identity** | | |
| did | string | Account DID |
| window_days | int | Feature window (7 or 30) |
| computed_at | timestamp[us] | When features were computed |
| | | |
| **Temporal Features** | | |
| total_posts | int | Posts in window |
| total_events | int | All events (posts + reposts + likes) in window |
| posts_per_hour_mean | float | Mean posts per hour |
| posts_per_hour_std | float | Std dev of posts per hour |
| posts_per_hour_max | float | Max posts in any hour |
| active_hours_count | int | Distinct hours with activity (0-24) |
| active_days_count | int | Distinct days with activity |
| hourly_entropy | float | Entropy of hourly post distribution (0-~3.18) |
| daily_entropy | float | Entropy of daily post distribution |
| night_post_ratio | float | Posts between 00:00-06:00 UTC / total |
| weekend_ratio | float | Weekend posts / weekday posts |
| inter_post_time_mean | float | Mean seconds between consecutive posts |
| inter_post_time_std | float | Std dev of inter-post times |
| inter_post_time_min | float | Minimum inter-post time |
| inter_post_time_entropy | float | Entropy of binned inter-post times |
| burstiness | float | (std - mean) / (std + mean) of inter-post times |
| response_latency_mean | float | Mean seconds to reply after parent post |
| response_latency_min | float | Minimum response latency |
| | | |
| **Content Features** | | |
| mean_post_length | float | Mean character count per post |
| std_post_length | float | Std dev of post length |
| median_post_length | float | Median post length |
| unique_word_ratio | float | Unique words / total words (type-token ratio) |
| vocabulary_size | int | Distinct words used |
| hashtag_per_post_mean | float | Mean hashtags per post |
| hashtag_total | int | Total hashtags used |
| unique_hashtags | int | Distinct hashtags |
| mention_per_post_mean | float | Mean @mentions per post |
| mention_total | int | Total mentions |
| unique_mentions | int | Distinct accounts mentioned |
| link_ratio | float | Posts with links / total posts |
| link_total | int | Total links shared |
| unique_domains | int | Distinct domains linked |
| image_ratio | float | Posts with images / total posts |
| emoji_ratio | float | Posts with emoji / total posts |
| question_ratio | float | Posts containing '?' / total posts |
| duplicate_text_ratio | float | Posts with exact duplicate text / total |
| near_duplicate_ratio | float | Posts with near-duplicate text / total |
| self_similarity_score | float | Mean pairwise cosine similarity of post embeddings |
| | | |
| **Behavioral Features** | | |
| repost_ratio | float | Reposts / total actions |
| like_ratio | float | Likes / total actions |
| reply_ratio | float | Replies / total posts |
| quote_ratio | float | Quote posts / total posts |
| original_post_ratio | float | Original posts (non-reply, non-quote) / total posts |
| like_to_post_ratio | float | Likes given / posts made |
| actions_per_post | float | Total actions / posts |
| unique_reply_targets | int | Distinct accounts replied to |
| unique_repost_sources | int | Distinct accounts reposted |
| unique_like_targets | int | Distinct accounts whose posts were liked |
| unique_quote_targets | int | Distinct accounts quoted |
| reply_target_entropy | float | Entropy of reply target distribution |
| repost_source_entropy | float | Entropy of repost source distribution |
| like_target_entropy | float | Entropy of like target distribution |
| top_reply_target_share | float | Replies to top target / total replies |
| top_repost_source_share | float | Reposts of top source / total reposts |
| top_like_target_share | float | Likes on top target / total likes |
| reply_target_hhi | float | Herfindahl index of reply targets |
| repost_source_hhi | float | Herfindahl index of repost sources |
| like_target_hhi | float | Herfindahl index of like targets |
| reply_to_non_followed_ratio | float | Replies to non-followed accounts / total replies |
| conversation_depth_mean | float | Mean thread depth of replies |
| self_reply_ratio | float | Replies to own posts / total replies |
| mutual_interaction_ratio | float | Interactions with accounts who interact back |
| | | |
| **Relational Features** | | |
| repost_target_concentration | float | HHI of accounts being reposted |
| reply_target_concentration | float | HHI of accounts being replied to |
| like_target_concentration | float | HHI of accounts being liked |
| mention_target_concentration | float | HHI of accounts being mentioned |
| rare_target_ratio | float | Interactions with accounts <100 followers / total |
| top_repost_targets | list[string] | Top 5 most reposted DIDs |
| top_reply_targets | list[string] | Top 5 most replied-to DIDs |
| top_like_targets | list[string] | Top 5 most liked DIDs |
| | | |
| **Coordination Features** | | |
| coordination_event_count | int | Total coordination events participated in |
| unique_coord_partners | int | Distinct accounts coordinated with |
| coord_partner_concentration | float | HHI of coordination partners |
| mean_coord_time_delta | float | Mean seconds between coordinated shares |
| median_coord_time_delta | float | Median coordination time delta |
| min_coord_time_delta | float | Minimum coordination time delta |
| url_coordination_count | int | URL coordination events |
| repost_coordination_count | int | Repost coordination events |
| text_coordination_count | int | Text coordination events |
| repeat_coord_partner_ratio | float | Partners coordinated with 3+ times / total partners |
| burst_participation_count | int | Content bursts participated in (5+ sharers in 4h) |
| suspicious_burst_count | int | Suspicious bursts (low timing variance) |
| top_coord_partners | list[string] | Top 5 coordination partners |
| top_coord_content | list[string] | Top 5 coordinated content fingerprints |
| | | |
| **Account Metadata Features** | | |
| account_age_days | int | Days since account creation |
| days_since_first_seen | int | Days since first seen in firehose |
| handle_length | int | Character count of handle |
| handle_digit_ratio | float | Digits in handle / total characters |
| handle_entropy | float | Character entropy of handle |
| has_avatar | bool | Whether account has avatar |
| has_bio | bool | Whether account has bio |
| bio_length | int | Bio character count |
| display_name_length | int | Display name character count |
| posts_per_day_lifetime | float | Total posts / account age |

#### network_features/{date}.parquet (Weekly)

Graph-based features computed from interaction network.

| Column | Type | Description |
|--------|------|-------------|
| did | string | Account DID |
| computed_at | timestamp[us] | When computed |
| | | |
| **Degree Metrics** | | |
| in_degree | int | Incoming interactions (others → this account) |
| out_degree | int | Outgoing interactions (this account → others) |
| degree_ratio | float | out_degree / in_degree |
| weighted_in_degree | float | Sum of incoming edge weights |
| weighted_out_degree | float | Sum of outgoing edge weights |
| | | |
| **Centrality Metrics** | | |
| clustering_coefficient | float | Local clustering coefficient |
| pagerank | float | PageRank score |
| eigenvector_centrality | float | Eigenvector centrality |
| betweenness_centrality | float | Betweenness centrality (sampled) |
| | | |
| **Community Metrics** | | |
| community_id | int | Community assignment (Louvain/Leiden) |
| community_size | int | Size of assigned community |
| inter_community_ratio | float | Edges to other communities / total edges |

#### embeddings/{date}.parquet (Weekly)

Dense vector representations for similarity computation.

| Column | Type | Description |
|--------|------|-------------|
| did | string | Account DID |
| embedding_type | string | Type of embedding |
| vector | list[float] | Embedding vector (typically 64 dimensions) |
| computed_at | timestamp[us] | When computed |

Embedding types:
- `content`: Sentence embedding of concatenated recent posts
- `repost_target`: SVD of TF-IDF repost target vector
- `reply_target`: SVD of TF-IDF reply target vector
- `like_target`: SVD of TF-IDF like target vector
- `graph`: Node2Vec embedding from interaction graph
- `bio`: Sentence embedding of bio text

#### clusters/{date}.parquet

Final cluster assignments with investigation metadata.

| Column | Type | Description |
|--------|------|-------------|
| did | string | Account DID |
| cluster_id | int | Cluster assignment (-1 = noise/unclustered) |
| cluster_probability | float | HDBSCAN membership probability |
| outlier_score | float | HDBSCAN outlier score |
| bot_likelihood | float | Computed bot likelihood score (0-1) |
| | | |
| **Cluster Metadata (denormalized)** | | |
| cluster_size | int | Number of accounts in cluster |
| cluster_mean_coord_events | float | Mean coordination events in cluster |
| cluster_mean_account_age | float | Mean account age in cluster |
| cluster_intent | string | Inferred intent category |
| | | |
| **Investigation Helpers** | | |
| top_coord_partners | list[string] | Top 5 coordination partners |
| top_coord_content | list[string] | Top 5 coordinated content fingerprints |
| top_repost_targets | list[string] | Top 5 repost targets |
| top_shared_urls | list[string] | Top 5 URLs shared |

#### cluster_summaries/{date}.parquet

Aggregate cluster-level statistics for review.

| Column | Type | Description |
|--------|------|-------------|
| cluster_id | int | Cluster identifier |
| member_count | int | Number of accounts |
| bot_likelihood_mean | float | Mean bot likelihood of members |
| bot_likelihood_std | float | Std dev of bot likelihood |
| | | |
| **Aggregate Features** | | |
| mean_account_age_days | float | Mean account age |
| std_account_age_days | float | Std dev account age |
| mean_coordination_events | float | Mean coordination events |
| mean_posts_per_hour | float | Mean posting rate |
| mean_repost_ratio | float | Mean repost ratio |
| mean_hourly_entropy | float | Mean hourly entropy |
| mean_inter_post_entropy | float | Mean inter-post time entropy |
| mean_top_target_share | float | Mean target concentration |
| | | |
| **Shared Targets** | | |
| top_shared_repost_targets | list[string] | DIDs most members repost |
| top_shared_reply_targets | list[string] | DIDs most members reply to |
| top_shared_content | list[string] | Content most members shared |
| top_shared_urls | list[string] | URLs most members shared |
| | | |
| **Classification** | | |
| intent_category | string | 'amplification', 'spam', 'harassment', 'engagement_farming', 'unknown' |
| intent_confidence | float | Confidence in intent classification |
| reviewed | bool | Whether human has reviewed |
| review_notes | string | Human reviewer notes |
| created_at | timestamp[us] | When cluster first detected |
| updated_at | timestamp[us] | Last update |

---

## Feature Computation Details

### Temporal Features

```sql
-- Computed from events table
WITH hourly_counts AS (
    SELECT 
        actor_did,
        DATE_TRUNC('hour', timestamp) as hour,
        COUNT(*) as posts
    FROM events
    WHERE event_type = 'post'
      AND timestamp >= :start_date
    GROUP BY actor_did, DATE_TRUNC('hour', timestamp)
),
inter_post_times AS (
    SELECT 
        actor_did,
        EXTRACT(EPOCH FROM (timestamp - LAG(timestamp) OVER (
            PARTITION BY actor_did ORDER BY timestamp
        ))) as gap_seconds
    FROM events
    WHERE event_type = 'post'
      AND timestamp >= :start_date
)
SELECT
    actor_did as did,
    -- Volume
    COUNT(*) as total_posts,
    AVG(posts) as posts_per_hour_mean,
    STDDEV(posts) as posts_per_hour_std,
    MAX(posts) as posts_per_hour_max,
    
    -- Activity spread
    COUNT(DISTINCT EXTRACT(HOUR FROM hour)) as active_hours_count,
    COUNT(DISTINCT DATE(hour)) as active_days_count,
    
    -- Entropy (using histogram approach)
    -- hourly_entropy computed in Python from hourly distribution
    
    -- Night activity
    SUM(CASE WHEN EXTRACT(HOUR FROM hour) BETWEEN 0 AND 5 THEN posts ELSE 0 END) * 1.0 /
        NULLIF(SUM(posts), 0) as night_post_ratio,
    
    -- Inter-post timing
    AVG(gap_seconds) as inter_post_time_mean,
    STDDEV(gap_seconds) as inter_post_time_std,
    MIN(gap_seconds) as inter_post_time_min,
    
    -- Burstiness
    (STDDEV(gap_seconds) - AVG(gap_seconds)) / 
        NULLIF(STDDEV(gap_seconds) + AVG(gap_seconds), 0) as burstiness
    
FROM hourly_counts
JOIN inter_post_times USING (actor_did)
GROUP BY actor_did
```

### Content Features

```sql
-- Computed from posts table
WITH word_stats AS (
    SELECT
        author_did,
        post_uri,
        LENGTH(text) as text_length,
        ARRAY_LENGTH(STRING_SPLIT(LOWER(text), ' ')) as word_count,
        -- unique words computed in Python
        ARRAY_LENGTH(urls) as url_count,
        ARRAY_LENGTH(hashtags) as hashtag_count,
        ARRAY_LENGTH(mentions) as mention_count,
        has_images,
        text LIKE '%?%' as has_question,
        text ~ '[\x{1F600}-\x{1F64F}]' as has_emoji  -- emoji detection
    FROM posts
    WHERE created_at >= :start_date
)
SELECT
    author_did as did,
    
    -- Length stats
    AVG(text_length) as mean_post_length,
    STDDEV(text_length) as std_post_length,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY text_length) as median_post_length,
    
    -- Hashtags
    AVG(hashtag_count) as hashtag_per_post_mean,
    SUM(hashtag_count) as hashtag_total,
    -- unique_hashtags computed separately
    
    -- Mentions
    AVG(mention_count) as mention_per_post_mean,
    SUM(mention_count) as mention_total,
    
    -- Links
    SUM(CASE WHEN url_count > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as link_ratio,
    SUM(url_count) as link_total,
    
    -- Images
    SUM(CASE WHEN has_images THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as image_ratio,
    
    -- Emoji and questions
    SUM(CASE WHEN has_emoji THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as emoji_ratio,
    SUM(CASE WHEN has_question THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as question_ratio

FROM word_stats
GROUP BY author_did
```

### Behavioral Features

```sql
-- Computed from events table
WITH action_counts AS (
    SELECT
        actor_did,
        COUNT(*) as total_actions,
        COUNT(*) FILTER (WHERE event_type = 'post') as posts,
        COUNT(*) FILTER (WHERE event_type = 'repost') as reposts,
        COUNT(*) FILTER (WHERE event_type = 'like') as likes,
        COUNT(*) FILTER (WHERE event_type = 'reply') as replies,
        COUNT(*) FILTER (WHERE event_type = 'quote') as quotes
    FROM events
    WHERE timestamp >= :start_date
    GROUP BY actor_did
),
target_stats AS (
    SELECT
        actor_did,
        event_type,
        target_did,
        COUNT(*) as cnt
    FROM events
    WHERE timestamp >= :start_date
      AND target_did IS NOT NULL
    GROUP BY actor_did, event_type, target_did
),
target_entropy AS (
    SELECT
        actor_did,
        event_type,
        COUNT(DISTINCT target_did) as unique_targets,
        MAX(cnt) * 1.0 / SUM(cnt) as top_target_share,
        SUM(POWER(cnt * 1.0 / SUM(cnt) OVER (PARTITION BY actor_did, event_type), 2)) as hhi
    FROM target_stats
    GROUP BY actor_did, event_type
)
SELECT
    a.actor_did as did,
    
    -- Ratios
    a.reposts * 1.0 / NULLIF(a.total_actions, 0) as repost_ratio,
    a.likes * 1.0 / NULLIF(a.total_actions, 0) as like_ratio,
    a.replies * 1.0 / NULLIF(a.posts, 0) as reply_ratio,
    a.quotes * 1.0 / NULLIF(a.posts, 0) as quote_ratio,
    (a.posts - a.replies - a.quotes) * 1.0 / NULLIF(a.posts, 0) as original_post_ratio,
    a.likes * 1.0 / NULLIF(a.posts, 0) as like_to_post_ratio,
    
    -- Target diversity (from target_entropy, pivoted)
    t_repost.unique_targets as unique_repost_sources,
    t_reply.unique_targets as unique_reply_targets,
    t_like.unique_targets as unique_like_targets,
    
    -- Concentration
    t_repost.top_target_share as top_repost_source_share,
    t_reply.top_target_share as top_reply_target_share,
    t_like.top_target_share as top_like_target_share,
    t_repost.hhi as repost_source_hhi,
    t_reply.hhi as reply_target_hhi,
    t_like.hhi as like_target_hhi

FROM action_counts a
LEFT JOIN target_entropy t_repost 
    ON a.actor_did = t_repost.actor_did AND t_repost.event_type = 'repost'
LEFT JOIN target_entropy t_reply 
    ON a.actor_did = t_reply.actor_did AND t_reply.event_type = 'reply'
LEFT JOIN target_entropy t_like 
    ON a.actor_did = t_like.actor_did AND t_like.event_type = 'like'
```

### Coordination Features

```sql
-- Computed from coordination_events table
WITH coord_stats AS (
    SELECT
        did,
        partner,
        content_type,
        COUNT(*) as event_count,
        AVG(time_delta_sec) as mean_delta,
        MIN(time_delta_sec) as min_delta
    FROM (
        SELECT account_a as did, account_b as partner, content_type, time_delta_sec
        FROM coordination_events
        UNION ALL
        SELECT account_b as did, account_a as partner, content_type, time_delta_sec
        FROM coordination_events
    )
    GROUP BY did, partner, content_type
)
SELECT
    did,
    
    -- Volume
    SUM(event_count) as coordination_event_count,
    COUNT(DISTINCT partner) as unique_coord_partners,
    
    -- Concentration
    SUM(POWER(event_count * 1.0 / SUM(event_count) OVER (PARTITION BY did), 2)) as coord_partner_concentration,
    
    -- Timing
    AVG(mean_delta) as mean_coord_time_delta,
    MIN(min_delta) as min_coord_time_delta,
    
    -- By type
    SUM(event_count) FILTER (WHERE content_type = 'url') as url_coordination_count,
    SUM(event_count) FILTER (WHERE content_type = 'repost') as repost_coordination_count,
    SUM(event_count) FILTER (WHERE content_type = 'text') as text_coordination_count,
    
    -- Repeat partners
    SUM(CASE WHEN event_count >= 3 THEN 1 ELSE 0 END) * 1.0 / 
        NULLIF(COUNT(DISTINCT partner), 0) as repeat_coord_partner_ratio

FROM coord_stats
GROUP BY did
```

### Account Metadata Features

```sql
-- Computed from accounts table
SELECT
    did,
    
    -- Age
    DATE_DIFF('day', created_at, CURRENT_DATE) as account_age_days,
    DATE_DIFF('day', first_seen, CURRENT_DATE) as days_since_first_seen,
    
    -- Handle analysis
    LENGTH(handle) as handle_length,
    LENGTH(REGEXP_REPLACE(handle, '[^0-9]', '', 'g')) * 1.0 / 
        NULLIF(LENGTH(handle), 0) as handle_digit_ratio,
    -- handle_entropy computed in Python
    
    -- Profile completeness
    has_avatar,
    bio IS NOT NULL AND LENGTH(bio) > 0 as has_bio,
    COALESCE(LENGTH(bio), 0) as bio_length,
    COALESCE(LENGTH(display_name), 0) as display_name_length,
    
    -- Activity rate
    COALESCE(post_count, 0) * 1.0 / 
        NULLIF(DATE_DIFF('day', created_at, CURRENT_DATE), 0) as posts_per_day_lifetime

FROM accounts
```

---

## Job Implementations

### Job 1: Content Fingerprinting (Daily)

```python
# jobs/1_fingerprint.py

import duckdb
import hashlib
import re
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

DATA_DIR = Path("data")

TRACKING_PARAMS = {
    'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
    'fbclid', 'gclid', 'ref', 'source', 'ref_src', 'ref_url', 'mc_eid'
}


def normalize_url(url: str) -> str:
    """Normalize URL for comparison."""
    if not url:
        return ""
    try:
        parsed = urlparse(url.lower().strip())
        query = parse_qs(parsed.query)
        filtered = {k: v for k, v in query.items() if k.lower() not in TRACKING_PARAMS}
        netloc = parsed.netloc.removeprefix('www.')
        path = parsed.path.rstrip('/')
        return urlunparse((parsed.scheme, netloc, path, '', urlencode(filtered, doseq=True), ''))
    except:
        return url


def normalize_text(text: str) -> str:
    """Normalize text for comparison."""
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r'https?://\S+', '', text)  # Remove URLs
    text = re.sub(r'@[\w.]+', '', text)        # Remove mentions
    text = re.sub(r'\s+', ' ', text).strip()   # Normalize whitespace
    return text


def fingerprint(content: str) -> str:
    """SHA256 hash of content."""
    if not content:
        return ""
    return hashlib.sha256(content.encode()).hexdigest()[:16]


def run(target_date: date):
    con = duckdb.connect()
    
    # Register Python functions
    con.create_function("normalize_url", normalize_url)
    con.create_function("normalize_text", normalize_text)
    con.create_function("fingerprint", fingerprint)
    
    posts_path = DATA_DIR / f"posts/{target_date}.parquet"
    events_path = DATA_DIR / f"events/{target_date}.parquet"
    output_path = DATA_DIR / f"content_shares/{target_date}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    query = f"""
    COPY (
        -- URL shares
        SELECT 
            author_did as actor_did,
            'url' as content_type,
            fingerprint(normalize_url(url)) as fingerprint,
            url as content_raw,
            post_uri,
            created_at
        FROM read_parquet('{posts_path}'),
        LATERAL UNNEST(urls) as t(url)
        WHERE urls IS NOT NULL AND LEN(urls) > 0
          AND LEN(url) > 0
        
        UNION ALL
        
        -- Reposts
        SELECT
            actor_did,
            'repost' as content_type,
            target_post_uri as fingerprint,
            target_post_uri as content_raw,
            NULL as post_uri,
            timestamp as created_at
        FROM read_parquet('{events_path}')
        WHERE event_type = 'repost'
          AND target_post_uri IS NOT NULL
        
        UNION ALL
        
        -- Quote posts
        SELECT
            author_did as actor_did,
            'quote' as content_type,
            quote_uri as fingerprint,
            quote_uri as content_raw,
            post_uri,
            created_at
        FROM read_parquet('{posts_path}')
        WHERE quote_uri IS NOT NULL
        
        UNION ALL
        
        -- Text content (for exact/near duplicate detection)
        SELECT
            author_did as actor_did,
            'text' as content_type,
            fingerprint(normalize_text(text)) as fingerprint,
            LEFT(text, 100) as content_raw,
            post_uri,
            created_at
        FROM read_parquet('{posts_path}')
        WHERE LENGTH(text) > 50
          AND reply_parent IS NULL  -- Original posts only
    )
    TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    
    con.execute(query)
    
    count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()[0]
    print(f"Fingerprinting complete for {target_date}: {count:,} content shares")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        target = date.fromisoformat(sys.argv[1])
    else:
        target = date.today() - timedelta(days=1)
    run(target)
```

### Job 2: Coordination Detection (Daily)

```python
# jobs/2_coordination.py

import duckdb
from datetime import date, timedelta
from pathlib import Path

DATA_DIR = Path("data")
WINDOW_HOURS = 2
LOOKBACK_DAYS = 7


def run(target_date: date):
    con = duckdb.connect()
    
    start_date = target_date - timedelta(days=LOOKBACK_DAYS)
    output_path = DATA_DIR / f"coordination_events/{target_date}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    query = f"""
    COPY (
        WITH shares AS (
            SELECT * FROM read_parquet('{DATA_DIR}/content_shares/*.parquet')
            WHERE created_at >= '{start_date}'
              AND fingerprint IS NOT NULL
              AND fingerprint != ''
        )
        SELECT 
            a.fingerprint,
            a.content_type,
            a.content_raw,
            a.actor_did as account_a,
            b.actor_did as account_b,
            a.created_at as time_a,
            b.created_at as time_b,
            CAST(EXTRACT(EPOCH FROM (b.created_at - a.created_at)) AS INTEGER) as time_delta_sec
        FROM shares a
        JOIN shares b 
            ON a.fingerprint = b.fingerprint
            AND a.content_type = b.content_type
            AND a.actor_did < b.actor_did
            AND b.created_at >= a.created_at
            AND b.created_at <= a.created_at + INTERVAL '{WINDOW_HOURS} hours'
    )
    TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    
    con.execute(query)
    
    # Summary stats
    summary = con.execute(f"""
        SELECT 
            COUNT(*) as pairs,
            COUNT(DISTINCT fingerprint) as unique_content,
            COUNT(DISTINCT account_a) as unique_a,
            COUNT(DISTINCT account_b) as unique_b
        FROM read_parquet('{output_path}')
    """).fetchone()
    
    unique_accounts = summary[2] + summary[3]  # Approximate (some overlap)
    print(f"Coordination detection for {target_date}:")
    print(f"  Pairs: {summary[0]:,}")
    print(f"  Unique content: {summary[1]:,}")
    print(f"  Unique accounts: ~{unique_accounts:,}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        target = date.fromisoformat(sys.argv[1])
    else:
        target = date.today() - timedelta(days=1)
    run(target)
```

### Job 3: Feature Computation (Daily)

```python
# jobs/3_features.py

import duckdb
import numpy as np
from datetime import date, timedelta
from pathlib import Path
from scipy.stats import entropy

DATA_DIR = Path("data")
WINDOW_DAYS = 7


def compute_entropy(counts: list) -> float:
    """Compute entropy from a list of counts."""
    if not counts or sum(counts) == 0:
        return 0.0
    probs = np.array(counts) / sum(counts)
    return float(entropy(probs))


def compute_handle_entropy(handle: str) -> float:
    """Compute character entropy of handle."""
    if not handle:
        return 0.0
    counts = {}
    for c in handle.lower():
        counts[c] = counts.get(c, 0) + 1
    return compute_entropy(list(counts.values()))


def run(target_date: date):
    con = duckdb.connect()
    
    # Register Python UDFs
    con.create_function("compute_entropy", compute_entropy)
    con.create_function("handle_entropy", compute_handle_entropy)
    
    start_date = target_date - timedelta(days=WINDOW_DAYS)
    output_path = DATA_DIR / f"features/{target_date}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    query = f"""
    COPY (
        WITH 
        -- ============ TEMPORAL FEATURES ============
        post_events AS (
            SELECT * FROM read_parquet('{DATA_DIR}/events/*.parquet')
            WHERE timestamp >= '{start_date}'
              AND event_type = 'post'
        ),
        hourly_posts AS (
            SELECT 
                actor_did,
                DATE_TRUNC('hour', timestamp) as hour,
                COUNT(*) as posts
            FROM post_events
            GROUP BY actor_did, DATE_TRUNC('hour', timestamp)
        ),
        hourly_distribution AS (
            SELECT
                actor_did,
                EXTRACT(HOUR FROM hour) as hour_of_day,
                SUM(posts) as posts
            FROM hourly_posts
            GROUP BY actor_did, EXTRACT(HOUR FROM hour)
        ),
        hourly_entropy_calc AS (
            SELECT
                actor_did,
                compute_entropy(LIST(posts ORDER BY hour_of_day)) as hourly_entropy
            FROM hourly_distribution
            GROUP BY actor_did
        ),
        inter_post AS (
            SELECT 
                actor_did,
                EXTRACT(EPOCH FROM (timestamp - LAG(timestamp) OVER (
                    PARTITION BY actor_did ORDER BY timestamp
                ))) as gap_sec
            FROM post_events
        ),
        temporal AS (
            SELECT 
                h.actor_did as did,
                SUM(h.posts) as total_posts,
                AVG(h.posts) as posts_per_hour_mean,
                STDDEV(h.posts) as posts_per_hour_std,
                MAX(h.posts) as posts_per_hour_max,
                COUNT(DISTINCT EXTRACT(HOUR FROM h.hour)) as active_hours_count,
                COUNT(DISTINCT DATE(h.hour)) as active_days_count,
                e.hourly_entropy,
                SUM(CASE WHEN EXTRACT(HOUR FROM h.hour) BETWEEN 0 AND 5 
                    THEN h.posts ELSE 0 END) * 1.0 / NULLIF(SUM(h.posts), 0) as night_post_ratio,
                AVG(i.gap_sec) as inter_post_time_mean,
                STDDEV(i.gap_sec) as inter_post_time_std,
                MIN(i.gap_sec) as inter_post_time_min,
                (STDDEV(i.gap_sec) - AVG(i.gap_sec)) / 
                    NULLIF(STDDEV(i.gap_sec) + AVG(i.gap_sec), 0) as burstiness
            FROM hourly_posts h
            LEFT JOIN hourly_entropy_calc e ON h.actor_did = e.actor_did
            LEFT JOIN inter_post i ON h.actor_did = i.actor_did
            GROUP BY h.actor_did, e.hourly_entropy
            HAVING SUM(h.posts) >= 5
        ),
        
        -- ============ CONTENT FEATURES ============
        posts AS (
            SELECT * FROM read_parquet('{DATA_DIR}/posts/*.parquet')
            WHERE created_at >= '{start_date}'
        ),
        content AS (
            SELECT
                author_did as did,
                AVG(LENGTH(text)) as mean_post_length,
                STDDEV(LENGTH(text)) as std_post_length,
                AVG(COALESCE(ARRAY_LENGTH(hashtags), 0)) as hashtag_per_post_mean,
                SUM(COALESCE(ARRAY_LENGTH(hashtags), 0)) as hashtag_total,
                AVG(COALESCE(ARRAY_LENGTH(mentions), 0)) as mention_per_post_mean,
                SUM(CASE WHEN urls IS NOT NULL AND ARRAY_LENGTH(urls) > 0 
                    THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as link_ratio,
                SUM(CASE WHEN has_images THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as image_ratio,
                SUM(CASE WHEN text LIKE '%?%' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as question_ratio
            FROM posts
            GROUP BY author_did
        ),
        
        -- ============ BEHAVIORAL FEATURES ============
        all_events AS (
            SELECT * FROM read_parquet('{DATA_DIR}/events/*.parquet')
            WHERE timestamp >= '{start_date}'
        ),
        action_counts AS (
            SELECT
                actor_did as did,
                COUNT(*) as total_actions,
                COUNT(*) FILTER (WHERE event_type = 'post') as posts,
                COUNT(*) FILTER (WHERE event_type = 'repost') as reposts,
                COUNT(*) FILTER (WHERE event_type = 'like') as likes,
                COUNT(*) FILTER (WHERE event_type = 'reply') as replies,
                COUNT(*) FILTER (WHERE event_type = 'quote') as quotes
            FROM all_events
            GROUP BY actor_did
        ),
        target_counts AS (
            SELECT
                actor_did as did,
                event_type,
                target_did,
                COUNT(*) as cnt
            FROM all_events
            WHERE target_did IS NOT NULL
            GROUP BY actor_did, event_type, target_did
        ),
        target_agg AS (
            SELECT
                did,
                event_type,
                COUNT(DISTINCT target_did) as unique_targets,
                MAX(cnt) * 1.0 / NULLIF(SUM(cnt), 0) as top_target_share,
                SUM(POWER(cnt * 1.0 / NULLIF(SUM(cnt) OVER (PARTITION BY did, event_type), 0), 2)) as hhi
            FROM target_counts
            GROUP BY did, event_type
        ),
        behavioral AS (
            SELECT
                a.did,
                a.total_actions,
                a.reposts * 1.0 / NULLIF(a.total_actions, 0) as repost_ratio,
                a.likes * 1.0 / NULLIF(a.total_actions, 0) as like_ratio,
                a.replies * 1.0 / NULLIF(a.posts, 0) as reply_ratio,
                a.quotes * 1.0 / NULLIF(a.posts, 0) as quote_ratio,
                (a.posts - COALESCE(a.replies, 0) - COALESCE(a.quotes, 0)) * 1.0 / 
                    NULLIF(a.posts, 0) as original_post_ratio,
                a.likes * 1.0 / NULLIF(a.posts, 0) as like_to_post_ratio,
                tr.unique_targets as unique_repost_sources,
                tp.unique_targets as unique_reply_targets,
                tl.unique_targets as unique_like_targets,
                tr.top_target_share as top_repost_source_share,
                tp.top_target_share as top_reply_target_share,
                tl.top_target_share as top_like_target_share,
                tr.hhi as repost_source_hhi,
                tp.hhi as reply_target_hhi,
                tl.hhi as like_target_hhi
            FROM action_counts a
            LEFT JOIN target_agg tr ON a.did = tr.did AND tr.event_type = 'repost'
            LEFT JOIN target_agg tp ON a.did = tp.did AND tp.event_type = 'reply'
            LEFT JOIN target_agg tl ON a.did = tl.did AND tl.event_type = 'like'
        ),
        
        -- ============ COORDINATION FEATURES ============
        coord_raw AS (
            SELECT account_a as did, account_b as partner, content_type, time_delta_sec
            FROM read_parquet('{DATA_DIR}/coordination_events/*.parquet')
            UNION ALL
            SELECT account_b as did, account_a as partner, content_type, time_delta_sec
            FROM read_parquet('{DATA_DIR}/coordination_events/*.parquet')
        ),
        coord_by_partner AS (
            SELECT
                did,
                partner,
                COUNT(*) as event_count,
                AVG(time_delta_sec) as mean_delta,
                MIN(time_delta_sec) as min_delta
            FROM coord_raw
            GROUP BY did, partner
        ),
        coordination AS (
            SELECT
                did,
                SUM(event_count) as coordination_event_count,
                COUNT(DISTINCT partner) as unique_coord_partners,
                AVG(mean_delta) as mean_coord_time_delta,
                MIN(min_delta) as min_coord_time_delta,
                SUM(CASE WHEN event_count >= 3 THEN 1 ELSE 0 END) * 1.0 / 
                    NULLIF(COUNT(DISTINCT partner), 0) as repeat_coord_partner_ratio
            FROM coord_by_partner
            GROUP BY did
        ),
        coord_by_type AS (
            SELECT
                did,
                SUM(CASE WHEN content_type = 'url' THEN 1 ELSE 0 END) as url_coordination_count,
                SUM(CASE WHEN content_type = 'repost' THEN 1 ELSE 0 END) as repost_coordination_count,
                SUM(CASE WHEN content_type = 'text' THEN 1 ELSE 0 END) as text_coordination_count
            FROM coord_raw
            GROUP BY did
        ),
        
        -- ============ ACCOUNT METADATA ============
        accounts AS (
            SELECT * FROM read_parquet('{DATA_DIR}/accounts/current.parquet')
        ),
        metadata AS (
            SELECT
                did,
                DATE_DIFF('day', created_at, CURRENT_DATE) as account_age_days,
                DATE_DIFF('day', first_seen, CURRENT_DATE) as days_since_first_seen,
                LENGTH(handle) as handle_length,
                LENGTH(REGEXP_REPLACE(handle, '[^0-9]', '', 'g')) * 1.0 / 
                    NULLIF(LENGTH(handle), 0) as handle_digit_ratio,
                handle_entropy(handle) as handle_entropy,
                has_avatar,
                bio IS NOT NULL AND LENGTH(bio) > 0 as has_bio,
                COALESCE(LENGTH(bio), 0) as bio_length,
                COALESCE(LENGTH(display_name), 0) as display_name_length
            FROM accounts
        )
        
        -- ============ FINAL JOIN ============
        SELECT 
            t.did,
            {WINDOW_DAYS} as window_days,
            CURRENT_TIMESTAMP as computed_at,
            
            -- Temporal
            t.total_posts,
            t.posts_per_hour_mean,
            t.posts_per_hour_std,
            t.posts_per_hour_max,
            t.active_hours_count,
            t.active_days_count,
            t.hourly_entropy,
            t.night_post_ratio,
            t.inter_post_time_mean,
            t.inter_post_time_std,
            t.inter_post_time_min,
            t.burstiness,
            
            -- Content
            c.mean_post_length,
            c.std_post_length,
            c.hashtag_per_post_mean,
            c.hashtag_total,
            c.mention_per_post_mean,
            c.link_ratio,
            c.image_ratio,
            c.question_ratio,
            
            -- Behavioral
            COALESCE(b.total_actions, 0) as total_actions,
            COALESCE(b.repost_ratio, 0) as repost_ratio,
            COALESCE(b.like_ratio, 0) as like_ratio,
            COALESCE(b.reply_ratio, 0) as reply_ratio,
            COALESCE(b.quote_ratio, 0) as quote_ratio,
            COALESCE(b.original_post_ratio, 0) as original_post_ratio,
            COALESCE(b.like_to_post_ratio, 0) as like_to_post_ratio,
            COALESCE(b.unique_repost_sources, 0) as unique_repost_sources,
            COALESCE(b.unique_reply_targets, 0) as unique_reply_targets,
            COALESCE(b.unique_like_targets, 0) as unique_like_targets,
            COALESCE(b.top_repost_source_share, 0) as top_repost_source_share,
            COALESCE(b.top_reply_target_share, 0) as top_reply_target_share,
            COALESCE(b.top_like_target_share, 0) as top_like_target_share,
            COALESCE(b.repost_source_hhi, 0) as repost_source_hhi,
            COALESCE(b.reply_target_hhi, 0) as reply_target_hhi,
            COALESCE(b.like_target_hhi, 0) as like_target_hhi,
            
            -- Coordination
            COALESCE(co.coordination_event_count, 0) as coordination_event_count,
            COALESCE(co.unique_coord_partners, 0) as unique_coord_partners,
            COALESCE(co.mean_coord_time_delta, 0) as mean_coord_time_delta,
            COALESCE(co.min_coord_time_delta, 0) as min_coord_time_delta,
            COALESCE(co.repeat_coord_partner_ratio, 0) as repeat_coord_partner_ratio,
            COALESCE(ct.url_coordination_count, 0) as url_coordination_count,
            COALESCE(ct.repost_coordination_count, 0) as repost_coordination_count,
            COALESCE(ct.text_coordination_count, 0) as text_coordination_count,
            
            -- Metadata
            COALESCE(m.account_age_days, 0) as account_age_days,
            COALESCE(m.days_since_first_seen, 0) as days_since_first_seen,
            COALESCE(m.handle_length, 0) as handle_length,
            COALESCE(m.handle_digit_ratio, 0) as handle_digit_ratio,
            COALESCE(m.handle_entropy, 0) as handle_entropy,
            COALESCE(m.has_avatar, FALSE) as has_avatar,
            COALESCE(m.has_bio, FALSE) as has_bio,
            COALESCE(m.bio_length, 0) as bio_length,
            COALESCE(m.display_name_length, 0) as display_name_length
            
        FROM temporal t
        LEFT JOIN content c ON t.did = c.did
        LEFT JOIN behavioral b ON t.did = b.did
        LEFT JOIN coordination co ON t.did = co.did
        LEFT JOIN coord_by_type ct ON t.did = ct.did
        LEFT JOIN metadata m ON t.did = m.did
    )
    TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    
    con.execute(query)
    
    count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()[0]
    print(f"Feature computation complete for {target_date}: {count:,} accounts")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        target = date.fromisoformat(sys.argv[1])
    else:
        target = date.today() - timedelta(days=1)
    run(target)
```

### Job 4: Clustering (Daily)

```python
# jobs/4_cluster.py

import duckdb
import numpy as np
import pandas as pd
from datetime import date, timedelta
from pathlib import Path
from sklearn.preprocessing import RobustScaler
from hdbscan import HDBSCAN
from umap import UMAP

DATA_DIR = Path("data")

# Features to use for clustering (subset of all features)
CLUSTER_FEATURES = [
    # Temporal
    'posts_per_hour_mean', 'posts_per_hour_std', 'active_hours_count',
    'hourly_entropy', 'inter_post_time_mean', 'inter_post_time_std', 'burstiness',
    
    # Content
    'mean_post_length', 'std_post_length', 'hashtag_per_post_mean',
    'link_ratio', 'image_ratio',
    
    # Behavioral
    'repost_ratio', 'like_ratio', 'reply_ratio', 'original_post_ratio',
    'unique_repost_sources', 'unique_reply_targets',
    'top_repost_source_share', 'top_reply_target_share',
    'repost_source_hhi', 'reply_target_hhi',
    
    # Coordination (heavily weighted)
    'coordination_event_count', 'unique_coord_partners',
    'mean_coord_time_delta', 'min_coord_time_delta',
    'url_coordination_count', 'repost_coordination_count',
    
    # Metadata
    'account_age_days', 'handle_entropy', 'handle_digit_ratio',
]

# Features to log-transform before scaling
LOG_FEATURES = [
    'posts_per_hour_mean', 'posts_per_hour_std', 'inter_post_time_mean', 'inter_post_time_std',
    'coordination_event_count', 'unique_coord_partners',
    'mean_coord_time_delta', 'min_coord_time_delta',
    'unique_repost_sources', 'unique_reply_targets',
    'account_age_days', 'hashtag_per_post_mean',
]


def compute_bot_likelihood(cluster_df: pd.DataFrame, all_df: pd.DataFrame) -> float:
    """
    Compute bot likelihood score for a cluster based on suspicious indicators.
    Returns score 0-1 where higher = more likely bot.
    """
    scores = []
    
    # Low hourly entropy (regular posting pattern)
    mean_entropy = cluster_df['hourly_entropy'].mean()
    all_mean = all_df['hourly_entropy'].mean()
    if mean_entropy < all_mean * 0.7:
        scores.append(0.8)
    elif mean_entropy < all_mean * 0.9:
        scores.append(0.4)
    else:
        scores.append(0.1)
    
    # High coordination events
    mean_coord = cluster_df['coordination_event_count'].mean()
    if mean_coord > 10:
        scores.append(0.9)
    elif mean_coord > 5:
        scores.append(0.6)
    elif mean_coord > 1:
        scores.append(0.3)
    else:
        scores.append(0.1)
    
    # High target concentration
    mean_hhi = cluster_df['repost_source_hhi'].mean()
    if mean_hhi > 0.5:
        scores.append(0.8)
    elif mean_hhi > 0.3:
        scores.append(0.5)
    else:
        scores.append(0.2)
    
    # Young accounts
    mean_age = cluster_df['account_age_days'].mean()
    if mean_age < 30:
        scores.append(0.7)
    elif mean_age < 90:
        scores.append(0.4)
    else:
        scores.append(0.2)
    
    # High repost ratio
    mean_repost = cluster_df['repost_ratio'].mean()
    if mean_repost > 0.7:
        scores.append(0.8)
    elif mean_repost > 0.5:
        scores.append(0.5)
    else:
        scores.append(0.2)
    
    return np.mean(scores)


def infer_intent(cluster_df: pd.DataFrame) -> str:
    """Infer cluster intent based on behavioral patterns."""
    mean_repost = cluster_df['repost_ratio'].mean()
    mean_reply = cluster_df['reply_ratio'].mean()
    mean_link = cluster_df['link_ratio'].mean()
    mean_hhi = cluster_df['repost_source_hhi'].mean()
    
    if mean_repost > 0.6 and mean_hhi > 0.4:
        return 'amplification'
    elif mean_link > 0.5:
        return 'spam'
    elif mean_reply > 0.6:
        return 'engagement'
    else:
        return 'unknown'


def run(target_date: date):
    con = duckdb.connect()
    
    features_path = DATA_DIR / f"features/{target_date}.parquet"
    output_path = DATA_DIR / f"clusters/{target_date}.parquet"
    summary_path = DATA_DIR / f"cluster_summaries/{target_date}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Load features
    df = con.execute(f"SELECT * FROM read_parquet('{features_path}')").fetchdf()
    print(f"Loaded {len(df):,} accounts")
    
    # Filter to accounts with minimum activity
    df = df[df['total_posts'] >= 5].copy()
    print(f"After filtering: {len(df):,} accounts")
    
    if len(df) < 100:
        print("Not enough accounts for clustering")
        return
    
    # Extract feature matrix
    X = df[CLUSTER_FEATURES].fillna(0).values.copy()
    
    # Log transform
    for i, col in enumerate(CLUSTER_FEATURES):
        if col in LOG_FEATURES:
            X[:, i] = np.log1p(X[:, i])
    
    # Scale
    scaler = RobustScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Dimensionality reduction
    print("Running UMAP...")
    reducer = UMAP(n_components=15, metric='cosine', min_dist=0.1, random_state=42)
    X_reduced = reducer.fit_transform(X_scaled)
    
    # Cluster
    print("Running HDBSCAN...")
    clusterer = HDBSCAN(min_cluster_size=20, min_samples=5, metric='euclidean')
    labels = clusterer.fit_predict(X_reduced)
    
    # Add cluster info to dataframe
    df['cluster_id'] = labels
    df['cluster_probability'] = clusterer.probabilities_
    df['outlier_score'] = clusterer.outlier_scores_
    
    # Compute bot likelihood per cluster
    bot_scores = {}
    intents = {}
    sizes = {}
    
    for cluster_id in sorted(set(labels)):
        if cluster_id == -1:
            continue
        cluster_df = df[df['cluster_id'] == cluster_id]
        bot_scores[cluster_id] = compute_bot_likelihood(cluster_df, df)
        intents[cluster_id] = infer_intent(cluster_df)
        sizes[cluster_id] = len(cluster_df)
    
    df['bot_likelihood'] = df['cluster_id'].map(bot_scores).fillna(0)
    df['cluster_intent'] = df['cluster_id'].map(intents).fillna('noise')
    df['cluster_size'] = df['cluster_id'].map(sizes).fillna(0)
    
    # Get top coordination partners and content per account
    top_partners = con.execute(f"""
        SELECT 
            did,
            ARRAY_AGG(partner ORDER BY cnt DESC)[:5] as top_coord_partners
        FROM (
            SELECT account_a as did, account_b as partner, COUNT(*) as cnt
            FROM read_parquet('{DATA_DIR}/coordination_events/*.parquet')
            GROUP BY account_a, account_b
            UNION ALL
            SELECT account_b as did, account_a as partner, COUNT(*) as cnt
            FROM read_parquet('{DATA_DIR}/coordination_events/*.parquet')
            GROUP BY account_b, account_a
        )
        GROUP BY did
    """).fetchdf()
    
    top_content = con.execute(f"""
        SELECT 
            actor_did as did,
            ARRAY_AGG(fingerprint ORDER BY cnt DESC)[:5] as top_coord_content
        FROM (
            SELECT actor_did, fingerprint, COUNT(*) as cnt
            FROM read_parquet('{DATA_DIR}/content_shares/*.parquet')
            GROUP BY actor_did, fingerprint
        )
        GROUP BY actor_did
    """).fetchdf()
    
    top_targets = con.execute(f"""
        SELECT
            actor_did as did,
            ARRAY_AGG(target_did ORDER BY cnt DESC)[:5] as top_repost_targets
        FROM (
            SELECT actor_did, target_did, COUNT(*) as cnt
            FROM read_parquet('{DATA_DIR}/events/*.parquet')
            WHERE event_type = 'repost' AND target_did IS NOT NULL
            GROUP BY actor_did, target_did
        )
        GROUP BY actor_did
    """).fetchdf()
    
    df = df.merge(top_partners, on='did', how='left')
    df = df.merge(top_content, on='did', how='left')
    df = df.merge(top_targets, on='did', how='left')
    
    # Select output columns
    output_cols = [
        'did', 'cluster_id', 'cluster_probability', 'outlier_score', 'bot_likelihood',
        'cluster_size', 'cluster_intent',
        'top_coord_partners', 'top_coord_content', 'top_repost_targets'
    ]
    
    output_df = df[output_cols].copy()
    output_df.to_parquet(output_path, compression='zstd')
    
    # Create cluster summary
    summaries = []
    for cluster_id in sorted(set(labels)):
        if cluster_id == -1:
            continue
        cluster_df = df[df['cluster_id'] == cluster_id]
        summaries.append({
            'cluster_id': cluster_id,
            'member_count': len(cluster_df),
            'bot_likelihood_mean': cluster_df['bot_likelihood'].mean(),
            'mean_account_age_days': cluster_df['account_age_days'].mean(),
            'mean_coordination_events': cluster_df['coordination_event_count'].mean(),
            'mean_posts_per_hour': cluster_df['posts_per_hour_mean'].mean(),
            'mean_repost_ratio': cluster_df['repost_ratio'].mean(),
            'mean_hourly_entropy': cluster_df['hourly_entropy'].mean(),
            'mean_repost_hhi': cluster_df['repost_source_hhi'].mean(),
            'intent_category': intents[cluster_id],
        })
    
    summary_df = pd.DataFrame(summaries)
    summary_df.to_parquet(summary_path, compression='zstd')
    
    # Print results
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise = (labels == -1).sum()
    
    print(f"\nClustering complete:")
    print(f"  Clusters: {n_clusters}")
    print(f"  Noise points: {n_noise:,}")
    
    print(f"\nTop suspicious clusters:")
    for _, row in summary_df.nlargest(5, 'bot_likelihood_mean').iterrows():
        print(f"  Cluster {row['cluster_id']}: {row['member_count']} members, "
              f"bot_score={row['bot_likelihood_mean']:.2f}, "
              f"intent={row['intent_category']}, "
              f"coord_events={row['mean_coordination_events']:.1f}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        target = date.fromisoformat(sys.argv[1])
    else:
        target = date.today() - timedelta(days=1)
    run(target)
```

### Job 5: Embeddings (Weekly)

```python
# jobs/5_embeddings.py

import duckdb
import numpy as np
import pandas as pd
from datetime import date, timedelta
from pathlib import Path
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfTransformer
from scipy.sparse import csr_matrix

DATA_DIR = Path("data")
EMBEDDING_DIM = 64
LOOKBACK_DAYS = 30


def build_interaction_matrix(con, interaction_type: str, start_date: date):
    """Build sparse actor->target interaction matrix."""
    
    query = f"""
        SELECT actor_did, target_did, COUNT(*) as cnt
        FROM read_parquet('{DATA_DIR}/events/*.parquet')
        WHERE event_type = '{interaction_type}'
          AND timestamp >= '{start_date}'
          AND target_did IS NOT NULL
        GROUP BY actor_did, target_did
    """
    
    df = con.execute(query).fetchdf()
    
    if len(df) == 0:
        return None, None, None
    
    # Create mappings
    actors = sorted(df['actor_did'].unique())
    targets = sorted(df['target_did'].unique())
    actor_to_idx = {a: i for i, a in enumerate(actors)}
    target_to_idx = {t: i for i, t in enumerate(targets)}
    
    # Build sparse matrix
    rows = df['actor_did'].map(actor_to_idx).values
    cols = df['target_did'].map(target_to_idx).values
    data = df['cnt'].values
    
    matrix = csr_matrix((data, (rows, cols)), shape=(len(actors), len(targets)))
    
    return matrix, actors, targets


def run(target_date: date):
    con = duckdb.connect()
    
    start_date = target_date - timedelta(days=LOOKBACK_DAYS)
    output_path = DATA_DIR / f"embeddings/{target_date}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    embeddings = []
    
    for interaction_type in ['repost', 'reply', 'like']:
        print(f"Building {interaction_type} embeddings...")
        
        matrix, actors, targets = build_interaction_matrix(con, interaction_type, start_date)
        
        if matrix is None or matrix.shape[0] < 100:
            print(f"  Skipping {interaction_type}: not enough data")
            continue
        
        # TF-IDF transform
        tfidf = TfidfTransformer(norm='l2', use_idf=True)
        matrix_tfidf = tfidf.fit_transform(matrix)
        
        # SVD
        n_components = min(EMBEDDING_DIM, matrix_tfidf.shape[1] - 1, matrix_tfidf.shape[0] - 1)
        svd = TruncatedSVD(n_components=n_components, random_state=42)
        vectors = svd.fit_transform(matrix_tfidf)
        
        # Pad to EMBEDDING_DIM if needed
        if vectors.shape[1] < EMBEDDING_DIM:
            padding = np.zeros((vectors.shape[0], EMBEDDING_DIM - vectors.shape[1]))
            vectors = np.hstack([vectors, padding])
        
        for i, actor in enumerate(actors):
            embeddings.append({
                'did': actor,
                'embedding_type': f'{interaction_type}_target',
                'vector': vectors[i].tolist(),
                'computed_at': pd.Timestamp.now()
            })
        
        print(f"  Generated {len(actors):,} embeddings")
    
    if embeddings:
        df = pd.DataFrame(embeddings)
        df.to_parquet(output_path, compression='zstd')
        print(f"\nSaved {len(df):,} embeddings to {output_path}")
    else:
        print("No embeddings generated")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        target = date.fromisoformat(sys.argv[1])
    else:
        target = date.today() - timedelta(days=1)
    run(target)
```

### Job 6: Network Features (Weekly)

```python
# jobs/6_network.py

import duckdb
import numpy as np
import pandas as pd
import networkx as nx
from datetime import date, timedelta
from pathlib import Path

DATA_DIR = Path("data")
LOOKBACK_DAYS = 30


def run(target_date: date):
    con = duckdb.connect()
    
    start_date = target_date - timedelta(days=LOOKBACK_DAYS)
    output_path = DATA_DIR / f"network_features/{target_date}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Load interactions
    print("Loading interactions...")
    edges_df = con.execute(f"""
        SELECT 
            actor_did as source,
            target_did as target,
            COUNT(*) as weight
        FROM read_parquet('{DATA_DIR}/events/*.parquet')
        WHERE timestamp >= '{start_date}'
          AND target_did IS NOT NULL
          AND event_type IN ('repost', 'reply', 'quote', 'like')
        GROUP BY actor_did, target_did
    """).fetchdf()
    
    print(f"Building graph with {len(edges_df):,} edges...")
    
    # Build graph
    G = nx.DiGraph()
    for _, row in edges_df.iterrows():
        G.add_edge(row['source'], row['target'], weight=row['weight'])
    
    print(f"Graph has {G.number_of_nodes():,} nodes, {G.number_of_edges():,} edges")
    
    # Compute metrics
    print("Computing metrics...")
    
    # Degree
    in_degree = dict(G.in_degree())
    out_degree = dict(G.out_degree())
    weighted_in = dict(G.in_degree(weight='weight'))
    weighted_out = dict(G.out_degree(weight='weight'))
    
    # PageRank
    print("  PageRank...")
    pagerank = nx.pagerank(G, weight='weight', max_iter=100)
    
    # Clustering (on undirected version)
    print("  Clustering coefficient...")
    G_undirected = G.to_undirected()
    clustering = nx.clustering(G_undirected)
    
    # Community detection
    print("  Community detection...")
    try:
        from community import community_louvain
        partition = community_louvain.best_partition(G_undirected)
    except ImportError:
        print("    python-louvain not installed, skipping communities")
        partition = {n: 0 for n in G.nodes()}
    
    # Build output
    records = []
    for node in G.nodes():
        records.append({
            'did': node,
            'in_degree': in_degree.get(node, 0),
            'out_degree': out_degree.get(node, 0),
            'degree_ratio': out_degree.get(node, 0) / max(in_degree.get(node, 0), 1),
            'weighted_in_degree': weighted_in.get(node, 0),
            'weighted_out_degree': weighted_out.get(node, 0),
            'clustering_coefficient': clustering.get(node, 0),
            'pagerank': pagerank.get(node, 0),
            'community_id': partition.get(node, -1),
            'computed_at': pd.Timestamp.now()
        })
    
    df = pd.DataFrame(records)
    
    # Add community sizes
    community_sizes = df['community_id'].value_counts().to_dict()
    df['community_size'] = df['community_id'].map(community_sizes)
    
    df.to_parquet(output_path, compression='zstd')
    print(f"\nSaved {len(df):,} network features to {output_path}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        target = date.fromisoformat(sys.argv[1])
    else:
        target = date.today() - timedelta(days=1)
    run(target)
```

---

## Running the Pipeline

### Makefile

```makefile
DATE := $(shell date -d "yesterday" +%Y-%m-%d)

.PHONY: all daily weekly clean

# Daily pipeline
all: daily

daily: fingerprint coordination features cluster

fingerprint:
	python jobs/1_fingerprint.py $(DATE)

coordination: fingerprint
	python jobs/2_coordination.py $(DATE)

features: coordination
	python jobs/3_features.py $(DATE)

cluster: features
	python jobs/4_cluster.py $(DATE)

# Weekly pipeline
weekly: embeddings network

embeddings:
	python jobs/5_embeddings.py $(DATE)

network:
	python jobs/6_network.py $(DATE)

# Investigation
inspect-cluster:
	@python -c "import duckdb; print(duckdb.query(\"SELECT * FROM 'data/clusters/$(DATE).parquet' WHERE cluster_id = $(CLUSTER)\").fetchdf().to_string())"

top-clusters:
	@python -c "import duckdb; print(duckdb.query(\"SELECT * FROM 'data/cluster_summaries/$(DATE).parquet' ORDER BY bot_likelihood_mean DESC LIMIT 10\").fetchdf().to_string())"

# Cleanup
clean:
	find data/events -name "*.parquet" -mtime +30 -delete
	find data/posts -name "*.parquet" -mtime +30 -delete
```

### Crontab

```cron
# Daily pipeline at 3am
0 3 * * * cd /path/to/project && make daily >> logs/daily.log 2>&1

# Weekly pipeline on Sundays at 4am
0 4 * * 0 cd /path/to/project && make weekly >> logs/weekly.log 2>&1
```

---

## Storage Estimates

| Data | Per Day | 30 Days |
|------|---------|---------|
| events/*.parquet | ~200-500 MB | ~6-15 GB |
| posts/*.parquet | ~100-300 MB | ~3-9 GB |
| content_shares/*.parquet | ~50-150 MB | ~1.5-4.5 GB |
| coordination_events/*.parquet | ~10-50 MB | ~300 MB-1.5 GB |
| features/*.parquet | ~20-50 MB | ~600 MB-1.5 GB |
| clusters/*.parquet | ~10-20 MB | ~300-600 MB |
| embeddings/*.parquet | ~50-100 MB | ~200-400 MB (weekly) |
| network_features/*.parquet | ~20-50 MB | ~80-200 MB (weekly) |
| **Total** | ~500 MB-1.2 GB | ~15-35 GB |

---

## Dependencies

```
# requirements.txt
duckdb>=0.9.0
pyarrow>=14.0.0
pandas>=2.0.0
numpy>=1.24.0
scipy>=1.11.0
scikit-learn>=1.3.0
hdbscan>=0.8.33
umap-learn>=0.5.4
networkx>=3.2
python-louvain>=0.16  # for community detection
websockets>=12.0      # for firehose consumer
```

---

## Ad-hoc Investigation

```python
# In Jupyter or Python REPL
import duckdb

con = duckdb.connect()

# Most coordinated URLs
con.execute("""
    SELECT fingerprint, content_raw, COUNT(*) as pairs,
           COUNT(DISTINCT account_a) + COUNT(DISTINCT account_b) as accounts
    FROM read_parquet('data/coordination_events/*.parquet')
    WHERE content_type = 'url'
    GROUP BY fingerprint, content_raw
    ORDER BY accounts DESC
    LIMIT 20
""").fetchdf()

# Examine a cluster
con.execute("""
    SELECT c.*, f.repost_ratio, f.coordination_event_count, f.hourly_entropy
    FROM read_parquet('data/clusters/2025-01-15.parquet') c
    JOIN read_parquet('data/features/2025-01-15.parquet') f ON c.did = f.did
    WHERE c.cluster_id = 3
    ORDER BY f.coordination_event_count DESC
""").fetchdf()

# What did cluster members share together?
con.execute("""
    WITH cluster_members AS (
        SELECT did FROM read_parquet('data/clusters/2025-01-15.parquet')
        WHERE cluster_id = 3
    )
    SELECT cs.fingerprint, cs.content_raw, cs.content_type,
           COUNT(DISTINCT cs.actor_did) as sharers
    FROM read_parquet('data/content_shares/*.parquet') cs
    WHERE cs.actor_did IN (SELECT did FROM cluster_members)
    GROUP BY cs.fingerprint, cs.content_raw, cs.content_type
    HAVING COUNT(DISTINCT cs.actor_did) >= 3
    ORDER BY sharers DESC
    LIMIT 20
""").fetchdf()
```
