# Bluesky Bot Detection ETL (Parquet + DuckDB + Ray)

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              INGESTION                                      │
│                                                                             │
│  Firehose Consumer (Python/Ray Actor)                                       │
│       │                                                                     │
│       ▼                                                                     │
│  data/                                                                      │
│  ├── events/                                                                │
│  │   ├── 2025-01-15.parquet                                                │
│  │   └── ...                                                                │
│  ├── posts/                                                                 │
│  │   ├── 2025-01-15.parquet                                                │
│  │   └── ...                                                                │
│  ├── follow_events/              (append-only follow/unfollow events)      │
│  │   ├── 2025-01-15.parquet                                                │
│  │   └── ...                                                                │
│  └── account_snapshots/          (periodic snapshots with timestamp)       │
│      ├── 2025-01-15.parquet                                                │
│      └── ...                                                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     BATCH PROCESSING (Ray on KubeRay)                       │
│                                                                             │
│  Ray Jobs using Ray Data for distributed processing                         │
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
│    7_follow_graph.py → data/follow_graph/YYYY-MM-DD.parquet                │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              OUTPUT                                         │
│                                                                             │
│  data/clusters/YYYY-MM-DD.parquet                                          │
│  Jupyter notebooks / API for exploration                                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Directory Structure

```
project/
├── ingest/
│   └── firehose.py           # Firehose consumer (Ray actor)
├── jobs/
│   ├── 1_fingerprint.py      # Content fingerprinting (Ray Data)
│   ├── 2_coordination.py     # Coordination detection (DuckDB)
│   ├── 3_features.py         # Feature computation (Ray Data + DuckDB)
│   ├── 4_cluster.py          # Clustering (Ray)
│   ├── 5_embeddings.py       # Embedding generation (Ray)
│   ├── 6_network.py          # Network features (Ray + NetworkX)
│   └── 7_follow_graph.py     # Reconstruct follow graph from events
├── lib/
│   ├── fingerprint.py        # URL/text normalization utilities
│   ├── features.py           # Feature computation helpers
│   └── ray_utils.py          # Ray initialization and utilities
├── data/                     # Parquet storage (object storage in prod)
├── k8s/
│   ├── ray-cluster.yaml      # KubeRay cluster definition
│   └── jobs/                 # Ray job definitions
└── notebooks/
    └── explore.ipynb
```

---

## Data Schemas

### Raw Data (from Ingestion)

#### events/{date}.parquet

All interactions from the firehose.

| Column | Type | Description |
|--------|------|-------------|
| event_type | string | 'post', 'repost', 'like', 'reply', 'quote', 'block' |
| actor_did | string | Who performed the action |
| target_did | string | Target account (for repost/like/reply/block) |
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
| mentions | list[string] | Mentioned DIDs |
| hashtags | list[string] | #hashtags |
| langs | list[string] | Detected languages |
| has_images | bool | Whether post contains images |
| image_count | int | Number of images |
| created_at | timestamp[us] | Post creation time |

#### follow_events/{date}.parquet

Append-only log of follow/unfollow events. Preserves full history.

| Column | Type | Description |
|--------|------|-------------|
| event_type | string | 'follow' or 'unfollow' |
| follower_did | string | Account doing the following |
| following_did | string | Account being followed |
| timestamp | timestamp[us] | When the event occurred |
| rkey | string | Record key (for deduplication) |

#### account_snapshots/{date}.parquet

Daily snapshots of account metadata. Each row is a point-in-time snapshot.

| Column | Type | Description |
|--------|------|-------------|
| did | string | Account DID |
| snapshot_date | date | Date of this snapshot |
| handle | string | Handle at snapshot time |
| display_name | string | Display name at snapshot time |
| bio | string | Profile bio/description |
| bio_length | int | Bio character count |
| has_avatar | bool | Whether account has avatar |
| has_banner | bool | Whether account has banner image |
| created_at | timestamp[us] | Account creation time (from DID document) |
| first_seen | timestamp[us] | First seen in firehose |
| last_active | timestamp[us] | Last activity before snapshot |
| follower_count | int | Followers at snapshot time (if available) |
| following_count | int | Following at snapshot time (if available) |
| post_count | int | Total posts at snapshot time (if available) |
| pds_host | string | PDS endpoint hostname |

---

### Derived Data (from follow_events)

#### follow_graph/{date}.parquet (Weekly)

Reconstructed point-in-time follow graph.

| Column | Type | Description |
|--------|------|-------------|
| follower_did | string | Account doing the following |
| following_did | string | Account being followed |
| followed_at | timestamp[us] | When the follow was created |
| snapshot_date | date | Date this graph represents |

#### follow_changes/{date}.parquet (Daily, optional)

Aggregated follow behavior for detecting suspicious patterns.

| Column | Type | Description |
|--------|------|-------------|
| did | string | Account DID |
| window_start | date | Start of analysis window |
| window_end | date | End of analysis window |
| follows_added | int | New follows in window |
| follows_removed | int | Unfollows in window |
| net_follows | int | Net change |
| follow_churn_rate | float | (added + removed) / total_follows |
| unique_followed | int | Distinct accounts followed in window |
| unique_unfollowed | int | Distinct accounts unfollowed in window |
| rapid_follow_unfollow | int | Accounts followed then unfollowed within 24h |

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
| **Follow Behavior Features** | | |
| follows_added_7d | int | New follows in last 7 days |
| follows_removed_7d | int | Unfollows in last 7 days |
| follow_churn_rate_7d | float | Follow churn rate |
| rapid_follow_unfollow_7d | int | Quick follow/unfollow cycles |
| follower_count | int | Current follower count (from latest snapshot) |
| following_count | int | Current following count |
| follower_following_ratio | float | followers / following |
| | | |
| **Account Metadata Features** | | |
| account_age_days | int | Days since account creation |
| days_since_first_seen | int | Days since first seen in firehose |
| handle_length | int | Character count of handle |
| handle_digit_ratio | float | Digits in handle / total characters |
| handle_entropy | float | Character entropy of handle |
| handle_changed | bool | Whether handle changed in last 30 days |
| handle_change_count_30d | int | Number of handle changes in 30 days |
| has_avatar | bool | Whether account has avatar |
| has_bio | bool | Whether account has bio |
| bio_length | int | Bio character count |
| bio_changed_30d | bool | Whether bio changed in last 30 days |
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
| mean_follow_churn | float | Mean follow churn rate |
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

## Ray Integration

### Ray Utilities

```python
# lib/ray_utils.py

import ray
import os


def init_ray():
    """Initialize Ray, connecting to cluster if available."""
    if ray.is_initialized():
        return
    
    # Check for KubeRay cluster
    ray_address = os.environ.get("RAY_ADDRESS")
    
    if ray_address:
        # Connect to existing cluster
        ray.init(address=ray_address)
    else:
        # Local mode for development
        ray.init()
    
    print(f"Ray initialized: {ray.cluster_resources()}")


def get_num_cpus() -> int:
    """Get available CPUs in Ray cluster."""
    return int(ray.cluster_resources().get("CPU", 1))
```

### Firehose Consumer (Ray Actor)

```python
# ingest/firehose.py

import ray
import asyncio
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, date
from pathlib import Path
from websockets import connect
from collections import defaultdict

from lib.ray_utils import init_ray


@ray.remote
class FirehoseConsumer:
    """
    Ray actor that consumes the firehose and writes to parquet.
    Can be scaled to multiple replicas if needed.
    """
    
    FIREHOSE_URL = "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"
    BATCH_SIZE = 10000
    FLUSH_INTERVAL_SEC = 30
    
    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
        self.buffers = defaultdict(list)  # table_name -> records
        self.current_date = date.today()
        self.last_flush = datetime.now()
        
        # Ensure directories exist
        for subdir in ['events', 'posts', 'follow_events', 'account_snapshots']:
            (self.data_dir / subdir).mkdir(parents=True, exist_ok=True)
    
    async def run(self):
        """Main consumer loop."""
        while True:
            try:
                async with connect(self.FIREHOSE_URL) as ws:
                    async for message in ws:
                        await self._process_message(message)
                        await self._maybe_flush()
            except Exception as e:
                print(f"Connection error: {e}, reconnecting...")
                await asyncio.sleep(5)
    
    async def _process_message(self, message: bytes):
        """Decode and route firehose message."""
        # Simplified - actual implementation needs CBOR decoding
        records = self._decode_message(message)
        
        for record in records:
            record_type = record.get('$type', '')
            
            if record_type == 'app.bsky.feed.post':
                self._buffer_post(record)
                self._buffer_event(record, 'post')
            elif record_type == 'app.bsky.feed.repost':
                self._buffer_event(record, 'repost')
            elif record_type == 'app.bsky.feed.like':
                self._buffer_event(record, 'like')
            elif record_type == 'app.bsky.graph.follow':
                self._buffer_follow_event(record, 'follow')
            elif record_type == 'app.bsky.graph.follow#delete':
                self._buffer_follow_event(record, 'unfollow')
            elif record_type == 'app.bsky.actor.profile':
                self._buffer_account_snapshot(record)
    
    def _buffer_event(self, record: dict, event_type: str):
        self.buffers['events'].append({
            'event_type': event_type,
            'actor_did': record.get('actor'),
            'target_did': record.get('subject', {}).get('did'),
            'target_post_uri': record.get('subject', {}).get('uri'),
            'timestamp': datetime.now(),
        })
    
    def _buffer_post(self, record: dict):
        self.buffers['posts'].append({
            'post_uri': record.get('uri'),
            'author_did': record.get('actor'),
            'text': record.get('text', ''),
            'text_length': len(record.get('text', '')),
            'reply_parent': record.get('reply', {}).get('parent', {}).get('uri'),
            'reply_root': record.get('reply', {}).get('root', {}).get('uri'),
            'quote_uri': self._extract_quote_uri(record),
            'urls': self._extract_urls(record),
            'mentions': self._extract_mentions(record),
            'hashtags': self._extract_hashtags(record),
            'has_images': self._has_images(record),
            'image_count': self._count_images(record),
            'created_at': datetime.now(),
        })
    
    def _buffer_follow_event(self, record: dict, event_type: str):
        self.buffers['follow_events'].append({
            'event_type': event_type,
            'follower_did': record.get('actor'),
            'following_did': record.get('subject'),
            'timestamp': datetime.now(),
            'rkey': record.get('rkey'),
        })
    
    def _buffer_account_snapshot(self, record: dict):
        self.buffers['account_snapshots'].append({
            'did': record.get('actor'),
            'snapshot_date': date.today(),
            'handle': record.get('handle'),
            'display_name': record.get('displayName'),
            'bio': record.get('description'),
            'bio_length': len(record.get('description', '') or ''),
            'has_avatar': record.get('avatar') is not None,
            'has_banner': record.get('banner') is not None,
            'last_active': datetime.now(),
            'pds_host': record.get('pds_host'),
        })
    
    async def _maybe_flush(self):
        """Flush buffers if size or time threshold reached."""
        now = datetime.now()
        today = date.today()
        
        # Check for date rollover
        if today != self.current_date:
            await self._flush_all()
            self.current_date = today
            return
        
        # Check size threshold
        for table, records in self.buffers.items():
            if len(records) >= self.BATCH_SIZE:
                await self._flush_table(table)
        
        # Check time threshold
        if (now - self.last_flush).total_seconds() >= self.FLUSH_INTERVAL_SEC:
            await self._flush_all()
    
    async def _flush_all(self):
        for table in list(self.buffers.keys()):
            await self._flush_table(table)
        self.last_flush = datetime.now()
    
    async def _flush_table(self, table: str):
        records = self.buffers[table]
        if not records:
            return
        
        path = self.data_dir / table / f"{self.current_date}.parquet"
        
        # Convert to Arrow table and append
        new_table = pa.Table.from_pylist(records)
        
        if path.exists():
            existing = pq.read_table(path)
            combined = pa.concat_tables([existing, new_table])
            pq.write_table(combined, path, compression='zstd')
        else:
            pq.write_table(new_table, path, compression='zstd')
        
        self.buffers[table] = []
        print(f"Flushed {len(records)} records to {path}")
    
    # Helper methods (simplified)
    def _decode_message(self, message): return []
    def _extract_quote_uri(self, record): return None
    def _extract_urls(self, record): return []
    def _extract_mentions(self, record): return []
    def _extract_hashtags(self, record): return []
    def _has_images(self, record): return False
    def _count_images(self, record): return 0


def main():
    init_ray()
    consumer = FirehoseConsumer.remote(data_dir="data")
    ray.get(consumer.run.remote())


if __name__ == "__main__":
    main()
```

---

## Job Implementations

### Job 1: Content Fingerprinting (Ray Data)

```python
# jobs/1_fingerprint.py

import ray
from ray import data as rd
import hashlib
import re
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from lib.ray_utils import init_ray

DATA_DIR = Path("data")

TRACKING_PARAMS = {
    'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
    'fbclid', 'gclid', 'ref', 'source', 'ref_src', 'ref_url', 'mc_eid'
}


def normalize_url(url: str) -> str:
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
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r'https?://\S+', '', text)
    text = re.sub(r'@[\w.]+', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def fingerprint(content: str) -> str:
    if not content:
        return ""
    return hashlib.sha256(content.encode()).hexdigest()[:16]


def extract_url_shares(batch: dict) -> dict:
    """Extract URL shares from posts batch."""
    results = {
        'actor_did': [],
        'content_type': [],
        'fingerprint': [],
        'content_raw': [],
        'post_uri': [],
        'created_at': [],
    }
    
    for i, urls in enumerate(batch['urls']):
        if urls:
            for url in urls:
                if url:
                    results['actor_did'].append(batch['author_did'][i])
                    results['content_type'].append('url')
                    results['fingerprint'].append(fingerprint(normalize_url(url)))
                    results['content_raw'].append(url)
                    results['post_uri'].append(batch['post_uri'][i])
                    results['created_at'].append(batch['created_at'][i])
    
    return results


def extract_text_shares(batch: dict) -> dict:
    """Extract text fingerprints from posts batch."""
    results = {
        'actor_did': [],
        'content_type': [],
        'fingerprint': [],
        'content_raw': [],
        'post_uri': [],
        'created_at': [],
    }
    
    for i, text in enumerate(batch['text']):
        if text and len(text) > 50 and batch['reply_parent'][i] is None:
            results['actor_did'].append(batch['author_did'][i])
            results['content_type'].append('text')
            results['fingerprint'].append(fingerprint(normalize_text(text)))
            results['content_raw'].append(text[:100] if text else '')
            results['post_uri'].append(batch['post_uri'][i])
            results['created_at'].append(batch['created_at'][i])
    
    return results


def run(target_date: date):
    init_ray()
    
    posts_path = str(DATA_DIR / f"posts/{target_date}.parquet")
    events_path = str(DATA_DIR / f"events/{target_date}.parquet")
    output_path = str(DATA_DIR / f"content_shares/{target_date}.parquet")
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    # Process posts for URLs and text
    posts_ds = rd.read_parquet(posts_path)
    
    url_shares = posts_ds.map_batches(
        extract_url_shares,
        batch_format="numpy"
    )
    
    text_shares = posts_ds.map_batches(
        extract_text_shares,
        batch_format="numpy"
    )
    
    # Process events for reposts
    events_ds = rd.read_parquet(events_path)
    
    reposts = events_ds.filter(
        lambda row: row['event_type'] == 'repost' and row['target_post_uri'] is not None
    ).map(lambda row: {
        'actor_did': row['actor_did'],
        'content_type': 'repost',
        'fingerprint': row['target_post_uri'],
        'content_raw': row['target_post_uri'],
        'post_uri': None,
        'created_at': row['timestamp'],
    })
    
    # Combine and write
    combined = url_shares.union(text_shares).union(reposts)
    combined.write_parquet(output_path)
    
    count = combined.count()
    print(f"Fingerprinting complete for {target_date}: {count:,} content shares")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        target = date.fromisoformat(sys.argv[1])
    else:
        target = date.today() - timedelta(days=1)
    run(target)
```

### Job 2: Coordination Detection (DuckDB)

```python
# jobs/2_coordination.py

import duckdb
from datetime import date, timedelta
from pathlib import Path

from lib.ray_utils import init_ray

DATA_DIR = Path("data")
WINDOW_HOURS = 2
LOOKBACK_DAYS = 7


def run(target_date: date):
    # DuckDB is already efficient for this join-heavy operation
    # No need for Ray here
    
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
    
    summary = con.execute(f"""
        SELECT 
            COUNT(*) as pairs,
            COUNT(DISTINCT fingerprint) as unique_content,
            COUNT(DISTINCT account_a) as unique_a,
            COUNT(DISTINCT account_b) as unique_b
        FROM read_parquet('{output_path}')
    """).fetchone()
    
    print(f"Coordination detection for {target_date}:")
    print(f"  Pairs: {summary[0]:,}, Content: {summary[1]:,}, Accounts: ~{summary[2] + summary[3]:,}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        target = date.fromisoformat(sys.argv[1])
    else:
        target = date.today() - timedelta(days=1)
    run(target)
```

### Job 3: Feature Computation (Ray + DuckDB Hybrid)

```python
# jobs/3_features.py

import ray
from ray import data as rd
import duckdb
import numpy as np
from datetime import date, timedelta
from pathlib import Path
from scipy.stats import entropy

from lib.ray_utils import init_ray, get_num_cpus

DATA_DIR = Path("data")
WINDOW_DAYS = 7


def compute_entropy(counts: list) -> float:
    if not counts or sum(counts) == 0:
        return 0.0
    probs = np.array(counts) / sum(counts)
    return float(entropy(probs))


def compute_handle_entropy(handle: str) -> float:
    if not handle:
        return 0.0
    counts = {}
    for c in handle.lower():
        counts[c] = counts.get(c, 0) + 1
    return compute_entropy(list(counts.values()))


def run(target_date: date):
    init_ray()
    con = duckdb.connect()
    
    start_date = target_date - timedelta(days=WINDOW_DAYS)
    output_path = DATA_DIR / f"features/{target_date}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Use DuckDB for the heavy SQL aggregations
    # This is more efficient than doing it in Ray for this use case
    
    con.create_function("compute_entropy", compute_entropy)
    con.create_function("handle_entropy", compute_handle_entropy)
    
    # Get latest account snapshot for each DID
    latest_snapshot_query = f"""
        WITH ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY did ORDER BY snapshot_date DESC) as rn
            FROM read_parquet('{DATA_DIR}/account_snapshots/*.parquet')
        )
        SELECT * FROM ranked WHERE rn = 1
    """
    
    # Get follow behavior from follow_events
    follow_behavior_query = f"""
        SELECT
            COALESCE(f.follower_did, u.follower_did) as did,
            COALESCE(f.follows_added, 0) as follows_added_7d,
            COALESCE(u.follows_removed, 0) as follows_removed_7d,
            COALESCE(f.follows_added, 0) + COALESCE(u.follows_removed, 0) as follow_churn_7d
        FROM (
            SELECT follower_did, COUNT(*) as follows_added
            FROM read_parquet('{DATA_DIR}/follow_events/*.parquet')
            WHERE event_type = 'follow' AND timestamp >= '{start_date}'
            GROUP BY follower_did
        ) f
        FULL OUTER JOIN (
            SELECT follower_did, COUNT(*) as follows_removed
            FROM read_parquet('{DATA_DIR}/follow_events/*.parquet')
            WHERE event_type = 'unfollow' AND timestamp >= '{start_date}'
            GROUP BY follower_did
        ) u ON f.follower_did = u.follower_did
    """
    
    # Main feature query (same structure as before, but with follow features)
    main_query = f"""
    COPY (
        WITH 
        -- TEMPORAL FEATURES
        post_events AS (
            SELECT * FROM read_parquet('{DATA_DIR}/events/*.parquet')
            WHERE timestamp >= '{start_date}' AND event_type = 'post'
        ),
        hourly_posts AS (
            SELECT actor_did, DATE_TRUNC('hour', timestamp) as hour, COUNT(*) as posts
            FROM post_events
            GROUP BY actor_did, DATE_TRUNC('hour', timestamp)
        ),
        hourly_distribution AS (
            SELECT actor_did, EXTRACT(HOUR FROM hour) as hour_of_day, SUM(posts) as posts
            FROM hourly_posts
            GROUP BY actor_did, EXTRACT(HOUR FROM hour)
        ),
        hourly_entropy_calc AS (
            SELECT actor_did, compute_entropy(LIST(posts ORDER BY hour_of_day)) as hourly_entropy
            FROM hourly_distribution
            GROUP BY actor_did
        ),
        inter_post AS (
            SELECT actor_did,
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
        
        -- CONTENT FEATURES
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
                AVG(COALESCE(ARRAY_LENGTH(mentions), 0)) as mention_per_post_mean,
                SUM(CASE WHEN urls IS NOT NULL AND ARRAY_LENGTH(urls) > 0 
                    THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as link_ratio,
                SUM(CASE WHEN has_images THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as image_ratio,
                SUM(CASE WHEN text LIKE '%?%' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as question_ratio
            FROM posts
            GROUP BY author_did
        ),
        
        -- BEHAVIORAL FEATURES
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
                COUNT(*) FILTER (WHERE event_type = 'reply') as replies
            FROM all_events
            GROUP BY actor_did
        ),
        target_counts AS (
            SELECT actor_did as did, event_type, target_did, COUNT(*) as cnt
            FROM all_events
            WHERE target_did IS NOT NULL
            GROUP BY actor_did, event_type, target_did
        ),
        target_agg AS (
            SELECT
                did, event_type,
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
        
        -- COORDINATION FEATURES
        coord_raw AS (
            SELECT account_a as did, account_b as partner, content_type, time_delta_sec
            FROM read_parquet('{DATA_DIR}/coordination_events/*.parquet')
            UNION ALL
            SELECT account_b as did, account_a as partner, content_type, time_delta_sec
            FROM read_parquet('{DATA_DIR}/coordination_events/*.parquet')
        ),
        coord_by_partner AS (
            SELECT did, partner, COUNT(*) as event_count,
                AVG(time_delta_sec) as mean_delta, MIN(time_delta_sec) as min_delta
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
            SELECT did,
                SUM(CASE WHEN content_type = 'url' THEN 1 ELSE 0 END) as url_coordination_count,
                SUM(CASE WHEN content_type = 'repost' THEN 1 ELSE 0 END) as repost_coordination_count,
                SUM(CASE WHEN content_type = 'text' THEN 1 ELSE 0 END) as text_coordination_count
            FROM coord_raw
            GROUP BY did
        ),
        
        -- FOLLOW BEHAVIOR
        follow_behavior AS ({follow_behavior_query}),
        
        -- ACCOUNT METADATA (from latest snapshot)
        accounts AS ({latest_snapshot_query}),
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
                COALESCE(LENGTH(display_name), 0) as display_name_length,
                follower_count,
                following_count,
                follower_count * 1.0 / NULLIF(following_count, 0) as follower_following_ratio
            FROM accounts
        )
        
        -- FINAL JOIN
        SELECT 
            t.did,
            {WINDOW_DAYS} as window_days,
            CURRENT_TIMESTAMP as computed_at,
            
            -- Temporal
            t.total_posts, t.posts_per_hour_mean, t.posts_per_hour_std, t.posts_per_hour_max,
            t.active_hours_count, t.active_days_count, t.hourly_entropy, t.night_post_ratio,
            t.inter_post_time_mean, t.inter_post_time_std, t.inter_post_time_min, t.burstiness,
            
            -- Content
            c.mean_post_length, c.std_post_length, c.hashtag_per_post_mean,
            c.mention_per_post_mean, c.link_ratio, c.image_ratio, c.question_ratio,
            
            -- Behavioral
            COALESCE(b.total_actions, 0) as total_actions,
            COALESCE(b.repost_ratio, 0) as repost_ratio,
            COALESCE(b.like_ratio, 0) as like_ratio,
            COALESCE(b.reply_ratio, 0) as reply_ratio,
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
            
            -- Follow behavior
            COALESCE(fb.follows_added_7d, 0) as follows_added_7d,
            COALESCE(fb.follows_removed_7d, 0) as follows_removed_7d,
            COALESCE(fb.follow_churn_7d, 0) as follow_churn_7d,
            
            -- Metadata
            COALESCE(m.account_age_days, 0) as account_age_days,
            COALESCE(m.days_since_first_seen, 0) as days_since_first_seen,
            COALESCE(m.handle_length, 0) as handle_length,
            COALESCE(m.handle_digit_ratio, 0) as handle_digit_ratio,
            COALESCE(m.handle_entropy, 0) as handle_entropy,
            COALESCE(m.has_avatar, FALSE) as has_avatar,
            COALESCE(m.has_bio, FALSE) as has_bio,
            COALESCE(m.bio_length, 0) as bio_length,
            COALESCE(m.display_name_length, 0) as display_name_length,
            COALESCE(m.follower_count, 0) as follower_count,
            COALESCE(m.following_count, 0) as following_count,
            COALESCE(m.follower_following_ratio, 0) as follower_following_ratio
            
        FROM temporal t
        LEFT JOIN content c ON t.did = c.did
        LEFT JOIN behavioral b ON t.did = b.did
        LEFT JOIN coordination co ON t.did = co.did
        LEFT JOIN coord_by_type ct ON t.did = ct.did
        LEFT JOIN follow_behavior fb ON t.did = fb.did
        LEFT JOIN metadata m ON t.did = m.did
    )
    TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    
    con.execute(main_query)
    
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

### Job 4: Clustering (Ray for distributed ML)

```python
# jobs/4_cluster.py

import ray
from ray import data as rd
import numpy as np
import pandas as pd
import duckdb
from datetime import date, timedelta
from pathlib import Path
from sklearn.preprocessing import RobustScaler

from lib.ray_utils import init_ray, get_num_cpus

DATA_DIR = Path("data")

CLUSTER_FEATURES = [
    'posts_per_hour_mean', 'posts_per_hour_std', 'active_hours_count',
    'hourly_entropy', 'inter_post_time_mean', 'inter_post_time_std', 'burstiness',
    'mean_post_length', 'std_post_length', 'hashtag_per_post_mean', 'link_ratio', 'image_ratio',
    'repost_ratio', 'like_ratio', 'reply_ratio',
    'unique_repost_sources', 'unique_reply_targets',
    'top_repost_source_share', 'top_reply_target_share',
    'repost_source_hhi', 'reply_target_hhi',
    'coordination_event_count', 'unique_coord_partners',
    'mean_coord_time_delta', 'min_coord_time_delta',
    'url_coordination_count', 'repost_coordination_count',
    'follows_added_7d', 'follows_removed_7d', 'follow_churn_7d',
    'account_age_days', 'handle_entropy', 'handle_digit_ratio',
    'follower_following_ratio',
]

LOG_FEATURES = [
    'posts_per_hour_mean', 'posts_per_hour_std', 'inter_post_time_mean', 'inter_post_time_std',
    'coordination_event_count', 'unique_coord_partners',
    'mean_coord_time_delta', 'min_coord_time_delta',
    'unique_repost_sources', 'unique_reply_targets',
    'account_age_days', 'hashtag_per_post_mean',
    'follows_added_7d', 'follows_removed_7d', 'follow_churn_7d',
]


@ray.remote
def cluster_partition(X: np.ndarray, min_cluster_size: int = 20) -> tuple:
    """Run UMAP + HDBSCAN on a data partition."""
    from umap import UMAP
    from hdbscan import HDBSCAN
    
    # UMAP
    reducer = UMAP(n_components=15, metric='cosine', min_dist=0.1, random_state=42)
    X_reduced = reducer.fit_transform(X)
    
    # HDBSCAN
    clusterer = HDBSCAN(min_cluster_size=min_cluster_size, min_samples=5, metric='euclidean')
    labels = clusterer.fit_predict(X_reduced)
    
    return labels, clusterer.probabilities_, clusterer.outlier_scores_


def compute_bot_likelihood(cluster_df: pd.DataFrame, all_df: pd.DataFrame) -> float:
    """Compute bot likelihood score for a cluster."""
    scores = []
    
    # Low hourly entropy
    mean_entropy = cluster_df['hourly_entropy'].mean()
    all_mean = all_df['hourly_entropy'].mean()
    scores.append(0.8 if mean_entropy < all_mean * 0.7 else 0.4 if mean_entropy < all_mean * 0.9 else 0.1)
    
    # High coordination
    mean_coord = cluster_df['coordination_event_count'].mean()
    scores.append(0.9 if mean_coord > 10 else 0.6 if mean_coord > 5 else 0.3 if mean_coord > 1 else 0.1)
    
    # High target concentration
    mean_hhi = cluster_df['repost_source_hhi'].mean()
    scores.append(0.8 if mean_hhi > 0.5 else 0.5 if mean_hhi > 0.3 else 0.2)
    
    # Young accounts
    mean_age = cluster_df['account_age_days'].mean()
    scores.append(0.7 if mean_age < 30 else 0.4 if mean_age < 90 else 0.2)
    
    # High repost ratio
    mean_repost = cluster_df['repost_ratio'].mean()
    scores.append(0.8 if mean_repost > 0.7 else 0.5 if mean_repost > 0.5 else 0.2)
    
    # High follow churn
    mean_churn = cluster_df['follow_churn_7d'].mean()
    scores.append(0.7 if mean_churn > 50 else 0.4 if mean_churn > 20 else 0.2)
    
    return np.mean(scores)


def infer_intent(cluster_df: pd.DataFrame) -> str:
    """Infer cluster intent based on behavioral patterns."""
    mean_repost = cluster_df['repost_ratio'].mean()
    mean_reply = cluster_df['reply_ratio'].mean()
    mean_link = cluster_df['link_ratio'].mean()
    mean_hhi = cluster_df['repost_source_hhi'].mean()
    mean_churn = cluster_df['follow_churn_7d'].mean()
    
    if mean_repost > 0.6 and mean_hhi > 0.4:
        return 'amplification'
    elif mean_link > 0.5:
        return 'spam'
    elif mean_reply > 0.6:
        return 'engagement'
    elif mean_churn > 50:
        return 'follow_farming'
    else:
        return 'unknown'


def run(target_date: date):
    init_ray()
    con = duckdb.connect()
    
    features_path = DATA_DIR / f"features/{target_date}.parquet"
    output_path = DATA_DIR / f"clusters/{target_date}.parquet"
    summary_path = DATA_DIR / f"cluster_summaries/{target_date}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Load features
    df = con.execute(f"SELECT * FROM read_parquet('{features_path}')").fetchdf()
    print(f"Loaded {len(df):,} accounts")
    
    # Filter
    df = df[df['total_posts'] >= 5].copy()
    print(f"After filtering: {len(df):,} accounts")
    
    if len(df) < 100:
        print("Not enough accounts for clustering")
        return
    
    # Extract and transform features
    X = df[CLUSTER_FEATURES].fillna(0).values.copy()
    
    for i, col in enumerate(CLUSTER_FEATURES):
        if col in LOG_FEATURES:
            X[:, i] = np.log1p(X[:, i])
    
    scaler = RobustScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Distribute clustering with Ray
    print("Running distributed clustering...")
    X_ref = ray.put(X_scaled)
    labels_future = cluster_partition.remote(X_ref, min_cluster_size=20)
    labels, probabilities, outlier_scores = ray.get(labels_future)
    
    # Add results to dataframe
    df['cluster_id'] = labels
    df['cluster_probability'] = probabilities
    df['outlier_score'] = outlier_scores
    
    # Compute cluster scores
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
    
    # Get investigation helpers
    top_partners = con.execute(f"""
        SELECT did, ARRAY_AGG(partner ORDER BY cnt DESC)[:5] as top_coord_partners
        FROM (
            SELECT account_a as did, account_b as partner, COUNT(*) as cnt
            FROM read_parquet('{DATA_DIR}/coordination_events/*.parquet')
            GROUP BY account_a, account_b
            UNION ALL
            SELECT account_b as did, account_a as partner, COUNT(*) as cnt
            FROM read_parquet('{DATA_DIR}/coordination_events/*.parquet')
            GROUP BY account_b, account_a
        ) GROUP BY did
    """).fetchdf()
    
    top_targets = con.execute(f"""
        SELECT actor_did as did, ARRAY_AGG(target_did ORDER BY cnt DESC)[:5] as top_repost_targets
        FROM (
            SELECT actor_did, target_did, COUNT(*) as cnt
            FROM read_parquet('{DATA_DIR}/events/*.parquet')
            WHERE event_type = 'repost' AND target_did IS NOT NULL
            GROUP BY actor_did, target_did
        ) GROUP BY actor_did
    """).fetchdf()
    
    df = df.merge(top_partners, on='did', how='left')
    df = df.merge(top_targets, on='did', how='left')
    
    # Output
    output_cols = [
        'did', 'cluster_id', 'cluster_probability', 'outlier_score', 'bot_likelihood',
        'cluster_size', 'cluster_intent', 'top_coord_partners', 'top_repost_targets'
    ]
    df[output_cols].to_parquet(output_path, compression='zstd')
    
    # Cluster summaries
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
            'mean_follow_churn': cluster_df['follow_churn_7d'].mean(),
            'intent_category': intents[cluster_id],
        })
    
    pd.DataFrame(summaries).to_parquet(summary_path, compression='zstd')
    
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise = (labels == -1).sum()
    print(f"\nClustering complete: {n_clusters} clusters, {n_noise:,} noise points")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        target = date.fromisoformat(sys.argv[1])
    else:
        target = date.today() - timedelta(days=1)
    run(target)
```

### Job 5: Embeddings (Ray for distributed SVD)

```python
# jobs/5_embeddings.py

import ray
from ray import data as rd
import numpy as np
import pandas as pd
import duckdb
from datetime import date, timedelta
from pathlib import Path
from scipy.sparse import csr_matrix

from lib.ray_utils import init_ray

DATA_DIR = Path("data")
EMBEDDING_DIM = 64
LOOKBACK_DAYS = 30


@ray.remote
def compute_svd_embedding(
    rows: np.ndarray, 
    cols: np.ndarray, 
    data: np.ndarray,
    n_actors: int,
    n_targets: int,
    n_components: int
) -> np.ndarray:
    """Compute TF-IDF + SVD embedding."""
    from sklearn.decomposition import TruncatedSVD
    from sklearn.feature_extraction.text import TfidfTransformer
    
    matrix = csr_matrix((data, (rows, cols)), shape=(n_actors, n_targets))
    
    tfidf = TfidfTransformer(norm='l2', use_idf=True)
    matrix_tfidf = tfidf.fit_transform(matrix)
    
    n_comp = min(n_components, matrix_tfidf.shape[1] - 1, matrix_tfidf.shape[0] - 1)
    svd = TruncatedSVD(n_components=n_comp, random_state=42)
    vectors = svd.fit_transform(matrix_tfidf)
    
    # Pad if needed
    if vectors.shape[1] < n_components:
        padding = np.zeros((vectors.shape[0], n_components - vectors.shape[1]))
        vectors = np.hstack([vectors, padding])
    
    return vectors


def run(target_date: date):
    init_ray()
    con = duckdb.connect()
    
    start_date = target_date - timedelta(days=LOOKBACK_DAYS)
    output_path = DATA_DIR / f"embeddings/{target_date}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    embeddings = []
    
    for interaction_type in ['repost', 'reply', 'like']:
        print(f"Building {interaction_type} embeddings...")
        
        df = con.execute(f"""
            SELECT actor_did, target_did, COUNT(*) as cnt
            FROM read_parquet('{DATA_DIR}/events/*.parquet')
            WHERE event_type = '{interaction_type}'
              AND timestamp >= '{start_date}'
              AND target_did IS NOT NULL
            GROUP BY actor_did, target_did
        """).fetchdf()
        
        if len(df) < 100:
            print(f"  Skipping {interaction_type}: not enough data")
            continue
        
        actors = sorted(df['actor_did'].unique())
        targets = sorted(df['target_did'].unique())
        actor_to_idx = {a: i for i, a in enumerate(actors)}
        target_to_idx = {t: i for i, t in enumerate(targets)}
        
        rows = df['actor_did'].map(actor_to_idx).values
        cols = df['target_did'].map(target_to_idx).values
        data = df['cnt'].values.astype(np.float32)
        
        # Distributed SVD
        vectors = ray.get(compute_svd_embedding.remote(
            rows, cols, data,
            len(actors), len(targets), EMBEDDING_DIM
        ))
        
        for i, actor in enumerate(actors):
            embeddings.append({
                'did': actor,
                'embedding_type': f'{interaction_type}_target',
                'vector': vectors[i].tolist(),
                'computed_at': pd.Timestamp.now()
            })
        
        print(f"  Generated {len(actors):,} embeddings")
    
    if embeddings:
        pd.DataFrame(embeddings).to_parquet(output_path, compression='zstd')
        print(f"\nSaved {len(embeddings):,} embeddings")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        target = date.fromisoformat(sys.argv[1])
    else:
        target = date.today() - timedelta(days=1)
    run(target)
```

### Job 6: Network Features (Ray + NetworkX)

```python
# jobs/6_network.py

import ray
import numpy as np
import pandas as pd
import networkx as nx
import duckdb
from datetime import date, timedelta
from pathlib import Path

from lib.ray_utils import init_ray

DATA_DIR = Path("data")
LOOKBACK_DAYS = 30


@ray.remote
def compute_pagerank(edges: list, nodes: list) -> dict:
    """Compute PageRank in a Ray task."""
    G = nx.DiGraph()
    G.add_nodes_from(nodes)
    G.add_weighted_edges_from(edges)
    return nx.pagerank(G, weight='weight', max_iter=100)


@ray.remote
def compute_clustering(edges: list, nodes: list) -> dict:
    """Compute clustering coefficients in a Ray task."""
    G = nx.Graph()
    G.add_nodes_from(nodes)
    G.add_weighted_edges_from([(s, t, w) for s, t, w in edges])
    return nx.clustering(G)


def run(target_date: date):
    init_ray()
    con = duckdb.connect()
    
    start_date = target_date - timedelta(days=LOOKBACK_DAYS)
    output_path = DATA_DIR / f"network_features/{target_date}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print("Loading interactions...")
    edges_df = con.execute(f"""
        SELECT actor_did as source, target_did as target, COUNT(*) as weight
        FROM read_parquet('{DATA_DIR}/events/*.parquet')
        WHERE timestamp >= '{start_date}'
          AND target_did IS NOT NULL
          AND event_type IN ('repost', 'reply', 'quote', 'like')
        GROUP BY actor_did, target_did
    """).fetchdf()
    
    nodes = list(set(edges_df['source'].tolist() + edges_df['target'].tolist()))
    edges = list(edges_df.itertuples(index=False, name=None))
    
    print(f"Graph: {len(nodes):,} nodes, {len(edges):,} edges")
    
    # Compute metrics in parallel
    print("Computing metrics...")
    pagerank_future = compute_pagerank.remote(edges, nodes)
    clustering_future = compute_clustering.remote(edges, nodes)
    
    pagerank = ray.get(pagerank_future)
    clustering = ray.get(clustering_future)
    
    # Degree (computed locally, fast)
    G = nx.DiGraph()
    G.add_nodes_from(nodes)
    G.add_weighted_edges_from(edges)
    
    in_degree = dict(G.in_degree())
    out_degree = dict(G.out_degree())
    weighted_in = dict(G.in_degree(weight='weight'))
    weighted_out = dict(G.out_degree(weight='weight'))
    
    # Community detection
    print("Community detection...")
    try:
        from community import community_louvain
        partition = community_louvain.best_partition(G.to_undirected())
    except ImportError:
        partition = {n: 0 for n in nodes}
    
    # Build output
    records = [{
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
    } for node in nodes]
    
    df = pd.DataFrame(records)
    community_sizes = df['community_id'].value_counts().to_dict()
    df['community_size'] = df['community_id'].map(community_sizes)
    
    df.to_parquet(output_path, compression='zstd')
    print(f"Saved {len(df):,} network features")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        target = date.fromisoformat(sys.argv[1])
    else:
        target = date.today() - timedelta(days=1)
    run(target)
```

### Job 7: Follow Graph Reconstruction (Weekly)

```python
# jobs/7_follow_graph.py

import duckdb
from datetime import date, timedelta
from pathlib import Path

DATA_DIR = Path("data")


def run(target_date: date):
    """Reconstruct point-in-time follow graph from follow events."""
    con = duckdb.connect()
    
    output_path = DATA_DIR / f"follow_graph/{target_date}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Reconstruct current state from all follow/unfollow events up to target_date
    query = f"""
    COPY (
        WITH events AS (
            SELECT 
                follower_did,
                following_did,
                event_type,
                timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY follower_did, following_did 
                    ORDER BY timestamp DESC
                ) as rn
            FROM read_parquet('{DATA_DIR}/follow_events/*.parquet')
            WHERE timestamp <= '{target_date} 23:59:59'
        ),
        latest_state AS (
            SELECT follower_did, following_did, event_type, timestamp
            FROM events
            WHERE rn = 1
        )
        SELECT 
            follower_did,
            following_did,
            timestamp as followed_at,
            '{target_date}'::DATE as snapshot_date
        FROM latest_state
        WHERE event_type = 'follow'
    )
    TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    
    con.execute(query)
    
    count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()[0]
    print(f"Follow graph reconstructed for {target_date}: {count:,} edges")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        target = date.fromisoformat(sys.argv[1])
    else:
        target = date.today() - timedelta(days=1)
    run(target)
```

---

## Kubernetes / KubeRay Configuration

### Ray Cluster Definition

```yaml
# k8s/ray-cluster.yaml
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: bluesky-bot-detection
spec:
  rayVersion: '2.9.0'
  headGroupSpec:
    rayStartParams:
      dashboard-host: '0.0.0.0'
    template:
      spec:
        containers:
        - name: ray-head
          image: bluesky-bot-detection:latest
          resources:
            limits:
              cpu: "4"
              memory: "16Gi"
            requests:
              cpu: "2"
              memory: "8Gi"
          volumeMounts:
          - name: data
            mountPath: /data
        volumes:
        - name: data
          persistentVolumeClaim:
            claimName: bluesky-data-pvc
  workerGroupSpecs:
  - replicas: 3
    minReplicas: 1
    maxReplicas: 10
    groupName: workers
    rayStartParams: {}
    template:
      spec:
        containers:
        - name: ray-worker
          image: bluesky-bot-detection:latest
          resources:
            limits:
              cpu: "4"
              memory: "16Gi"
            requests:
              cpu: "2"
              memory: "8Gi"
          volumeMounts:
          - name: data
            mountPath: /data
        volumes:
        - name: data
          persistentVolumeClaim:
            claimName: bluesky-data-pvc
```

### Ray Job Submission

```yaml
# k8s/jobs/daily-pipeline.yaml
apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: daily-pipeline
spec:
  entrypoint: |
    python jobs/1_fingerprint.py && \
    python jobs/2_coordination.py && \
    python jobs/3_features.py && \
    python jobs/4_cluster.py
  runtimeEnvYAML: |
    working_dir: /app
    env_vars:
      DATA_DIR: /data
  clusterSelector:
    ray.io/cluster: bluesky-bot-detection
  ttlSecondsAfterFinished: 86400
```

---

## Running the Pipeline

### Local Development

```bash
# Start Ray locally
ray start --head

# Run pipeline
python jobs/1_fingerprint.py 2025-01-15
python jobs/2_coordination.py 2025-01-15
python jobs/3_features.py 2025-01-15
python jobs/4_cluster.py 2025-01-15
```

### On KubeRay

```bash
# Submit job
kubectl apply -f k8s/jobs/daily-pipeline.yaml

# Check status
kubectl get rayjobs
kubectl logs -l ray.io/cluster=bluesky-bot-detection
```

### Makefile

```makefile
DATE := $(shell date -d "yesterday" +%Y-%m-%d)

.PHONY: daily weekly local k8s

daily: fingerprint coordination features cluster

fingerprint:
	python jobs/1_fingerprint.py $(DATE)

coordination:
	python jobs/2_coordination.py $(DATE)

features:
	python jobs/3_features.py $(DATE)

cluster:
	python jobs/4_cluster.py $(DATE)

weekly: embeddings network follow-graph

embeddings:
	python jobs/5_embeddings.py $(DATE)

network:
	python jobs/6_network.py $(DATE)

follow-graph:
	python jobs/7_follow_graph.py $(DATE)

# Submit to KubeRay
k8s-daily:
	kubectl apply -f k8s/jobs/daily-pipeline.yaml

k8s-weekly:
	kubectl apply -f k8s/jobs/weekly-pipeline.yaml
```

---

## Dependencies

```
# requirements.txt
ray[data]>=2.9.0
duckdb>=0.9.0
pyarrow>=14.0.0
pandas>=2.0.0
numpy>=1.24.0
scipy>=1.11.0
scikit-learn>=1.3.0
hdbscan>=0.8.33
umap-learn>=0.5.4
networkx>=3.2
python-louvain>=0.16
websockets>=12.0
```

---

## Storage Estimates

| Data | Per Day | 30 Days |
|------|---------|---------|
| events/*.parquet | ~200-500 MB | ~6-15 GB |
| posts/*.parquet | ~100-300 MB | ~3-9 GB |
| follow_events/*.parquet | ~50-100 MB | ~1.5-3 GB |
| account_snapshots/*.parquet | ~20-50 MB | ~600 MB-1.5 GB |
| content_shares/*.parquet | ~50-150 MB | ~1.5-4.5 GB |
| coordination_events/*.parquet | ~10-50 MB | ~300 MB-1.5 GB |
| features/*.parquet | ~20-50 MB | ~600 MB-1.5 GB |
| follow_graph/*.parquet | ~100-200 MB | ~400-800 MB (weekly) |
| clusters/*.parquet | ~10-20 MB | ~300-600 MB |
| embeddings/*.parquet | ~50-100 MB | ~200-400 MB (weekly) |
| network_features/*.parquet | ~20-50 MB | ~80-200 MB (weekly) |
| **Total** | ~650 MB-1.5 GB | ~15-40 GB |
