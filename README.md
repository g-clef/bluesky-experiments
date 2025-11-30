# bluesky-experiments
Experiments examining data from the Bluesky firehose

# Intent

Identify clusters of accounts using metadata of bluesky posts. Don't do a deep analysis
of content, as that will require too much storage. Content can be manually
explored after clustering to try to identify the purpose of the clusters. Overall
the intent is to find bots and non-human influencers on Bluesky.

# Questions to answer/explore

Possible patterns:
 * accounts that repost just one account, or a limited set of accounts, into many other accounts conversations
 * accounts post over the full 24 hour period at a time, handle travel
 * accounts that respond to many accounts but with the same text
 * accounts that respond to many accounts with small posts, but always repost one account
 * accounts that consistently post identical content
 * accounts that consistently like the same accounts posts aka farmers
 * accounts that respond to lots of accounts that they don't follow
 * accounts that trigger a big spike of blocks after they post anything
 * accounts that have predictable inter-post times (regular intervals, fixed times of the day)
 * accounts that respond to posts unnaturally quickly (single digit seconds)
 * one of the above but also doing it close in time to other accounts doing the same thing
 * accounts doing the above that are also created at nearly the same time
 * Young accounts with lots of activity
 * Old accounts that had little activity but sudden spike later on.
 * stair-step jumps in followers un-correlated to a viral post
 * accounts that follow thousands of people with few followers of their own
 * accounts with nearly-identical sets of followers
 * identical avatars
 * similar bios
 * accounts that all post identical links but with different text.

Possible identifying features:
 * time period of posting (business hours in which timezone)
 * subject matter (find by manually checking after clustering)
 * identity of accounts they're responding to (particular set of targets/victims)
