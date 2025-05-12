"""
Reddit API connector for retrieving post and comment data.
Uses the PRAW (Python Reddit API Wrapper) library to access Reddit data.
"""

import os
import json
import logging
from datetime import datetime
import praw

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RedditAPI:
    """
    Client for interacting with the Reddit API.
    Retrieves posts, comments, and other data from sports subreddits.
    """

    def __init__(self, client_id=None, client_secret=None, user_agent=None):
        """
        Initialize the RedditAPI client.

        Args:
            client_id (str, optional): Reddit API client ID
            client_secret (str, optional): Reddit API client secret
            user_agent (str, optional): User agent string for Reddit API
        """
        self.client_id = client_id or os.environ.get('REDDIT_CLIENT_ID')
        self.client_secret = client_secret or os.environ.get('REDDIT_CLIENT_SECRET')
        self.user_agent = user_agent or os.environ.get('REDDIT_USER_AGENT', 'sports_data_pipeline')

        if not all([self.client_id, self.client_secret]):
            raise ValueError("Reddit API credentials are required")

        # Initialize the Reddit API client
        self.reddit = praw.Reddit(
            client_id=self.client_id,
            client_secret=self.client_secret,
            user_agent=self.user_agent
        )

    def get_subreddit_posts(self, subreddit_name, limit=25, sort_by='hot'):
        """
        Get posts from a subreddit.

        Args:
            subreddit_name (str): Name of the subreddit
            limit (int, optional): Maximum number of posts to retrieve
            sort_by (str, optional): Sort method ('hot', 'new', 'top', 'rising')

        Returns:
            list: List of post dictionaries
        """
        logger.info(f"Getting {sort_by} posts from r/{subreddit_name}")

        try:
            subreddit = self.reddit.subreddit(subreddit_name)

            # Get posts based on sort method
            if sort_by == 'hot':
                posts_generator = subreddit.hot(limit=limit)
            elif sort_by == 'new':
                posts_generator = subreddit.new(limit=limit)
            elif sort_by == 'top':
                posts_generator = subreddit.top(limit=limit)
            elif sort_by == 'rising':
                posts_generator = subreddit.rising(limit=limit)
            else:
                posts_generator = subreddit.hot(limit=limit)

            # Convert posts to dictionaries
            posts = []
            for post in posts_generator:
                post_dict = {
                    'id': post.id,
                    'title': post.title,
                    'author': post.author.name if post.author else '[deleted]',
                    'created_utc': datetime.fromtimestamp(post.created_utc).isoformat(),
                    'score': post.score,
                    'upvote_ratio': post.upvote_ratio,
                    'num_comments': post.num_comments,
                    'url': post.url,
                    'permalink': post.permalink,
                    'selftext': post.selftext,
                    'is_self': post.is_self,
                    'flair': post.link_flair_text
                }
                posts.append(post_dict)

            # Save raw data to file with timestamp
            self._save_raw_data(posts, f"subreddit_{subreddit_name}_{sort_by}")

            logger.info(f"Retrieved {len(posts)} posts from r/{subreddit_name}")
            return posts

        except Exception as e:
            logger.error(f"Error retrieving posts from r/{subreddit_name}: {str(e)}")
            return []

    def get_post_comments(self, post_id, sort_by='top', limit=100):
        """
        Get comments for a specific post.

        Args:
            post_id (str): ID of the post
            sort_by (str, optional): Sort method ('top', 'new', 'controversial', 'old')
            limit (int, optional): Maximum number of comments to retrieve

        Returns:
            list: List of comment dictionaries
        """
        logger.info(f"Getting comments for post {post_id}")

        try:
            submission = self.reddit.submission(id=post_id)
            submission.comment_sort = sort_by
            submission.comments.replace_more(limit=0)  # Don't fetch MoreComments

            # Get comments (limited to specified number)
            comments = submission.comments.list()[:limit]

            # Convert comments to dictionaries
            comment_dicts = []
            for comment in comments:
                comment_dict = {
                    'id': comment.id,
                    'author': comment.author.name if comment.author else '[deleted]',
                    'created_utc': datetime.fromtimestamp(comment.created_utc).isoformat(),
                    'body': comment.body,
                    'score': comment.score,
                    'is_submitter': comment.is_submitter,
                    'permalink': comment.permalink,
                    'parent_id': comment.parent_id
                }
                comment_dicts.append(comment_dict)

            # Save raw data to file with timestamp
            self._save_raw_data(comment_dicts, f"post_comments_{post_id}")

            logger.info(f"Retrieved {len(comment_dicts)} comments for post {post_id}")
            return comment_dicts

        except Exception as e:
            logger.error(f"Error retrieving comments for post {post_id}: {str(e)}")
            return []

    def search_posts(self, query, subreddit=None, limit=25, sort='relevance'):
        """
        Search for posts matching a query.

        Args:
            query (str): Search query
            subreddit (str, optional): Restrict search to a specific subreddit
            limit (int, optional): Maximum number of results to return
            sort (str, optional): Sort method ('relevance', 'hot', 'new', 'top')

        Returns:
            list: List of post dictionaries
        """
        logger.info(f"Searching for posts with query: '{query}'")

        try:
            if subreddit:
                search_results = self.reddit.subreddit(subreddit).search(query, sort=sort, limit=limit)
            else:
                search_results = self.reddit.subreddit('all').search(query, sort=sort, limit=limit)

            # Convert search results to dictionaries
            posts = []
            for post in search_results:
                post_dict = {
                    'id': post.id,
                    'title': post.title,
                    'subreddit': post.subreddit.display_name,
                    'author': post.author.name if post.author else '[deleted]',
                    'created_utc': datetime.fromtimestamp(post.created_utc).isoformat(),
                    'score': post.score,
                    'upvote_ratio': post.upvote_ratio,
                    'num_comments': post.num_comments,
                    'url': post.url,
                    'permalink': post.permalink,
                    'selftext': post.selftext,
                    'is_self': post.is_self
                }
                posts.append(post_dict)

            # Save raw data to file with timestamp
            search_query = query.replace(" ", "_")[:30]  # Truncate long queries for filename
            self._save_raw_data(posts, f"search_{search_query}")

            logger.info(f"Retrieved {len(posts)} posts matching query '{query}'")
            return posts

        except Exception as e:
            logger.error(f"Error searching for posts: {str(e)}")
            return []

    def get_sport_subreddits(self, sport_name):
        """
        Get a list of subreddits related to a specific sport.

        Args:
            sport_name (str): Name of the sport

        Returns:
            list: List of subreddit dictionaries
        """
        logger.info(f"Finding subreddits related to {sport_name}")

        try:
            # Search for subreddits related to the sport
            subreddits = self.reddit.subreddits.search(sport_name, limit=10)

            # Convert subreddits to dictionaries
            subreddit_dicts = []
            for subreddit in subreddits:
                subreddit_dict = {
                    'id': subreddit.id,
                    'name': subreddit.display_name,
                    'title': subreddit.title,
                    'description': subreddit.description,
                    'subscribers': subreddit.subscribers,
                    'created_utc': datetime.fromtimestamp(subreddit.created_utc).isoformat(),
                    'url': subreddit.url
                }
                subreddit_dicts.append(subreddit_dict)

            # Save raw data to file with timestamp
            self._save_raw_data(subreddit_dicts, f"sport_subreddits_{sport_name}")

            logger.info(f"Found {len(subreddit_dicts)} subreddits related to {sport_name}")
            return subreddit_dicts

        except Exception as e:
            logger.error(f"Error finding subreddits for {sport_name}: {str(e)}")
            return []

    def get_match_thread(self, team_name, subreddit_name='soccer'):
        """
        Find a match thread for a specific team.

        Args:
            team_name (str): Name of the team
            subreddit_name (str, optional): Subreddit to search in

        Returns:
            dict: Match thread post if found, None otherwise
        """
        logger.info(f"Looking for match thread for {team_name} in r/{subreddit_name}")

        try:
            # Search for posts with "Match Thread" and team name
            query = f"Match Thread {team_name}"
            search_results = self.reddit.subreddit(subreddit_name).search(
                query, sort='new', time_filter='day', limit=5
            )

            # Find the most relevant match thread
            for post in search_results:
                if "Match Thread" in post.title and team_name.lower() in post.title.lower():
                    # Convert post to dictionary
                    post_dict = {
                        'id': post.id,
                        'title': post.title,
                        'author': post.author.name if post.author else '[deleted]',
                        'created_utc': datetime.fromtimestamp(post.created_utc).isoformat(),
                        'score': post.score,
                        'upvote_ratio': post.upvote_ratio,
                        'num_comments': post.num_comments,
                        'url': post.url,
                        'permalink': post.permalink,
                        'selftext': post.selftext
                    }

                    # Save raw data to file with timestamp
                    self._save_raw_data(post_dict, f"match_thread_{team_name}")

                    logger.info(f"Found match thread for {team_name}: {post.title}")
                    return post_dict

            logger.info(f"No match thread found for {team_name} in r/{subreddit_name}")
            return None

        except Exception as e:
            logger.error(f"Error finding match thread for {team_name}: {str(e)}")
            return None

    def _save_raw_data(self, data, prefix):
        """
        Save raw data to a file with timestamp.

        Args:
            data (list/dict): Data to save
            prefix (str): Prefix for the filename
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = f"data/raw/reddit_{prefix}_{timestamp}.json"

        # Ensure directory exists
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        # Write data to file
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(f"Saved raw data to {filepath}")


# Example usage
if __name__ == "__main__":
    # Initialize the API client
    api = RedditAPI()

    # Example: Get posts from soccer subreddit
    posts = api.get_subreddit_posts("soccer", limit=5, sort_by="hot")

    # Print the first post
    if posts:
        first_post = posts[0]
        print(f"Post: {first_post['title']}")
        print(f"Score: {first_post['score']} with {first_post['num_comments']} comments")
        print(f"Posted at: {first_post['created_utc']}")

        # Get comments for this post
        comments = api.get_post_comments(first_post['id'], limit=5)

        if comments:
            print(f"\nFound {len(comments)} comments")
            print(f"Top comment: {comments[0]['body'][:100]}...")

    # Example: Search for posts about a specific team
    team_posts = api.search_posts("Manchester United", subreddit="soccer", limit=3)

    if team_posts:
        print(f"\nFound {len(team_posts)} posts about Manchester United")
        print(f"Most relevant: {team_posts[0]['title']}")

    # Example: Find match thread
    match_thread = api.get_match_thread("Liverpool")

    if match_thread:
        print(f"\nFound match thread: {match_thread['title']}")
        print(f"With {match_thread['num_comments']} comments")