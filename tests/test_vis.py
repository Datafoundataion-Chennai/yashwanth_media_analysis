import unittest
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

class TestMediaAnalyticsDashboard(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Sample YouTube data
        cls.youtube_data = {
            "channel_name": ["Channel A", "Channel B", "Channel A", "Channel C"],
            "publish_time": [datetime.now() - timedelta(days=i) for i in range(4)],
            "views": [1000, 2000, 1500, 3000],
            "subscribers_count": [5000, 10000, 7500, 12000]
        }
        cls.df_youtube = pd.DataFrame(cls.youtube_data)

        # Sample News data
        cls.news_data = {
            "category": ["Politics", "Sports", "Technology", "Politics"],
            "date": [datetime.now() - timedelta(days=i) for i in range(4)],
            "title": ["Article 1", "Article 2", "Article 3", "Article 4"]
        }
        cls.df_news = pd.DataFrame(cls.news_data)

    def test_load_youtube_data(self):
        # Test if the YouTube data is loaded correctly
        self.assertIsInstance(self.df_youtube, pd.DataFrame)
        self.assertFalse(self.df_youtube.empty)
        self.assertEqual(len(self.df_youtube), 4)

    def test_load_news_data(self):
        # Test if the News data is loaded correctly
        self.assertIsInstance(self.df_news, pd.DataFrame)
        self.assertFalse(self.df_news.empty)
        self.assertEqual(len(self.df_news), 4)

    def test_date_conversion(self):
        # Test if the date columns are correctly converted to datetime
        self.df_youtube["publish_time"] = pd.to_datetime(self.df_youtube["publish_time"])
        self.df_news["date"] = pd.to_datetime(self.df_news["date"])

        self.assertTrue(pd.api.types.is_datetime64_any_dtype(self.df_youtube["publish_time"]))
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(self.df_news["date"]))

    def test_youtube_filtering(self):
        # Test YouTube filtering logic
        start_date = datetime.now() - timedelta(days=2)
        end_date = datetime.now()
        filtered_youtube = self.df_youtube[
            (self.df_youtube["publish_time"].dt.date >= start_date.date()) &
            (self.df_youtube["publish_time"].dt.date <= end_date.date())
        ]
        self.assertEqual(len(filtered_youtube), 3)

        # Test channel filtering
        filtered_youtube = filtered_youtube[filtered_youtube["channel_name"] == "Channel A"]
        self.assertEqual(len(filtered_youtube), 2)

    def test_news_filtering(self):
    # Test News filtering logic
        start_date = datetime.now() - timedelta(days=2)
        end_date = datetime.now()

    # Apply date range filter
        filtered_news = self.df_news[
            (self.df_news["date"].dt.date >= start_date.date()) &
            (self.df_news["date"].dt.date <= end_date.date())
        ]
        self.assertEqual(len(filtered_news), 2)  # Should return 2 rows

    # Apply category filter
        filtered_news = filtered_news[filtered_news["category"] == "Politics"]
        self.assertEqual(len(filtered_news), 2)  # Should return 2 rows


    def test_pagination(self):
        # Test pagination logic
        page_size = 2
        page_number = 0
        paginated_data = self.df_youtube.iloc[page_number * page_size:(page_number + 1) * page_size]
        self.assertEqual(len(paginated_data), page_size)

    def test_youtube_visualization(self):
        # Test YouTube visualization logic
        channel_views = self.df_youtube.groupby("channel_name")["views"].sum().sort_values(ascending=False)
        fig, ax = plt.subplots()
        ax.barh(channel_views.index, channel_views.values)
        self.assertIsNotNone(fig)

    def test_news_visualization(self):
        # Test News visualization logic
        news_category_count = self.df_news["category"].value_counts()
        fig, ax = plt.subplots()
        ax.barh(news_category_count.index, news_category_count.values)
        self.assertIsNotNone(fig)

if __name__ == "__main__":
    unittest.main()