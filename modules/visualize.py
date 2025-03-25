import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
import os
import logging

logging.basicConfig(filename="data_visualization.log", level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Set Google Credentials
try:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\YASHWANTH\Desktop\Media Analytics\keys\media-content-analytics-454110-42ae23de3324.json"
    client = bigquery.Client()
except Exception as e:
    st.error(f"Error initializing BigQuery client: {e}")
    logging.error(f"Error initializing BigQuery client: {e}")
    st.stop()

# Project & Dataset Info
project_id = "media-content-analytics-454110"
dataset_id = "News_Dataset"
youtube_table = "youtubeapi_data"
news_table = "News"

@st.cache_data
def run_query(query):
    try:
        logging.info(f"Executing query: {query}")
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Error executing query: {e}")
        logging.error(f"Error executing query: {e}")
        return pd.DataFrame()

st.title("Media Content Analytics Dashboard")
st.markdown("Explore YouTube and News data easily with charts and insights!")
logging.info("Dashboard Loaded")

# Sidebar
st.sidebar.title("Control Panel")
dataset_options = ["YouTube", "News"]
selected_dataset = st.sidebar.selectbox("Choose a Dataset:", dataset_options, help="Pick YouTube for video stats or News for articles.")
logging.info(f"Selected Dataset: {selected_dataset}")

# Filters
if selected_dataset == "YouTube":
    with st.sidebar.expander("YouTube Filters", expanded=True):
        channels_query = f"SELECT DISTINCT channel_name FROM `{project_id}.{dataset_id}.{youtube_table}`"
        channels_df = run_query(channels_query)
        channels = channels_df["channel_name"].tolist()
        selected_channel = st.selectbox("Select Channel:", ["All"] + channels, key="youtube_channel", 
                                        help="Choose a specific channel or 'All' to see everything.")
        date_range_query = f"SELECT MIN(publish_time) as min_date, MAX(publish_time) as max_date FROM `{project_id}.{dataset_id}.{youtube_table}`"
        date_range = run_query(date_range_query)
        min_date = pd.to_datetime(date_range["min_date"].iloc[0]).date()
        max_date = pd.to_datetime(date_range["max_date"].iloc[0]).date()
        youtube_start_date = st.date_input("Start Date", min_date, key="youtube_start", 
                                           help="Pick the earliest date for videos.")
        youtube_end_date = st.date_input("End Date", max_date, key="youtube_end", 
                                         help="Pick the latest date for videos.")
        field_options = ["channel_name", "category_id", "tags"]
        selected_field = st.selectbox("Analyze This Field:", field_options, key="youtube_field", 
                                      help="Choose what to focus on (e.g., channels, categories, or tags).")
        if selected_field == "tags":
            tags_query = f"SELECT DISTINCT REGEXP_EXTRACT_ALL(tags, r'[^,]+') as tag FROM `{project_id}.{dataset_id}.{youtube_table}`, UNNEST(SPLIT(tags, ', ')) as tags"
            tags_df = run_query(tags_query)
            all_tags = tags_df["tag"].explode().unique()
            selected_tag = st.selectbox("Select Tag:", ["All"] + list(all_tags), key="youtube_tag", 
                                        help="Filter videos by a specific tag.")

    youtube_base_query = f"""
        SELECT *
        FROM `{project_id}.{dataset_id}.{youtube_table}`
        WHERE publish_time BETWEEN '{youtube_start_date}' AND '{youtube_end_date}'
        {f"AND channel_name = '{selected_channel}'" if selected_channel != "All" else ""}
        {f"AND tags LIKE '%{selected_tag}%'" if selected_field == "tags" and selected_tag != "All" else ""}
    """

elif selected_dataset == "News":
    with st.sidebar.expander("News Filters", expanded=True):
        categories_query = f"SELECT DISTINCT category FROM `{project_id}.{dataset_id}.{news_table}`"
        categories_df = run_query(categories_query)
        news_categories = categories_df["category"].tolist()
        selected_category = st.selectbox("Select Category:", ["All"] + news_categories, key="news_category", 
                                         help="Choose a news category or 'All'.")
        date_range_query = f"SELECT MIN(date) as min_date, MAX(date) as max_date FROM `{project_id}.{dataset_id}.{news_table}`"
        date_range = run_query(date_range_query)
        min_date = pd.to_datetime(date_range["min_date"].iloc[0]).date()
        max_date = pd.to_datetime(date_range["max_date"].iloc[0]).date()
        news_start_date = st.date_input("Start Date", min_date, key="news_start", 
                                        help="Earliest date for news articles.")
        news_end_date = st.date_input("End Date", max_date, key="news_end", 
                                      help="Latest date for news articles.")

    news_base_query = f"""
        SELECT *
        FROM `{project_id}.{dataset_id}.{news_table}`
        WHERE date BETWEEN '{news_start_date}' AND '{news_end_date}'
        {f"AND category = '{selected_category}'" if selected_category != "All" else ""}
    """

def paginate_query(base_query, page_size, page_number):
    offset = page_number * page_size
    paginated_query = f"{base_query} LIMIT {page_size} OFFSET {offset}"
    return run_query(paginated_query)

if "page" not in st.session_state:
    st.session_state.page = 0

page_size = 10

tab1, tab2, tab3 = st.tabs(["Data", "Visualizations", "Insights"])

with tab1:
    start_index = st.session_state.page * page_size + 1

    if selected_dataset == "YouTube":
        st.header("YouTube Data")
        paginated_youtube = paginate_query(youtube_base_query, page_size, st.session_state.page)
        paginated_youtube.index = range(start_index, start_index + len(paginated_youtube))
        st.dataframe(paginated_youtube, use_container_width=True)
        col1, col2 = st.columns(2)
        with col1:
            if st.session_state.page > 0:
                if st.button("Previous Page", key="prev_youtube"):
                    st.session_state.page -= 1
                    st.rerun()
        with col2:
            if len(paginated_youtube) == page_size:
                if st.button("Next Page", key="next_youtube"):
                    st.session_state.page += 1
                    st.rerun()
    elif selected_dataset == "News":
        st.header("News Data")
        paginated_news = paginate_query(news_base_query, page_size, st.session_state.page)
        paginated_news.index = range(start_index, start_index + len(paginated_news))
        st.dataframe(paginated_news, use_container_width=True)
        col1, col2 = st.columns(2)
        with col1:
            if st.session_state.page > 0:
                if st.button("Previous Page", key="prev_news"):
                    st.session_state.page -= 1
                    st.rerun()
        with col2:
            if len(paginated_news) == page_size:
                if st.button("Next Page", key="next_news"):
                    st.session_state.page += 1
                    st.rerun()

with tab2:
    if selected_dataset == "YouTube":
        st.header("YouTube Visualizations")
        viz_options = [
            "Total Views by Channel (Bar)",
            "Views Trend Over Time (Line)",
            "Subscribers Distribution (Pie)",
            "Field Counts (Bar)",
            "Views vs Likes (Scatter)",
            "Trending Channels (Line)"
        ]
        selected_viz = st.selectbox("Pick a Chart:", viz_options, key="youtube_viz", 
                                    help="Choose a chart to see your data in action!")

        # Apply a modern style
        plt.style.use('seaborn-v0_8')  # Updated seaborn style

        if selected_viz == "Total Views by Channel (Bar)":
            st.subheader("Total Views by Channel")
            query = f"""
                SELECT channel_name, SUM(views) as total_views
                FROM ({youtube_base_query})
                GROUP BY channel_name
                ORDER BY total_views DESC
            """
            df = run_query(query)
            fig, ax = plt.subplots(figsize=(10, 6))
            bars = ax.bar(df["channel_name"], df["total_views"], 
                         color=plt.cm.Set3(range(len(df))), 
                         edgecolor='black', linewidth=0.5)
            ax.set_xlabel("Channel Name", fontsize=12)
            ax.set_ylabel("Total Views", fontsize=12)
            ax.set_title("Total Views by Channel", fontsize=14, pad=15)
            ax.grid(True, axis='y', linestyle='--', alpha=0.7)
            plt.xticks(rotation=45, ha="right", fontsize=10)
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{int(height):,}', ha='center', va='bottom', fontsize=9)
            plt.tight_layout()
            st.pyplot(fig)

        elif selected_viz == "Views Trend Over Time (Line)":
            st.subheader("Views Trend Over Time")
            query = f"""
                SELECT DATE(publish_time) as date, SUM(views) as total_views
                FROM ({youtube_base_query})
                GROUP BY DATE(publish_time)
                ORDER BY date
            """
            df = run_query(query)
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.plot(df["date"], df["total_views"], marker="o", 
                   linestyle="-", color="#FF6B6B", linewidth=2)
            ax.set_xlabel("Date", fontsize=12)
            ax.set_ylabel("Total Views", fontsize=12)
            ax.set_title("Views Trend Over Time", fontsize=14, pad=15)
            ax.grid(True, linestyle='--', alpha=0.7)
            plt.xticks(rotation=45, fontsize=10)
            plt.tight_layout()
            st.pyplot(fig)

        elif selected_viz == "Subscribers Distribution (Pie)":
            st.subheader("Subscribers Distribution")
            query = f"""
                SELECT channel_name, AVG(subscribers_count) as avg_subscribers
                FROM ({youtube_base_query})
                GROUP BY channel_name
            """
            df = run_query(query)
            fig, ax = plt.subplots(figsize=(8, 8))
            wedges, texts, autotexts = ax.pie(df["avg_subscribers"], 
                                            labels=df["channel_name"], 
                                            autopct="%1.1f%%", 
                                            startangle=90, 
                                            colors=plt.cm.Pastel1(range(len(df))),
                                            wedgeprops={'edgecolor': 'white', 'linewidth': 1})
            ax.set_title("Subscribers Distribution", fontsize=14, pad=15)
            for text in texts:
                text.set_fontsize(10)
            for autotext in autotexts:
                autotext.set_fontsize(9)
                autotext.set_color('black')
            plt.tight_layout()
            st.pyplot(fig)

        elif selected_viz == "Field Counts (Bar)":
            st.subheader(f"Count of {selected_field}")
            if selected_field == "tags":
                query = f"""
                    SELECT tag, COUNT(*) as count
                    FROM ({youtube_base_query}), UNNEST(SPLIT(tags, ', ')) as tag
                    GROUP BY tag
                    ORDER BY count DESC
                    LIMIT 10
                """
            else:
                query = f"""
                    SELECT {selected_field}, COUNT(*) as count
                    FROM ({youtube_base_query})
                    GROUP BY {selected_field}
                    ORDER BY count DESC
                    LIMIT 10
                """
            df = run_query(query)
            fig, ax = plt.subplots(figsize=(10, 6))
            bars = ax.bar(df[selected_field if selected_field != "tags" else "tag"], 
                         df["count"], 
                         color=plt.cm.Set2(range(len(df))),
                         edgecolor='black', linewidth=0.5)
            ax.set_xlabel(selected_field.capitalize(), fontsize=12)
            ax.set_ylabel("Count", fontsize=12)
            ax.set_title(f"Top 10 {selected_field.capitalize()} Counts", fontsize=14, pad=15)
            ax.grid(True, axis='y', linestyle='--', alpha=0.7)
            plt.xticks(rotation=45, ha="right", fontsize=10)
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{int(height)}', ha='center', va='bottom', fontsize=9)
            plt.tight_layout()
            st.pyplot(fig)

        elif selected_viz == "Views vs Likes (Scatter)":
            st.subheader("Views vs Likes")
            query = f"""
                SELECT views, likes
                FROM ({youtube_base_query})
            """
            df = run_query(query)
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.scatter(df["views"], df["likes"], alpha=0.6, 
                      color="#4ECDC4", edgecolors='black', linewidth=0.5)
            ax.set_xlabel("Views", fontsize=12)
            ax.set_ylabel("Likes", fontsize=12)
            ax.set_title("Views vs Likes", fontsize=14, pad=15)
            ax.grid(True, linestyle='--', alpha=0.7)
            plt.tight_layout()
            st.pyplot(fig)

        elif selected_viz == "Trending Channels (Line)":
            st.subheader("Trending Channels by Views Over Time")
            query = f"""
                SELECT DATE(publish_time) as date, channel_name, SUM(views) as total_views
                FROM ({youtube_base_query})
                GROUP BY DATE(publish_time), channel_name
                ORDER BY date
            """
            df = run_query(query)
            trending = df.pivot(index="date", columns="channel_name", values="total_views").fillna(0)
            fig, ax = plt.subplots(figsize=(12, 6))
            colors = plt.cm.tab20(range(len(trending.columns)))
            for i, channel in enumerate(trending.columns):
                ax.plot(trending.index, trending[channel], marker="o", 
                       linestyle="-", label=channel, color=colors[i], linewidth=2)
            ax.set_xlabel("Date", fontsize=12)
            ax.set_ylabel("Total Views", fontsize=12)
            ax.set_title("Trending Channels by Views", fontsize=14, pad=15)
            ax.grid(True, linestyle='--', alpha=0.7)
            ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
            plt.xticks(rotation=45, fontsize=10)
            plt.tight_layout()
            st.pyplot(fig)

    elif selected_dataset == "News":
        st.header("News Visualizations")
        viz_options = ["Articles Over Time (Line)", "Top Categories (Bar)"]
        selected_viz = st.selectbox("Pick a Chart:", viz_options, key="news_viz", 
                                    help="Choose a chart to explore news data!")

        plt.style.use('seaborn-v0_8')

        if selected_viz == "Articles Over Time (Line)":
            st.subheader("News Articles Over Time")
            query = f"""
                SELECT DATE(date) as date, COUNT(*) as article_count
                FROM ({news_base_query})
                GROUP BY DATE(date)
                ORDER BY date
            """
            df = run_query(query)
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.plot(df["date"], df["article_count"], marker="o", 
                   linestyle="-", color="#45B7D1", linewidth=2)
            ax.set_xlabel("Date", fontsize=12)
            ax.set_ylabel("Number of Articles", fontsize=12)
            ax.set_title("News Articles Over Time", fontsize=14, pad=15)
            ax.grid(True, linestyle='--', alpha=0.7)
            plt.xticks(rotation=45, fontsize=10)
            plt.tight_layout()
            st.pyplot(fig)

        elif selected_viz == "Top Categories (Bar)":
            st.subheader("Top Categories by Articles")
            query = f"""
                SELECT category, COUNT(*) as article_count
                FROM ({news_base_query})
                GROUP BY category
                ORDER BY article_count DESC
            """
            df = run_query(query)
            fig, ax = plt.subplots(figsize=(10, 6))
            bars = ax.bar(df["category"], df["article_count"], 
                         color=plt.cm.Set1(range(len(df))),
                         edgecolor='black', linewidth=0.5)
            ax.set_xlabel("Category", fontsize=12)
            ax.set_ylabel("Number of Articles", fontsize=12)
            ax.set_title("Top Categories by Articles", fontsize=14, pad=15)
            ax.grid(True, axis='y', linestyle='--', alpha=0.7)
            plt.xticks(rotation=45, ha="right", fontsize=10)
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{int(height)}', ha='center', va='bottom', fontsize=9)
            plt.tight_layout()
            st.pyplot(fig)

with tab3:
    if selected_dataset == "YouTube":
        st.header("YouTube Insights")
        total_query = f"""
            SELECT COUNT(*) as total_videos, SUM(views) as total_views
            FROM ({youtube_base_query})
        """
        total_df = run_query(total_query)
        top_channel_query = f"""
            SELECT channel_name, SUM(views) as total_views
            FROM ({youtube_base_query})
            GROUP BY channel_name
            ORDER BY total_views DESC
            LIMIT 1
        """
        top_channel_df = run_query(top_channel_query)

        if not total_df.empty and not top_channel_df.empty:
            total_videos = total_df["total_videos"].iloc[0]
            total_views = total_df["total_views"].iloc[0]
            top_channel = top_channel_df["channel_name"].iloc[0]
            st.write(f"**Total Videos**: {total_videos}")
            st.write(f"**Total Views**: {total_views:,}")
            st.write(f"**Top Channel**: {top_channel} with the most views!")
        else:
            st.write("No data available for insights.")

    elif selected_dataset == "News":
        st.header("News Insights")
        total_query = f"""
            SELECT COUNT(*) as total_articles
            FROM ({news_base_query})
        """
        total_df = run_query(total_query)
        top_category_query = f"""
            SELECT category, COUNT(*) as article_count
            FROM ({news_base_query})
            GROUP BY category
            ORDER BY article_count DESC
            LIMIT 1
        """
        top_category_df = run_query(top_category_query)

        if not total_df.empty and not top_category_df.empty:
            total_articles = total_df["total_articles"].iloc[0]
            top_category = top_category_df["category"].iloc[0]
            st.write(f"**Total Articles**: {total_articles}")
            st.write(f"**Top Category**: {top_category} has the most articles!")
        else:
            st.write("No data available for insights.")

st.markdown("---")