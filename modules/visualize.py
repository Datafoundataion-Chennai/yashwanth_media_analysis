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

youtube_query = f"SELECT * FROM `{project_id}.{dataset_id}.{youtube_table}`"
news_query = f"SELECT * FROM `{project_id}.{dataset_id}.{news_table}`LIMIT 20000"

@st.cache_data
def load_youtube_data():
    try:
        logging.info("Loading YouTube data from BigQuery")
        return client.query(youtube_query).to_dataframe()
    except Exception as e:
        st.error(f"Error loading YouTube data: {e}")
        logging.error(f"Error loading YouTube data: {e}")
        return pd.DataFrame()

@st.cache_data
def load_news_data():
    try:
        logging.info("Loading News data from BigQuery")
        return client.query(news_query).to_dataframe()
    except Exception as e:
        st.error(f"Error loading News data: {e}")
        logging.error(f"Error loading News data: {e}")
        return pd.DataFrame()

# Load Data
df_youtube = load_youtube_data()
df_news = load_news_data()

# Convert Date Columns
try:
    df_youtube["publish_time"] = pd.to_datetime(df_youtube["publish_time"])
    df_news["date"] = pd.to_datetime(df_news["date"])
except Exception as e:
    st.error(f"Error converting date columns: {e}")
    logging.error(f"Error converting date columns: {e}")
    st.stop()

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
        channels = df_youtube["channel_name"].unique()
        selected_channel = st.selectbox("Select Channel:", ["All"] + list(channels), key="youtube_channel", 
                                        help="Choose a specific channel or 'All' to see everything.")
        youtube_start_date = st.date_input("Start Date", df_youtube["publish_time"].min().date(), key="youtube_start", 
                                           help="Pick the earliest date for videos.")
        youtube_end_date = st.date_input("End Date", df_youtube["publish_time"].max().date(), key="youtube_end", 
                                         help="Pick the latest date for videos.")
        field_options = ["channel_name", "category_id", "tags"]
        selected_field = st.selectbox("Analyze This Field:", field_options, key="youtube_field", 
                                      help="Choose what to focus on (e.g., channels, categories, or tags).")
        if selected_field == "tags":
            all_tags = df_youtube["tags"].str.split(", ").explode().unique()
            selected_tag = st.selectbox("Select Tag:", ["All"] + list(all_tags), key="youtube_tag", 
                                        help="Filter videos by a specific tag.")

    # Apply Filters
    filtered_youtube = df_youtube[
        (df_youtube["publish_time"].dt.date >= youtube_start_date) &
        (df_youtube["publish_time"].dt.date <= youtube_end_date)
    ]
    if selected_channel != "All":
        filtered_youtube = filtered_youtube[filtered_youtube["channel_name"] == selected_channel]
    if selected_field == "tags" and selected_tag and selected_tag != "All":
        filtered_youtube = filtered_youtube[filtered_youtube["tags"].str.contains(selected_tag, na=False)]
    filtered_youtube.index = range(1, len(filtered_youtube) + 1)

elif selected_dataset == "News":
    with st.sidebar.expander("🔍 News Filters", expanded=True):
        news_categories = df_news["category"].unique()
        selected_category = st.selectbox("Select Category:", ["All"] + list(news_categories), key="news_category", 
                                         help="Choose a news category or 'All'.")
        news_start_date = st.date_input("Start Date", df_news["date"].min().date(), key="news_start", 
                                        help="Earliest date for news articles.")
        news_end_date = st.date_input("End Date", df_news["date"].max().date(), key="news_end", 
                                      help="Latest date for news articles.")

    # Apply Filters
    filtered_news = df_news[
        (df_news["date"].dt.date >= news_start_date) &
        (df_news["date"].dt.date <= news_end_date)
    ]
    if selected_category != "All":
        filtered_news = filtered_news[filtered_news["category"] == selected_category]
    filtered_news.index = range(1, len(filtered_news) + 1)

# Pagination
def paginate_data(data, page_size, page_number):
    start = page_number * page_size
    end = start + page_size
    return data.iloc[start:end]

if "page" not in st.session_state:
    st.session_state.page = 0
page_size = 10

# Tabs
tab1, tab2, tab3 = st.tabs(["Data", "Visualizations", "Insights"])

# Tab 1: Data
with tab1:
    if selected_dataset == "YouTube":
        st.header("YouTube Data")
        paginated_youtube = paginate_data(filtered_youtube, page_size, st.session_state.page)
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
        paginated_news = paginate_data(filtered_news, page_size, st.session_state.page)
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

# Tab 2: Visualizations
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

        if selected_viz == "Total Views by Channel (Bar)":
            st.subheader("Total Views by Channel")
            channel_views = filtered_youtube.groupby("channel_name")["views"].sum().sort_values(ascending=False)
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.bar(channel_views.index, channel_views.values, color="skyblue")
            ax.set_xlabel("Channel Name")
            ax.set_ylabel("Total Views")
            ax.set_title("Total Views by Channel")
            plt.xticks(rotation=45, ha="right")
            plt.tight_layout()
            st.pyplot(fig)

        elif selected_viz == "Views Trend Over Time (Line)":
            st.subheader("Views Trend Over Time")
            filtered_youtube["date"] = filtered_youtube["publish_time"].dt.date
            views_over_time = filtered_youtube.groupby("date")["views"].sum()
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.plot(views_over_time.index, views_over_time.values, marker="o", linestyle="-", color="red")
            ax.set_xlabel("Date")
            ax.set_ylabel("Total Views")
            ax.set_title("Views Trend Over Time")
            plt.xticks(rotation=45)
            plt.tight_layout()
            st.pyplot(fig)

        elif selected_viz == "Subscribers Distribution (Pie)":
            st.subheader("Subscribers Distribution")
            subscribers = filtered_youtube.groupby("channel_name")["subscribers_count"].mean()
            fig, ax = plt.subplots(figsize=(8, 8))
            ax.pie(subscribers, labels=subscribers.index, autopct="%1.1f%%", startangle=90, colors=plt.cm.Paired.colors)
            ax.set_title("Subscribers Distribution")
            plt.tight_layout()
            st.pyplot(fig)

        elif selected_viz == "Field Counts (Bar)":
            st.subheader(f"Count of {selected_field}")
            if selected_field == "tags":
                counts = filtered_youtube["tags"].str.split(", ").explode().value_counts().head(10)
            else:
                counts = filtered_youtube[selected_field].value_counts().head(10)
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.bar(counts.index, counts.values, color="purple")
            ax.set_xlabel(selected_field.capitalize())
            ax.set_ylabel("Count")
            ax.set_title(f"Top 10 {selected_field.capitalize()} Counts")
            plt.xticks(rotation=45, ha="right")
            plt.tight_layout()
            st.pyplot(fig)

        elif selected_viz == "Views vs Likes (Scatter)":
            st.subheader("Views vs Likes")
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.scatter(filtered_youtube["views"], filtered_youtube["likes"], alpha=0.5, color="green")
            ax.set_xlabel("Views")
            ax.set_ylabel("Likes")
            ax.set_title("Views vs Likes")
            plt.tight_layout()
            st.pyplot(fig)

        elif selected_viz == "Trending Channels (Line)":
            st.subheader("Trending Channels by Views Over Time")
            filtered_youtube["date"] = filtered_youtube["publish_time"].dt.date
            trending = filtered_youtube.groupby(["date", "channel_name"])["views"].sum().unstack()
            fig, ax = plt.subplots(figsize=(12, 6))
            for channel in trending.columns:
                ax.plot(trending.index, trending[channel], marker="o", linestyle="-", label=channel)
            ax.set_xlabel("Date")
            ax.set_ylabel("Total Views")
            ax.set_title("Trending Channels by Views")
            ax.legend()
            plt.xticks(rotation=45)
            plt.tight_layout()
            st.pyplot(fig)

    elif selected_dataset == "News":
        st.header("News Visualizations")
        viz_options = ["Articles Over Time (Line)", "Top Categories (Bar)"]
        selected_viz = st.selectbox("Pick a Chart:", viz_options, key="news_viz", 
                                    help="Choose a chart to explore news data!")

        if selected_viz == "Articles Over Time (Line)":
            st.subheader("News Articles Over Time")
            filtered_news["date"] = filtered_news["date"].dt.date
            news_over_time = filtered_news.groupby("date").size()
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.plot(news_over_time.index, news_over_time.values, marker="o", linestyle="-", color="green")
            ax.set_xlabel("Date")
            ax.set_ylabel("Number of Articles")
            ax.set_title("News Articles Over Time")
            plt.xticks(rotation=45)
            plt.tight_layout()
            st.pyplot(fig)

        elif selected_viz == "Top Categories (Bar)":
            st.subheader("Top Categories by Articles")
            news_category_count = filtered_news["category"].value_counts()
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.bar(news_category_count.index, news_category_count.values, color="orange")
            ax.set_xlabel("Category")
            ax.set_ylabel("Number of Articles")
            ax.set_title("Top Categories by Articles")
            plt.xticks(rotation=45, ha="right")
            plt.tight_layout()
            st.pyplot(fig)

# Tab 3: Insights
with tab3:
    if selected_dataset == "YouTube":
        st.header("YouTube Insights")
        total_videos = len(filtered_youtube)
        total_views = filtered_youtube["views"].sum()
        top_channel = filtered_youtube.groupby("channel_name")["views"].sum().idxmax()
        st.write(f"**Total Videos**: {total_videos}")
        st.write(f"**Total Views**: {total_views:,}")
        st.write(f"**Top Channel**: {top_channel} with the most views!")

    elif selected_dataset == "News":
        st.header("News Insights")
        total_articles = len(filtered_news)
        top_category = filtered_news["category"].value_counts().idxmax()
        st.write(f"**Total Articles**: {total_articles}")
        st.write(f"**Top Category**: {top_category} has the most articles!")

st.markdown("---")
