from googleapiclient.discovery import build
import pandas as pd
import pymysql
import streamlit as st


st.set_page_config(
    page_title="YouTube Analytics",
    page_icon="📊", 
    layout="wide"
)


st.markdown("<h1 style='text-align: center;'>Shanth's DashBoard</h1>", unsafe_allow_html=True)
a = st.selectbox(label="Select your Desired work page",options=["FAQ","Data Scrapping"])

if a == "Data Scrapping":
        st.markdown("<h1 style='text-align: center;'>Youtube Data Scrapping</h1>",unsafe_allow_html=True)

        a = st.text_input("Enter your channel ID:")

        #Basic Details
        api_key = "AIzaSyDkW3NmT3ccQqMTaP0k1hrqUl1CYF6eiRs"
        channel_id = a
        youtube = build('youtube','v3',developerKey = api_key)
        my_connection = pymysql.connect(host='127.0.0.1',user = 'root',passwd="Shashi@007",database='youtube_data')

        #creating func to extract channel details
        def get_channel_details(youtube,channel_id):
            all_data = []
            request = youtube.channels().list(
                part = 'snippet,contentDetails,statistics,status',
                id = channel_id)
            response = request.execute()
            data = dict(channel_id = response['items'][0]['id'],
                        channel_name =response['items'][0]['snippet']['title'],
                        channel_views =response['items'][0]['statistics']['viewCount'],
                        Total_videos = response['items'][0]['statistics']['videoCount'],
                        channel_description = response['items'][0]['snippet']['description'],
                        channel_status = response['items'][0]['status']['privacyStatus'],
                        playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
                        )
            all_data.append(data)
            return all_data

            #creating func to extract channel details
        def get_channel_details(youtube,channel_id):
            all_data = []
            request = youtube.channels().list(
                part = 'snippet,contentDetails,statistics,status',
                id = channel_id)
            response = request.execute()
            data = dict(channel_id = response['items'][0]['id'],
                        channel_name =response['items'][0]['snippet']['title'],
                        channel_views =response['items'][0]['statistics']['viewCount'],
                        Total_videos = response['items'][0]['statistics']['videoCount'],
                        channel_description = response['items'][0]['snippet']['description'],
                        channel_status = response['items'][0]['status']['privacyStatus'],
                        playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
                        )
            all_data.append(data)
            return all_data

        channel_temp = get_channel_details(youtube,channel_id)
        channel = pd.DataFrame(channel_temp)

        st.dataframe(channel)
        

        def insert_data_to_mysql(table_name, host='127.0.0.1', user='root', passwd='Shashi@007', database='youtube_data'):
            columns = ",".join(f"{i} Text" for i in channel.columns)
            my_connection = pymysql.connect(host='127.0.0.1',user = 'root',passwd="Shashi@007",database='youtube_data')
            temp = ','.join(['%s']*len(channel.columns))
            query = f"Insert into youtube_data.channel values ({temp}) "
            for i in range(len(channel)):
                my_connection.cursor().execute(query,tuple(channel.iloc[i]))
                my_connection.commit()
            
        st.button('Feed to databse',on_click=insert_data_to_mysql(channel, host='127.0.0.1', user='root', passwd='Shashi@007', database='youtube_data'))
        


        playlist_id = channel['playlist_id'][0]

        def playlist_ext(youtube,playlist_id):
                request = youtube.playlists().list(
                    part='snippet,status,contentDetails,id,localizations,player',
                    id=playlist_id
                )
                response = request.execute()
                playlist_info = []
                for item in response['items']:
                    snippet = item.get('snippet', {})
                    content_details = item.get('contentDetails', {})
                    status = item.get('status', {})
                    playlist_id = item.get('id')
                    channel_id = snippet.get('channelId')
                    playlist_title = snippet.get('title')
                    playlist_description = snippet.get('description')
                    published_at = snippet.get('publishedAt')
                    privacy_status = status.get('privacyStatus')
                    video_count = content_details.get('itemCount')
                    playlist_info.append({
                        'playlist_id': playlist_id,
                        'channel_id': channel_id,
                        'playlist_title': playlist_title,
                        'playlist_description': playlist_description,
                        'published_at': published_at,
                        'privacy_status': privacy_status,
                        'video_count': video_count
                        })
                return pd.DataFrame(playlist_info)

        playlist = playlist_ext(youtube,playlist_id)

        st.subheader("Playlist Table")
        st.dataframe(playlist)
        

        def insert_playlist_to_mysql(table_name, host='127.0.0.1', user='root', passwd='Shashi@007', database='youtube_data'):
            columns = ",".join(f"{i} Text" for i in playlist.columns)
            my_connection = pymysql.connect(host='127.0.0.1',user = 'root',passwd="Shashi@007",database='youtube_data')
            temp = ','.join(['%s']*len(playlist.columns))
            query = f"Insert into youtube_data.playlist values ({temp}) "
            for i in range(len(playlist)):
                my_connection.cursor().execute(query,tuple(playlist.iloc[i]))
                my_connection.commit()
        st.button('Feed playlist to database',on_click=insert_playlist_to_mysql(playlist, host='127.0.0.1', user='root', passwd='Shashi@007', database='youtube_data'))
            
        #video ids func
        def get_video_id_func(youtube,playlist_id):
            
            request = youtube.playlistItems().list(
                        part='contentDetails,snippet,status,id',
                        playlistId = playlist_id,
                        maxResults = 50)
            response = request.execute()
            
            video_ids = []
            
            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])
            
            next_page_token = response.get('nextPageToken')
            more_pages = True
            
            while more_pages:
                if next_page_token is None:
                    more_pages = False
                else:
                    request = youtube.playlistItems().list(
                                part='contentDetails',
                                playlistId = playlist_id,
                                maxResults = 50,
                                pageToken = next_page_token)
                    response = request.execute()
            
                    for i in range(len(response['items'])):
                        video_ids.append(response['items'][i]['contentDetails']['videoId'])
                
                    next_page_token = response.get('nextPageToken')
                    
            return video_ids

        video_ids = get_video_id_func(youtube,playlist_id)

        def video_details(youtube,video_ids):
            all_video_info = []
            
            for  i in range(0,len(video_ids),50):
                request = youtube.videos().list(
                    part = 'snippet,contentDetails,statistics',
                    id = ",".join(video_ids[i:i+50])
                )
                response  = request.execute()
            
                for video in response['items']:
                    stats_to_keep = {'snippet':['channelId','channelTitle','title','description','publishedAt'],
                                    'statistics':['viewCount','likeCount','favouriteCount','commentCount'],
                                    'contentDetails':['duration','definition','caption']
                                    }
                    video_info = {}
                    video_info['video']=video['id']

                    for k in stats_to_keep.keys():
                        for v in stats_to_keep[k]:
                            try:
                                video_info[v] = video[k][v]
                            except:
                                video_info[v] = None

                    all_video_info.append(video_info)

            return pd.DataFrame(all_video_info)


        video = video_details(youtube,video_ids)

        st.subheader("Video details Table")
        st.dataframe(video)
        

        def insert_video_to_mysql(table_name, host='127.0.0.1', user='root', passwd='Shashi@007', database='youtube_data'):
            my_connection = pymysql.connect(host='127.0.0.1',user = 'root',passwd="Shashi@007",database='youtube_data')
            columns = ",".join(f"{i} Text" for i in video.columns)
            temp = ','.join(['%s']*len(video.columns))
            query = f"Insert into youtube_data.video values ({temp}) "
            for i in range(len(video)):
                my_connection.cursor().execute(query,tuple(video.iloc[i]))
                my_connection.commit()

        st.button("Feed Video Details",on_click= insert_video_to_mysql(video, host='127.0.0.1', user='root', passwd='Shashi@007', database='youtube_data'))
        st.spinner(text="In progress...")

        video_to_play = f"https://www.youtube.com/watch?v={video_ids[0]}"
        st.subheader("Recent Video")
        st.video(video_to_play, format="video/mp4", start_time=10)
        st.snow()
        st.success('successfully loaded!', icon="✅")


elif a == 'FAQ':
    st.title("FAQ")

a = st.selectbox(label="Frequently asked question",options=['What are the names of all the videos and their corresponding channels?',
                                      'Which channels have the most number of videos, and how many videos do they have?',
                                      'What are the top 10 most viewed videos and their respective channels?',
                                      'How many comments were made on each video, and what are their corresponding video names?',
                                      'Which videos have the highest number of likes, and what are their corresponding channel names?',
                                      'What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                      'What is the total number of views for each channel, and what are their corresponding channel names?',
                                      'What are the names of all the channels that have published videos in the year 2022?',
                                      'What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                      'Which videos have the highest number of comments, and what are their corresponding channel names?'])

#Basic Details
api_key = "AIzaSyDkW3NmT3ccQqMTaP0k1hrqUl1CYF6eiRs"
channel_id = a
youtube = build('youtube','v3',developerKey = api_key)
my_connection = pymysql.connect(host='127.0.0.1',user = 'root',passwd="Shashi@007",database='youtube_data')


if a == 'What are the names of all the videos and their corresponding channels?':
    #sql queries 
    #What are the names of all the videos and their corresponding channels?
    channel_name = '''select video.title,channel.channel_name from youtube_data.channel inner join youtube_data.video on video.channelId = channel.channel_id'''
    names_of_channel = pd.read_sql_query(channel_name,my_connection)
    df_name_of_channels = pd.DataFrame(names_of_channel)
    st.dataframe(df_name_of_channels)
    

elif a == 'Which channels have the most number of videos, and how many videos do they have?':
    #Which channels have the most number of videos, and how many videos do they have?
    max_count = '''SELECT
        a.channel_name,
        MAX(a.video_count) AS max_video_count
    FROM
        (SELECT
            channel.channel_name,
            COUNT(video.title) AS video_count
        FROM
            youtube_data.channel
        INNER JOIN
            youtube_data.video ON video.channelId = channel.channel_id
        GROUP BY
            1) AS a
    GROUP BY
        1
    ORDER BY
        2 DESC
    LIMIT 1;'''

    most_number_of_video = pd.read_sql_query(max_count,my_connection)
    df_most_num_video = pd.DataFrame(most_number_of_video)
    st.dataframe(df_most_num_video)
    

elif a == "What are the top 10 most viewed videos and their respective channels?":

    top10videos = '''select title,channelTitle,max(viewCount) as Count from youtube_data.video
    group by 1,2 order by 3 desc LIMIT 10 ;'''

    top = pd.read_sql_query(top10videos,my_connection)
    st.dataframe(top)
    st.bar_chart(top.set_index('title')['Count'])


elif a == 'Which videos have the highest number of likes, and what are their corresponding channel names?':
    like_count = '''select title,channelTitle,max(likeCount) as Count from youtube_data.video group by 1,2 order by 3 desc LIMIT 1;'''
    like = pd.read_sql_query(like_count,my_connection)
    df_like = pd.DataFrame(like)
    st.dataframe(df_like)

elif a == 'What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    total_like = '''select title,channelTitle,sum(likeCount) as Count from youtube_data.video group by 1,2 order by 3 desc ;'''
    likes = pd.read_sql_query(total_like,my_connection)
    df_likes = pd.DataFrame(likes)
    st.dataframe(df_likes)

elif a == 'What is the total number of views for each channel, and what are their corresponding channel names?':
    total_view = '''select channelTitle,concat(sum(viewCount)/1000," ","K") as Count from youtube_data.video
    group by 1 order by 2 desc ;'''
    total_views = pd.read_sql_query(total_view,my_connection)
    df_total_views = pd.DataFrame(total_views)
    st.dataframe(df_total_views)

elif a == 'What are the names of all the channels that have published videos in the year 2022?':

    publishedYear = '''select channelTitle from (select channelTitle,count(year_) from(select channelTitle, year(publishedAt) as year_ from youtube_data.video having year_ =2022) as a group by 1 having count(year_) >1) as a;'''
    published = pd.read_sql_query(publishedYear,my_connection)
    df_published = pd.DataFrame(published)
    st.dataframe(df_published)

elif a == 'Which videos have the highest number of comments, and what are their corresponding channel names?':

    comment = '''select channelTitle,max(commentCount) as max_comment from youtube_data.video group by 1 order by 2 desc limit 1;'''
    comment = pd.read_sql_query(comment,my_connection)
    df_comment = pd.DataFrame(comment)
    st.dataframe(df_comment)

elif a == 'How many comments were made on each video, and what are their corresponding video names?':
    comments = '''select title,sum(commentCount) as comment_count from youtube_data.video group by 1 ;'''
    a = pd.read_sql_query(comments,my_connection)
    df_comments_ = pd.DataFrame(a)
    st.dataframe(df_comments_)
else:
    print("Sorry we're working on it")

st.sidebar.markdown("---")
st.sidebar.subheader("About")
st.sidebar.info("This app is a YouTube Analytics Dashboard created with Streamlit.")


