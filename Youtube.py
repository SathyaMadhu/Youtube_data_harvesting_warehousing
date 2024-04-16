from googleapiclient.discovery import build
import mysql.connector as sql
import pandas as pd
from datetime import datetime
import streamlit as st

def display_Home():
    st.title("Home")
    st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING USING PYTHON AND SQL]")

    #API KEY Connection
    def Api_connect():
        Api_id ="AIzaSyBUFsZSjBLRJiJ31JZeMCWXuoG8RzTYur0"
        api_service_name = "youtube"
        api_version = "v3"
        youtube = build(api_service_name, api_version, developerKey=Api_id)
        return youtube
    youtube = Api_connect()


    #Retrieve channel Information
    def get_channel_info(Channel_id):
        request=youtube.channels().list(            
                        part="snippet,contentDetails,statistics",
                        id=Channel_id)
        response=request.execute()
        for i in response ['items']:
            Channel_data =dict(Channel_Name=i["snippet"]["title"],
                    Channel_Id=i["id"],
                    Subscribers=i['statistics']['subscriberCount'],
                    Views = i['statistics']['viewCount'],
                    Total_videos = i['statistics']['videoCount'],
                    Channel_Description = i["snippet"]["description"],
                    Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
        return Channel_data

    #Channel_data= get_channel_info("UCG98ruDeyp55THxbpBCIv3g")

    # get video_ids
    def get_video_ids(Channel_id):
        video_ids = []
        response=youtube.channels().list(            
                            id=Channel_id,
                            part="contentDetails").execute()
        Playlist_Id=response['items'][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        next_page_token=None
        while True:
            response1=youtube.playlistItems().list(
                part='snippet',
                playlistId=Playlist_Id,
                maxResults=50,
                pageToken=next_page_token).execute()
            for i in range (len(response1['items'])):
                video_ids.append(response1['items'][i]["snippet"]["resourceId"]["videoId"])
            next_page_token=response1.get("nextPageToken")

            if next_page_token is None:
                break
        return video_ids

    #video_ids = get_video_ids("UCWv7vMbMWH4-V0ZXdmDpPBA")

    def iso8601_to_seconds(duration):
        duration = duration[2:]  # Remove 'PT' prefix
        seconds = 0
        if 'H' in duration:
            hours, duration = duration.split('H')
            seconds += int(hours) * 3600
        if 'M' in duration:
            minutes, duration = duration.split('M')
            seconds += int(minutes) * 60
        if 'S' in duration:
            seconds += int(duration[:-1])
        return seconds

    # get video Information
    def get_video_data(video_ids):
        Video_info=[]
        for video_id in video_ids:
            request=youtube.videos().list(
                part = "snippet,contentDetails,statistics",
                id = video_id
            )
            response = request.execute()
            for item in response["items"]:
                published_date = datetime.strptime(item["snippet"]["publishedAt"], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
                duration_seconds = iso8601_to_seconds(item["contentDetails"]["duration"])
                data = dict(Channel_Name=item["snippet"]["channelTitle"],
                            channel_id=item["snippet"]["channelId"],
                            video_Id=item["id"],
                            Title = item["snippet"]["title"],
                            Thumbnail = item["snippet"]["thumbnails"]["default"]["url"],
                            Description = item.get("description"),
                            Published_Date = published_date,
                            like_count = item['statistics'].get('likeCount',0),
                            dislike_count = item['statistics'].get('dislikeCount',0),
                            Duration=duration_seconds,
                            Views=item["statistics"]["viewCount"],
                            #Comments=item["statistics"]["commentCount"],
                            Comments= item["statistics"].get("commentCount", 0),
                            Favourite_Count=item["statistics"]["favoriteCount"],
                            Definition = item["contentDetails"]["definition"],
                            Caption_Status=item["contentDetails"]["caption"]
                        )
                Video_info.append(data)
        return Video_info

    #Video_info = get_video_data(video_ids)

    # get Comment information
    def get_comment_info(Video_Ids):
        Comment_Data=[]
        try:
            for video_id in Video_Ids:
                request = youtube.commentThreads().list(
                part="snippet",
                videoId = video_id,
                maxResults=50
                )
                response=request.execute()
                for item in response["items"]:
                    Comment_Published = datetime.strptime(item["snippet"]['topLevelComment']["snippet"]['publishedAt'], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
                    data = dict(Comment_Id=item["snippet"]['topLevelComment']["id"],
                                Video_Id=item["snippet"]['topLevelComment']["snippet"]["videoId"],
                                Comment_Text=item["snippet"]['topLevelComment']["snippet"]['textDisplay'],
                                Comment_Author=item["snippet"]['topLevelComment']["snippet"]['authorDisplayName'],
                                #Comment_Published=item["snippet"]['topLevelComment']["snippet"]['publishedAt'],
                                Comment_Published=Comment_Published)
                    Comment_Data.append(data)
        except:
            pass
        return Comment_Data

    #Comment_Data = get_comment_info(video_ids)

    # get playlist details
    def get_Playlist_info(Channel_id):
        next_page_token=None
        Playlist_Data=[]
        while True:
                request=youtube.playlists().list(
                        part = "snippet,contentDetails",
                        channelId=Channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                    )
                response=request.execute()

                for item in response["items"]:
                        PublishedAt = datetime.strptime(item["snippet"]['publishedAt'], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
                        data = dict(Playlist_Id=item["id"],
                                    Title=item["snippet"]["title"],
                                    Channel_Id=item["snippet"]["channelId"],
                                    Channel_Name=item["snippet"]["channelTitle"],
                                    PublishedAt=PublishedAt,
                                    Video_Count=item["contentDetails"]["itemCount"])
                        Playlist_Data.append(data)
                next_page_token=response.get("nextPageToken")
                if next_page_token is None:
                    break
        return Playlist_Data

    #Playlist_Data=get_Playlist_info("UCWv7vMbMWH4-V0ZXdmDpPBA")

    # creating Channel table in sql
    import mysql.connector as sql
    def Channel_Table():
        connection = sql.connect(
                            host="localhost",
                            user="root",
                            password="Nisama@2021",
                            database="youtube_data")
                        
        cursor=connection.cursor()
        create_query = ("""Create table if not exists channels(Channel_Name VARCHAR(100),  
                                                                    Channel_Id VARCHAR(80) PRIMARY KEY,
                                                                    Subscribers VARCHAR(100),
                                                                    Views VARCHAR(100),
                                                                    Total_videos VARCHAR(100),
                                                                    Channel_Description TEXT,
                                                                    Playlist_Id VARCHAR(80))""")
        cursor.execute(create_query)
        connection.commit()

        print(cursor.rowcount, "Table Channel created")

    #Creating Video Table
    import mysql.connector as sql
    def Video_Table():
            connection = sql.connect(
                            host="localhost",
                            user="root",
                            password="Nisama@2021",
                            database="youtube_data")
                    
            cursor=connection.cursor()
            create_query = ("""Create table if not exists Videos(Channel_Name VARCHAR(100),
                                                    Channel_Id VARCHAR(80),
                                                    Video_Id VARCHAR(100) PRIMARY KEY,
                                                    Title VARCHAR(100),
                                                    Thumbnail TEXT,                            
                                                    Description TEXT,
                                                    Published_Date TIMESTAMP,
                                                    Like_count BIGINT,
                                                    DisLike_count BIGINT,
                                                    Duration BIGINT,                            
                                                    Views BIGINT,
                                                    Comments VARCHAR(50),                                
                                                    Favourite_Count INT,
                                                    Definition VARCHAR(80),
                                                    Caption_Status VARCHAR(80))""")   
            cursor.execute(create_query)
            connection.commit()
            cursor.close()
            connection.close()
            print(cursor.rowcount, "Video Table created")


    #Table Creation for Comment
    def Comment_Table():
        connection = sql.connect(
                                host="localhost",
                                user="root",
                                password="Nisama@2021",
                                database="youtube_data")
                        
        cursor=connection.cursor()
        cursor.execute("""Create table if not exists Comments(Comment_Id VARCHAR(100) PRIMARY KEY, 
                                                                Video_Id VARCHAR(80),
                                                                Comment_Text TEXT,
                                                                Comment_Author VARCHAR(255),
                                                                Comment_Published TIMESTAMP)""")
        connection.commit()
        cursor.close()
        connection.close()
        print(cursor.rowcount, "Comment Table created ")


    #Table creation for Playlist Data
    import mysql.connector as sql
    def Playlist_Table():
        connection = sql.connect(
                    host="localhost",
                    user="root",
                    password="Nisama@2021",
                    database="youtube_data"
                )

        cursor=connection.cursor()
        create_query = """Create table if not exists Playlists(Playlist_Id VARCHAR(100) PRIMARY KEY,
                                                                    Title VARCHAR(100),
                                                                    Channel_Id VARCHAR(100),
                                                                    Channel_Name VARCHAR(100),
                                                                    PublishedAt TIMESTAMP,
                                                                    Video_Count INT)"""
        cursor.execute(create_query)
        connection.commit()
        cursor.close()
        connection.close()

        print(cursor.rowcount, "Playlist Table Created ")


    def Tables():
        Channel_Table()
        Video_Table()
        Playlist_Table()
        Comment_Table()
        return "Tables are Created"


    def youtube_data_extract(Channel_id):
        channel_data = get_channel_info(Channel_id)
        video_ids = get_video_ids(Channel_id)
        video_info = get_video_data(video_ids)
        comment_data = get_comment_info(video_ids)
        playlist_data = get_Playlist_info(Channel_id)
        return channel_data, video_info, comment_data, playlist_data


    #code to identify the channel id is already exist or not
    import mysql.connector as sql
    def channel_exists(channel_id):
        connection = sql.connect(
            host="localhost",
            user="root",
            password="Nisama@2021",
            database="youtube_data"
        )
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM channels WHERE Channel_Id = %s", (channel_id,))
        result = cursor.fetchone()
        cursor.close()
        connection.close()
        return result is not None

    #Insert Channel Details
    import mysql.connector as sql
    def Insert_Channel_Values(channel_data):
        try:
            connection = sql.connect(
                                    host="localhost",
                                    user="root",
                                    password="Nisama@2021",
                                    database="youtube_data")
                                
            cursor=connection.cursor()
            sql_insert = "INSERT INTO channels (Channel_Name, Channel_Id, Subscribers, Views, Total_videos, Channel_Description, Playlist_Id) VALUES (%s,%s,%s,%s,%s,%s,%s)"
            val = tuple(channel_data.values())
            cursor.execute(sql_insert, val)
            connection.commit()
            print(cursor.rowcount, "Channel Values inserted.")
        except Exception as e:
            print("Error inserting channel data:", e)
        finally:
            cursor.close()
            connection.close()

    #Insert Video Details
    import mysql.connector as sql
    def Insert_Video_data(Video_info):
        connection = sql.connect(
                        host="localhost",
                        user="root",
                        password="Nisama@2021",
                        database="youtube_data")
                    
        cursor=connection.cursor()
        Video = []
        for i in Video_info:
                    Video.append(tuple(i.values()))
        sql_vi = "INSERT INTO Videos(Channel_Name, Channel_Id, Video_Id, Title, Thumbnail, Description, Published_Date, Like_count, DisLike_count, Duration, Views, Comments, Favourite_Count, Definition,Caption_Status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.executemany(sql_vi,Video)
        print("Inserting Video data:", Video)
        connection.commit()
        cursor.close()
        connection.close()
        print(cursor.rowcount, "Video record inserted.")


    #Insert comment Details
    import mysql.connector as sql
    def Insert_Comment_data(Comment_Data):
            connection = sql.connect(
                                    host="localhost",
                                    user="root",
                                    password="Nisama@2021",
                                    database="youtube_data")
                            
            cursor=connection.cursor()

            Comment = []
            for i in Comment_Data:
                    Comment.append(tuple(i.values()))
                    cursor=connection.cursor()
            sql_co = "INSERT INTO Comments(Comment_Id, Video_Id,Comment_Text, Comment_Author,Comment_Published) VALUES (%s,%s,%s,%s,%s)"
            values = Comment
            for i in Comment:
                    cursor.execute(sql_co,i)
                    print("Inserting comment data:", i)
                    connection.commit()
            cursor.close()
            connection.close()

            print(cursor.rowcount, "Comments record inserted.")


    #Insert Playlist values
    import mysql.connector as sql
    def Insert_Playlist_Values(Playlist_Data):
        connection = sql.connect(
                    host="localhost",
                    user="root",
                    password="Nisama@2021",
                    database="youtube_data"
                )

        cursor=connection.cursor()

        Playlist = []
        for i in Playlist_Data:
                Playlist.append(tuple(i.values()))

        sql_pl = "INSERT INTO Playlists(Playlist_Id, Title, Channel_Id, Channel_Name, PublishedAt, Video_Count) VALUES (%s,%s,%s,%s,%s,%s)"
        values = Playlist
        for i in Playlist:
            cursor.execute(sql_pl,i)
            print("Inserting playlist data:", i)
            connection.commit()
        cursor.close()
        connection.close()

        print(cursor.rowcount, "Playlist  record inserted.")

    def insert_data_SQL(Channel_data, Video_info, Comment_Data, Playlist_Data):
         Insert_Channel_Values(Channel_data)
         Insert_Video_data(Video_info)
         Insert_Comment_data(Comment_Data)
         Insert_Playlist_Values(Playlist_Data)
         return "Channel, videos, Playlist and comments data Stored in sql"

    #Streamlit Portion
    #Main  content
    Channel_id=st.text_input("ENTER YOUR CHANNEL ID")
    if st.button("Retrieve Channel Data and Stored in SQL"):
        if Channel_id:
            if channel_exists(Channel_id):
                    st.warning('Channel ID already exists in the database!')
            else:
                    channel_data, video_info, comment_data, playlist_data = youtube_data_extract(Channel_id)
                    #channel_data = get_channel_info(Channel_id)
                    st.write("Channel Data:", channel_data)
                    #st.write("Video Info:", video_info)
                    #st.write("Comment Data:", comment_data)
                    #st.write("Playlist Data:", playlist_data)
                    Insert_Channel_Values(channel_data)
                    Insert_Video_data(video_info)
                    Insert_Comment_data(comment_data)
                    Insert_Playlist_Values(playlist_data)
                    st.success('Channel data retrieved and stored successfully!')
        else:
                st.warning('Please enter a valid Channel ID!')

    #if st.button ("Store Data in SQL Tables"):
            #if 'Channel_Data' in locals():
             #   Insert_Channel_Values(channel_data)
              #  st.success('Channel data Stored in SQL Tables successfully!')
            #else:
                # Handle the case when channel_data is not defined
             #   st.error('Channel data is not available.')
            
#Display tables
def display_Table(table_name):
    connection = sql.connect(
            host="localhost",
            user="root",
            password="Nisama@2021",
            database="youtube_data"
        )   
    cursor = connection.cursor()
    Query = f"SELECT * FROM {table_name}"
    cursor.execute(Query)
    data = cursor.fetchall()
    columns = [i[0] for i in cursor.description]
    df = pd.DataFrame(data, columns=columns)
    connection.close()
    return df
def display_tables():
    st.title("Display Tables")
    tables = ["CHANNELS", "PLAYLISTS", "VIDEOS","COMMENTS"]
    Show_Table=st.radio("SELECT TABLE FOR VIEW",tables)
    st.write(f"Displaying table: {Show_Table}")
    df = display_Table(Show_Table)
    st.write(df)
def execute_sql_queries():
    st.title("Execute SQL Queries")

    #SQL CONNECTION
    import mysql.connector as sql
    connection = sql.connect(
                            host="localhost",
                            user="root",
                            password="Nisama@2021",
                            database="youtube_data")
                    
    cursor=connection.cursor()
    Queries = st.selectbox("Select Your Question",("Names of all the videos and their corresponding channels",
                                                    "channels with most number of videos and their count",
                                                    "Top 10 most viewed videos and their respective channels",
                                                    "Number of comments were made on each video and their corresponding video names",
                                                    "Videos with the highest number of likes and their corresponding channel names",
                                                    "Total number of likes and dislikes for each video and their corresponding video names",
                                                    "Total number of views for each channel and their corresponding channel names",
                                                    "Channels that have published videos in the year 2022",
                                                    "Average duration of all videos in each channel and their corresponding channel names",
                                                    "videos with the highest number of comments and their corresponding channel names"))


    if Queries=="Names of all the videos and their corresponding channels":

            Q1="""SELECT Title as VideoTitle, Channel_Name from videos"""
            cursor.execute(Q1)
            data1=cursor.fetchall()
            connection.commit()
            df1 = (pd.DataFrame(data1,columns=["Video_Title","Channel_Name"]))
            st.write(df1)

    elif Queries=="channels with most number of videos and their count":

            Q2="""SELECT Channel_Name,Total_videos from channels order by Total_videos desc;"""
            cursor.execute(Q2)
            data2=cursor.fetchall()
            connection.commit()
            df2=pd.DataFrame(data2,columns=["Channel_Name","Total_videos"])
            st.write(df2)

    elif Queries=="Top 10 most viewed videos and their respective channels":

            Q3="""SELECT Channel_Name,Title,Views as viewcount FROM videos ORDER BY ViewCount DESC LIMIT 10;"""
            cursor.execute(Q3)
            data3=cursor.fetchall()
            connection.commit()
            df3=pd.DataFrame(data3,columns=["Channel_Name","Title","ViewCount"])
            st.write(df3)

    elif Queries=="Number of comments were made on each video and their corresponding video names":

            Q4="""select Title as VideoTitle, comments from videos order by comments desc"""
            cursor.execute(Q4)
            data4=cursor.fetchall()
            connection.commit()
            df4=pd.DataFrame(data4,columns=["VideoTitle","comments"])
            st.write(df4)

    elif Queries=="Videos with the highest number of likes and their corresponding channel names":

            Q5="""select Channel_Name, Title, MAX(like_count) as max_likes from videos GROUP BY Channel_Name, Title ORDER BY max_likes DESC"""
            cursor.execute(Q5)
            data5=cursor.fetchall()
            connection.commit()
            df5=pd.DataFrame(data5,columns=["Channel_Name","Title","max_likes"])
            st.write(df5)
            
    elif Queries== "Total number of likes and dislikes for each video and their corresponding video names":

            Q6="""select Title, like_count as Likes, dislike_count as Dislikes from videos"""
            cursor.execute(Q6)
            data6=cursor.fetchall()
            connection.commit()
            df6=pd.DataFrame(data6,columns=["Title","Likes","Dislikes"])
            st.write(df6)

    elif Queries== "Total number of views for each channel and their corresponding channel names":

            Q7="""select Channel_Name, Views as Channel_views from Channels"""
            cursor.execute(Q7)
            data7=cursor.fetchall()
            connection.commit()
            df7=pd.DataFrame(data7,columns=["Channel_Name","Channel_views"])
            st.write(df7)

    elif Queries== "Channels that have published videos in the year 2022":

            Q8="""select Channel_Name, Title, Published_Date from Videos where year(Published_Date) = 2022"""
            cursor.execute(Q8)
            data8=cursor.fetchall()
            connection.commit()
            df8=pd.DataFrame(data8,columns=["Channel_Name","Title", "Published_Date"])
            st.write(df8)

    elif Queries== "Average duration of all videos in each channel and their corresponding channel names":

            Q9="""select Channel_Name, AVG(Duration) as Average_Duration_in_secs from videos group by Channel_Name"""
            cursor.execute(Q9)
            data9=cursor.fetchall()
            connection.commit()
            if data9:
                df9 = pd.DataFrame(data9, columns=["Channel_Name", "avg_duration"])
                st.dataframe(df9)
            else:
                st.warning("No data found for average duration.")
            
    elif Queries== "videos with the highest number of comments and their corresponding channel names":

            Q10="""SELECT Channel_Name, Title, Comments from videos order by Comments DESC;"""
            cursor.execute(Q10)
            data10=cursor.fetchall()
            connection.commit()
            df10=pd.DataFrame(data10,columns=["Channel_Name","Title","comments"])
            st.write(df10)

def main():
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["Home", "Display Tables", "Execute SQL Queries"])

    if page == "Home":
        display_Home()
    elif page == "Display Tables":
        display_tables()
    elif page == "Execute SQL Queries":
        execute_sql_queries()

if __name__ == "__main__":
    main()
