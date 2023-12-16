# you-tube-data-harvesting-project-
from googleapiclient.discovery import build
import pymongo
import pandas as pd
import streamlit as st
import psycopg2


def Api_connect():   
    

    api_key="AIzaSyAE5cknasCqnBc2X1zqazrjtsqZ2NkgiJE"
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=api_key)
    
    return youtube

youtube=Api_connect()
    
    
def get_channel_info(channel_id):
    request = youtube.channels().list(
                        part="snippet,contentDetails,statistics",
                        id=channel_id
    )
    response = request.execute()

    for i in response['items']:
        data = dict(channel_Name=i["snippet"]["title"],
                    channel_id=i['id'],
                    subscribers= i['statistics']['subscriberCount'],
                    Views=i['statistics']['viewCount'],  # Corrected 'videoCount' to 'viewCount'
                    Total_videos=i['statistics']['videoCount'],
                    channel_Description=i['snippet']['description'],
                    playlist_id=i['contentDetails']['relatedPlaylists']['uploads'])
        

    return data



def get_video_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                        part='snippet',
                                        playlistId=Playlist_Id,
                                        maxResults=50,
                                        pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response.get('nextpagetoken')
        
        if next_page_token is None:
            break
    return video_ids  


def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
    )
        response=request.execute()
        
        for item in response["items"]:
            data=dict(channel_name=item['snippet']['channelTitle'],
                channel_id=item['snippet']['channelId'],
                video_id=item['id'],
                Title=item['snippet']['title'],
                tags=item['snippet'].get('tags'),
                thumbnail=item['snippet']['thumbnails']['default']['url'],
                description=item['snippet']['description'],
                published_date=item['snippet']['publishedAt'],
                duration=item['contentDetails']['duration'],
                vews=item['statistics']['viewCount'],
                Likes=item['statistics'].get('likeCount'),      
                comments=item['statistics']['commentCount'],
                favorite_count=item['statistics']['favoriteCount'],
                definition=item['contentDetails']['definition'],
                caption_status=item['contentDetails']['caption']
                ) 
            video_data.append(data)
    return video_data    

def get_comment_info(video_ids):
    comment_data=[]
    try:
            for video_id in video_ids:
                request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()

            for item in response["items"]:
                data = dict(comment_id=item['snippet']['topLevelComment']['id'],
                            video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                            comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                comment_data.append(data)
    except:
        pass
    return comment_data


def get_playlist_details(channel_id):
        next_page_token=None
        all_data=[]
        while True:

                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId="UC-lHJZR3Gqxm24_Vd_AJ5Yw",
                        maxResults=50,
                        pageToken=next_page_token
)
                response=request.execute()

                for item in response['items']:
                        data=dict(playlist_id=item['id'],
                                title=item['snippet']['title'],
                                channel_Id=item['snippet']['channelId'],
                                channel_Name_Id=item['snippet']['channelTitle'],
                                PublishedAt=item['snippet']['publishedAt'],
                                video_count=item['contentDetails']['itemCount'])
                        all_data.append(data)
                
                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return all_data   
    
client = pymongo.MongoClient('mongodb://localhost:27017')
db=client["youtube_data"]

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_details=get_video_info(vi_ids)   
    com_details=get_comment_info(vi_ids)
    
    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                    "video_information":vi_details,"comment_information":com_details})
    
    
    return"upload completed successfully"

def channels_table():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Gayathri@152233",
        database="youtube_data",
        port="5432"
    )

    cursor = mydb.cursor()

    drop_query = '''DROP TABLE IF EXISTS channels'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = ''' CREATE TABLE IF NOT EXISTS channels (channel_Name varchar(100),
                                                                channel_id varchar(80) PRIMARY KEY,
                                                                subscribers bigint,
                                                                Views bigint,
                                                                Total_videos int,
                                                                channel_Description text,
                                                                playlist_id varchar(80))'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("channels table already created")

    ch_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    df = pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        insert_query = '''INSERT INTO channels (
            channel_Name,
            channel_id,
            subscribers,
            Views,
            Total_videos,
            channel_Description,
            playlist_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)'''

        values = (
            row['channel_Name'],
            row['channel_id'],
            row['subscribers'],
            row['Views'],
            row['Total_videos'],
            row['channel_Description'],
            row['playlist_id']
        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except:
            print("Channel values are already inserted")
            
def playlist_table():
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="Gayathri@152233",
                            database="youtube_data",
                            port="5432")
    cursor = mydb.cursor()

    drop_query = '''DROP TABLE IF EXISTS playlists'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query = ''' CREATE TABLE IF NOT EXISTS playlists(playlist_id varchar(100) primary key,
                                                            title varchar(100),
                                                            channel_Id varchar(100),
                                                            channel_Name_id varchar (100),
                                                            PublishedAt timestamp,
                                                            video_count int)''' 
    cursor.execute(create_query)
    mydb.commit()
    pl_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for pl_data in coll1.find({},{"_id": 0, "playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=pd.DataFrame(pl_list) 
    for index, row in df1.iterrows():
        insert_query = '''INSERT INTO playlists(playlist_id,
                                                title,
                                                channel_Id,
                                                channel_Name_Id,
                                                PublishedAt,
                                                video_count
                                                ) 
                                                VALUES (%s, %s, %s, %s, %s, %s)'''

    values = (
        row['playlist_id'],
        row['title'],
        row['channel_Id'],
        row['channel_Name_Id'],
        row['PublishedAt'],
        row['video_count']
    )

    try:
        cursor.execute(insert_query, values)
        mydb.commit()
    except:
        print("Channel values are already inserted")

def Videos_table():   
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Gayathri@152233",
        database="youtube_data",
        port="5432"
    )
    cursor = mydb.cursor()

    drop_query = '''DROP TABLE IF EXISTS videos'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''CREATE TABLE IF NOT EXISTS videos(channel_name varchar(100),
                                                        channel_id varchar(100),
                                                        video_id varchar(30) primary key,
                                                        Title varchar(150),
                                                        tags text,
                                                        thumbnail varchar(200),
                                                        description text,
                                                        published_date timestamp,
                                                        duration interval,
                                                        vews bigint,
                                                        Likes bigint,
                                                        comments int,
                                                        favorite_count int,
                                                        definition varchar(10),
                                                        caption_status varchar(50)
                                                    )'''
    cursor.execute(create_query)
    mydb.commit()

    vi_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])

    df2 = pd.DataFrame(vi_list)

    for index, row in df2.iterrows():
        insert_query = '''INSERT INTO videos(channel_name,
                                            channel_id,
                                            video_id,
                                            Title,
                                            tags,
                                            thumbnail,
                                            description,
                                            published_date,
                                            duration,
                                            vews,
                                            Likes,
                                            comments,
                                            favorite_count,
                                            definition,
                                            caption_status
                                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

        values = (
            row['channel_name'],
            row['channel_id'],
            row['video_id'],
            row['Title'],
            row['tags'],
            row['thumbnail'],
            row['description'],
            row['published_date'],
            row['duration'],
            row['vews'],
            row['Likes'],
            row['comments'],
            row['favorite_count'],
            row['definition'],
            row['caption_status']
        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except:
            print("Video values are already inserted")

def comments_table():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Gayathri@152233",
        database="youtube_data",
        port="5432"
    )
    cursor = mydb.cursor()

    drop_query = '''DROP TABLE IF EXISTS comments'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''CREATE TABLE IF NOT EXISTS comments(
                        comment_id varchar(100) PRIMARY KEY,
                        video_id varchar(50),
                        comment_Text text,
                        comment_Author varchar(150),
                        comment_Published timestamp
                    )'''
    cursor.execute(create_query)
    mydb.commit()

    com_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])

    df3 = pd.DataFrame(com_list)

    for index, row in df3.iterrows():
        insert_query = '''INSERT INTO comments(
                            comment_id,
                            video_id,
                            comment_Text,
                            comment_Author,
                            comment_Published
                        ) 
                        VALUES (%s, %s, %s, %s, %s)'''

        values = (
            row['comment_id'],
            row['video_id'],
            row['comment_Text'],
            row['comment_Author'],
            row['comment_Published'])
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print ("channel values are alredy inserted")
def tables():   
    channels_table()
    playlist_table()
    Videos_table()
    comments_table()
    
    return "tables created successfully" 


def show_channels_table():
    ch_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    df = st.dataframe(ch_list)
    return df

def show_playlists_table():  
    pl_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for pl_data in coll1.find({},{"_id": 0, "playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=st.dataframe(pl_list) 
    return df1

def show_videos_table():     
    vi_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = st.dataframe(vi_list)
    return df2   

def show_comments_table(): 
    com_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])

    df3 = st.dataframe(com_list)
    return df3

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management Using MongoDB and SQL")
channel_id = st.text_input("Enter the channel ID")
if st.button("collect and store data"):
    ch_ids = []
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_ids.append(ch_data["channel_information"]["channel_id"])
        
    if channel_id in ch_ids:
        st.success('channel details of the given channels id already exists')
        
    else:
        insert = channel_details(channel_id)
        st.success(insert)
    if st.button("Migrate to sql"):
        Table = tables()
        st.success(Table)
    show_table = st.radio("SELECT THE TABLE FOR VIEW", ("CHANNELS", "PLAYLIST", "VIDEOS", "COMMENTS"))
    if show_table == "CHANNELS":    
        show_channels_table()
        
    elif show_table == "PLAYLIST":
        show_playlists_table()
        
    elif show_table == "VIDEOS":
        show_videos_table()
        
    elif show_table == "COMMENTS":
        show_comments_table()


mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Gayathri@152233",
        database="youtube_data",
        port="5432"
    )
cursor = mydb.cursor()
question=st.selectbox("Select your question",("1. All the videos and the channel name"
                                                "2. channels with most number of videos",
                                                "3. 10 most viewed videos",
                                                "4. comments in each videos",
                                                "5. Videos with higest likes",
                                                "6. likes of all videos",
                                                "7. views of each channel",
                                                "8. videos published in the year of 2022",
                                                "9. average duration of all videos in each channel",
                                                "10. videos with highest number of comments"))

if question=="1. All the videos and the channel name":
    query1='''swlect title as video,channel_name as channelname from video'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title ","channel name"])
    st.write(df)
    
elif question=="2.channels with most number of videos":
    query2='''select channel_name as channelname,total_videos as no_vedios from channels
                order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","no of videos"])
    st.write(df2)    
    
elif question=="3.10 most viewed videos":
    query3 = '''SELECT vews AS views, channel_name AS channelname, Title AS videotitle FROM videos WHERE vews IS NOT NULL
                ORDER BY views DESC LIMIT 10'''
    cursor.execute(query3)
    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3, columns=["views", "channel name", "videotitle"])    
    st.write(df3) 
    
elif question=="4. comments in each videos":
    query4 = '''SELECT comments as no_comments, Title from videos where comments is not null'''
    cursor.execute(query4)
    t4 = cursor.fetchall()
    df4 = pd.DataFrame(t4, columns=["no of comments","videotitle"])    
    st.write(df4) 
    
elif question=="5. Videos with higest likes":
    query5 = '''SELECT title AS videotitle, channel_name AS channelname, Likes AS likecount
                    FROM videos
                    WHERE Likes IS NOT NULL
                    ORDER BY likecount DESC'''
    cursor.execute(query5)
    t5 = cursor.fetchall()
    df5 = pd.DataFrame(t5, columns=["videotitle", "channelname", "likecount"])    
    st.write(df5)

elif question=="6. likes of all videos":
    query6 = '''select likes as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    t6 = cursor.fetchall()
    df6 = pd.DataFrame(t6, columns=["likecount", "videotitle"])
    st.write(df6)
    
elif question=="7. views of each channel":
    query7 = '''select channel_name as channelname ,views as totalviews from channels'''
    cursor.execute(query7)
    t7 = cursor.fetchall()
    df7 = pd.DataFrame(t7, columns=["likecount", "videotitle"])   
    st.write(df7)

elif question=="8. videos published in the year of 2022":
    query8 = '''select title as video_title,published_date as videorelease,channel_name as channelname from videos
            where extract(year from published_date)=2022'''
    cursor.execute(query8)
    t8 = cursor.fetchall()
    df8 = pd.DataFrame(t8, columns=["videotitle","published_data","channelname"])
    st.write(df8)
    
elif question=="9. average duration of all videos in each channel":
    query9 = '''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    t9 = cursor.fetchall()
    df9 = pd.DataFrame(t9, columns=["channelname","averageduration"])
    st.write(df9)
    T9 = []
    for index, row in df9.iterrows():
        channel_title = row["channelname"]
        average_duration = row["averageduration"]
        average_duration_str = str(average_duration)
        T9.append(dict(channeltitle=channel_title, avgduration=average_duration_str))
    df1 = pd.DataFrame(T9)  
    st.write(df1)
    
elif question=="10. videos with highest number of comments":
    query10 = '''select title as videotitle, channel_name as channelname,comments as comments from videos where 
                comments is not null order by comments desc'''
    cursor.execute(query10)
    t10 = cursor.fetchall()
    df10 = pd.DataFrame(t10, columns=["video title","channel name","comments"]) 
    st.write(df10)   

