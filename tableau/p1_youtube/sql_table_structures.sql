

create table videos as (
    SID int autoincrement primary key
    , Video_Id varchar(64)
    , Trending_Date varchar(10)
    , Title varchar(255)
    , Channel_title varchar(64)
    , Category_Id int
    , Publish_Time datetime2
    , Tags varchar(500)
    , Views int
    , Likes int
    , Dislikes int
    , Comment_Count int
    , Thumbnail_Link varchar(255)
    , Comments_Disabled bit
    , VIdeo_Error_Or_Removed bit
    , Description varchar(1000)
    , File_Paths varchar(12)
)

create table categories as (
    SID int autoincrement primary key
    , CountryCode varchar(2)
    , index int
    , id int
    , Category varchar(200)


)