CREATE TABLE [HIDDEN_username].user_g_data
(

    `Full Name` Nullable(String),

    `Nickname` Nullable(String),

    `Gender` Nullable(String),

    `Birthday` Nullable(String),

    `Age` Nullable(String),

    `Region` Nullable(String),

    `Email Address` Nullable(String),

    `Phone Number` Nullable(String),

    `Most Played Game` Nullable(String),

    `N. Games Owned` Nullable(Int64),

    `N. in Wishlist` Nullable(Int64),

    `N. Games Refunded` Nullable(Int64),

    `Total Playtime` Nullable(Float32),
    
    `DM Creation` Nullable(DateTime)
)
ENGINE = PostgreSQL('172.30.58.112:5432',
 '[HIDDEN_username]_db',
 'user_g_data',
 '[HIDDEN_username]',
 '[HIDDEN]',
 'project_dm');


CREATE TABLE [HIDDEN_username].g_owned_data
(

    `Game` Nullable(String),

    `Genre` Nullable(String),

    `Release Date` Nullable(Date),

    `Developer` Nullable(String),

    `Publisher` Nullable(String),

    `Price` Nullable(Float32),

    `Buyers` Nullable(String),

    `Date Bought` Nullable(Date),

    `DM Creation` Nullable(DateTime)
)
ENGINE = PostgreSQL('172.30.58.112:5432',
 '[HIDDEN_username]_db',
 'g_owned_data',
 '[HIDDEN_username]',
 '[HIDDEN]',
 'project_dm');