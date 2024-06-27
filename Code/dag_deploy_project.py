from airflow.decorators import dag
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator


DAG_ID = 'deploy_project_[HIDDEN_username]'

@dag(
  dag_id=DAG_ID,
  start_date=datetime(2024,3,22),
  schedule=None,
  catchup=False,
  is_paused_upon_creation=False,
  tags = ['[HIDDEN_username] - Project'],
)
def deploy_database():
  create_schemas_task = PostgresOperator(
    task_id="create_schemas",
    postgres_conn_id="[HIDDEN_username]__db",
    sql="""CREATE SCHEMA IF NOT EXISTS project_stg;
    CREATE SCHEMA IF NOT EXISTS project_ods; 
    CREATE SCHEMA IF NOT EXISTS project_dds;
    CREATE SCHEMA IF NOT EXISTS project_dm;
    """,
    )
  create_tables_task = PostgresOperator(
    task_id="create_tables",
    postgres_conn_id="[HIDDEN_username]__db",
    sql="""DROP VIEW IF EXISTS project_dm.user_g_data; 
    DROP VIEW IF EXISTS project_dm.g_owned_data;
    
    DELETE FROM tech.last_update WHERE table_name='project_stg.wishlist';
    DELETE FROM tech.last_update WHERE table_name='project_stg.client';
    DELETE FROM tech.last_update WHERE table_name='project_stg.game'; 
    DELETE FROM tech.last_update WHERE table_name='project_stg.sales';

    DROP TABLE IF EXISTS project_stg.client;
    CREATE TABLE project_stg.client (
        user_id int8 NULL,
        full_name varchar(256) NULL,
        nickname varchar(256) NULL,
        gender bpchar NULL,
        region varchar(50) NULL,
        birthday date NULL,
        age int4 NULL,
        email varchar(256) NULL,
        password varchar(50) NULL,
        phone_number varchar(50) NULL,
        games_owned int4 NULL,
        last_update timestamp NULL,
        deleted_flg BIT NULL
    );

    DROP TABLE IF EXISTS project_stg.game;
    CREATE TABLE project_stg.game (
        game_id int8 NULL,
        title varchar(256) NULL,
        game_description text NULL,
        genre_name varchar(50) NULL,
        release_date date NULL,
        price float4 NULL,
        publisher varchar(256) NULL,
        developer varchar(256) NULL,
        reviews varchar(50) NULL,
        last_update timestamp NULL,
        deleted_flg BIT NULL
    );

    DROP TABLE IF EXISTS project_stg.sales;
    CREATE TABLE project_stg.sales (
        user_id int8 NULL,
        game_id int8 NULL,
        date_bought timestamp NULL,
        payment_method varchar(50) NULL,
        started_once bool NULL,
        played_hours float4 NULL,
        refunded bool NULL,
        date_refunded timestamp NULL
    );

    DROP TABLE IF EXISTS project_stg.wishlist;
    CREATE TABLE project_stg.wishlist (
        user_id int8 NULL,
        game_id int8 NULL,
        date_added timestamp NULL,
        deleted_flg BIT NULL
    );


    DROP TABLE IF EXISTS project_ods.client;
    CREATE TABLE project_ods.client (
    	user_id int8 NULL,
    	full_name varchar(256) NULL,
    	nickname varchar(256) NULL,
    	gender bpchar NULL,
    	region varchar(50) NULL,
    	birthday date NULL,
    	age int4 NULL,
    	email varchar(256) NULL,
    	password varchar(50) NULL,
    	phone_number varchar(50) NULL,
    	games_owned int4 NULL,
        last_update timestamp NULL,
    	deleted_flg BIT NULL,
    	upload_to_ods_date timestamp NULL
    );
    
    DROP TABLE IF EXISTS project_ods.game;
    CREATE TABLE project_ods.game (
    	game_id int8 NULL,
    	title varchar(256) NULL,
    	game_description text NULL,
    	genre_name varchar(50) NULL,
    	release_date date NULL,
    	price float4 NULL,
    	publisher varchar(256) NULL,
    	developer varchar(256) NULL,
    	reviews varchar(50) NULL,
        last_update timestamp NULL,
    	deleted_flg BIT NULL,
    	upload_to_ods_date timestamp NULL
    );
    
    DROP TABLE IF EXISTS project_ods.sales;
    CREATE TABLE project_ods.sales (
    	user_id int8 NULL,
    	game_id int8 NULL,
    	date_bought timestamp NULL,
    	payment_method varchar(50) NULL,
    	started_once bool NULL,
    	played_hours float4 NULL,
    	refunded bool NULL,
    	date_refunded timestamp NULL,
    	upload_to_ods_date timestamp NULL
    );
    
    DROP TABLE IF EXISTS project_ods.wishlist;
    CREATE TABLE project_ods.wishlist (
    	user_id int8 NULL,
    	game_id int8 NULL,
    	date_added timestamp NULL,
    	deleted_flg BIT NULL,
    	upload_to_ods_date timestamp NULL
    );


    DROP TABLE IF EXISTS project_dds.client;
    CREATE TABLE project_dds.client (
        user_id int8 NOT NULL,
        full_name varchar(256) NOT NULL,
        gender bpchar NULL,
        region_id int8 NULL,
        birthday date NULL,
        age int4 NULL,
        login_id int8 NOT NULL,
        is_deleted boolean NOT NULL,
        load_id int8 DEFAULT 1 NOT NULL,
        load_date timestamp DEFAULT now() NOT NULL  
    );

    DROP TABLE IF EXISTS project_dds.credentials;
    CREATE TABLE project_dds.credentials (
        login_id int8 NOT NULL,
        nickname varchar(256) NOT NULL,
        email_address varchar(256) NOT NULL,
        hashed_password varchar(50) NOT NULL,
        mobile_num varchar(50) NULL,
        is_deleted boolean NOT NULL,
        load_id int8 DEFAULT 1 NOT NULL,
        load_date timestamp DEFAULT now() NOT NULL  
    );

    DROP TABLE IF EXISTS project_dds.region;
    CREATE TABLE project_dds.region (
        region_id serial NOT NULL,
        region_name varchar(256) NOT NULL,
        load_id int8 DEFAULT 1 NOT NULL,
        load_date timestamp DEFAULT now() NOT NULL  
    );

    DROP TABLE IF EXISTS project_dds.game;
    CREATE TABLE project_dds.game (
        game_id int8 NOT NULL,
        title varchar(256) NOT NULL,
        description text NOT NULL,
        genre_id int8 NOT NULL,
        release_date date NOT NULL,
        price float4 NOT NULL,
        developer_id int8 NOT NULL,
        publisher_id int8 NOT NULL,
        reviews_id int4 NOT NULL,
        is_deleted boolean DEFAULT 'false' NOT NULL,
        load_id int8 DEFAULT 1 NOT NULL,
        load_date timestamp DEFAULT now() NOT NULL  
    );

    DROP TABLE IF EXISTS project_dds.genre;
    CREATE TABLE project_dds.genre (
        genre_id serial NOT NULL,
        genre_name varchar(128) NOT NULL,
        load_id int8 DEFAULT 1 NOT NULL,
        load_date timestamp DEFAULT now() NOT NULL  
    );

    DROP TABLE IF EXISTS project_dds.publisher;
    CREATE TABLE project_dds.publisher (
        publisher_id serial NOT NULL,
        publisher_name varchar(256) NOT NULL,
        is_deleted boolean DEFAULT 'false' NOT NULL,
        load_id int8 DEFAULT 1 NOT NULL,
        load_date timestamp DEFAULT now() NOT NULL 
    );

    DROP TABLE IF EXISTS project_dds.developer;
    CREATE TABLE project_dds.developer (
        developer_id serial NOT NULL,
        developer_name varchar(256) NOT NULL,
        is_deleted boolean DEFAULT 'false' NOT NULL,
        load_id int8 DEFAULT 1 NOT NULL,
        load_date timestamp DEFAULT now() NOT NULL 
    );

    DROP TABLE IF EXISTS project_dds.reviews;
    CREATE TABLE project_dds.reviews (
        reviews_id serial NOT NULL,
        reviews_detail varchar(128) NOT NULL,
        load_id int8 DEFAULT 1 NOT NULL,
        load_date timestamp DEFAULT now() NOT NULL 
    );

    DROP TABLE IF EXISTS project_dds.sales;
    CREATE TABLE project_dds.sales (
        user_id int8 NOT NULL,
        game_id int8 NOT NULL,
        date_bought timestamp NOT NULL,
        pm_id int4 NOT NULL,
        load_id int8 DEFAULT 1 NOT NULL,
        load_date timestamp DEFAULT now() NOT NULL  
    );

    DROP TABLE IF EXISTS project_dds.wishlist;
    CREATE TABLE project_dds.wishlist (
        user_id int8 NOT NULL,
        game_id int8 NOT NULL,
        date_added timestamp NOT NULL,
        is_deleted boolean DEFAULT 'false' NOT NULL,
        load_id int8 DEFAULT 1 NOT NULL,
        load_date timestamp DEFAULT now() NOT NULL
    );

    DROP TABLE IF EXISTS project_dds.refunds;
    CREATE TABLE project_dds.refunds (
        user_id int8 NOT NULL,
        game_id int8 NOT NULL,
        date_bought timestamp NOT NULL,
        date_refunded timestamp NOT NULL,
        load_id int8 DEFAULT 1 NOT NULL,
        load_date timestamp DEFAULT now() NOT NULL  
    );

    DROP TABLE IF EXISTS project_dds.payment_method;
    CREATE TABLE project_dds.payment_method (
        pm_id serial NOT NULL,
        method_details varchar(128) NOT NULL,
        load_id int8 DEFAULT 1 NOT NULL,
        load_date timestamp DEFAULT now() NOT NULL  
    );

    DROP TABLE IF EXISTS project_dds.user_x_game;
    CREATE TABLE project_dds.user_x_game (
        user_id int8 NOT NULL,
        game_id int8 NOT NULL,
        started_once boolean NOT NULL,
        played_hours float4 NOT NULL,
        is_deleted boolean DEFAULT 'false' NOT NULL,
        load_id int8 DEFAULT 1 NOT NULL,
        load_date timestamp DEFAULT now() NOT NULL
    )
    """,
    )
  create_funcs_task = PostgresOperator(
    task_id="create_funcs",
    postgres_conn_id="[HIDDEN_username]__db",
    sql="""DROP PROCEDURE IF EXISTS project_stg.sales_load();
    CREATE OR REPLACE PROCEDURE project_stg.sales_load()
    AS $$
    DECLARE  
        last_update_dt timestamp;
    BEGIN 
        last_update_dt = COALESCE(
            (SELECT max(update_dt)
                FROM tech.last_update
                WHERE table_name = 'project_stg.sales'), 
            '1900-01-01'::date );
        DELETE FROM project_stg.sales;
        INSERT INTO project_stg.sales
                    (user_id,
                    game_id,
                    date_bought,
                    payment_method,
                    started_once,
                    played_hours,
                    refunded,
                    date_refunded)
        SELECT  
                    "User_ID",
                    "Game_ID",
                    "Date_Bought"::timestamp,
                    "Payment_Method",
                    "Started_Once",
                    "Played_Hours"::numeric,
                    "Refunded",
                    CASE 	
                        WHEN "Date_Refunded" != '' THEN "Date_Refunded"::timestamp
                        ELSE NULL
                    END
        FROM project_src_data."Sales" 
        WHERE  
            CASE  
                WHEN "Date_Refunded" != '' THEN "Date_Refunded"::timestamp > last_update_dt
                ELSE "Date_Bought"::timestamp > last_update_dt
            END;
        INSERT INTO tech.last_update
                (table_name,
                update_dt)
            VALUES 
                ('project_stg.sales',
                now());
    END;
    $$ LANGUAGE plpgsql;


    DROP PROCEDURE IF EXISTS project_stg.game_load();
    CREATE OR REPLACE PROCEDURE project_stg.game_load()
    AS $$
    DECLARE 
        last_update_dt timestamp;
    BEGIN
        last_update_dt = COALESCE(
            (SELECT max(update_dt)
                FROM tech.last_update
                WHERE table_name = 'project_stg.game'), 
            '1900-01-01'::date );
        DELETE FROM project_stg.game;
        INSERT INTO project_stg.game
                    (game_id,
                    title,
                    game_description,
                    genre_name,
                    release_date,
                    price,
                    publisher,
                    developer,
                    reviews,
                    last_update,
                    deleted_flg)
        SELECT  
                    game_id,
                    title,
                    game_description,
                    genre_name,
                    to_char(to_date(release_date, 'DD-MM-YYYY'), 'YYYY-MM-DD')::date,
                    price,
                    publisher,
                    developer,
                    reviews,
                    last_update::timestamp,
                    '0'
        FROM project_src_data."Games"
        WHERE last_update::timestamp > last_update_dt; 
        INSERT INTO project_stg.game
                    (game_id,
                    title,
                    game_description,
                    genre_name,
                    release_date,
                    price,
                    publisher,
                    developer,
                    reviews,
                    last_update,
                    deleted_flg)
        SELECT  
                    g.game_id,
                    g.title,
                    g.game_description,
                    g.genre_name,
                    g.release_date,
                    g.price,
                    g.publisher,
                    g.developer,
                    g.reviews,
                    now(),
                    '1' 
        FROM project_ods.game g
        LEFT JOIN project_src_data."Games" s ON g.game_id = s.game_id
        WHERE s.game_id IS NULL;
        INSERT INTO tech.last_update
                (table_name,
                update_dt)
            VALUES 
                ('project_stg.game',
                now());
    END;
    $$ LANGUAGE plpgsql;


    DROP PROCEDURE IF EXISTS project_stg.client_load();
    CREATE OR REPLACE PROCEDURE project_stg.client_load()
    AS $$
    DECLARE 
            last_update_dt timestamp;
    BEGIN 
            last_update_dt = coalesce (
            (SELECT max(update_dt)
                FROM tech.last_update
                WHERE table_name = 'project_stg.client'),
            '1900-01-01'::date);
        DELETE FROM project_stg.client;
        INSERT INTO project_stg.client
                        (user_id,
                        full_name,
                        nickname,
                        gender,
                        region,
                        birthday,
                        age,
                        email,
                        password,
                        phone_number,
                        games_owned,
                        last_update,
                        deleted_flg 
                        )
        SELECT   
                        "ID",
                        "Full_Name",
                        "Nickname",
                        "Sex",
                        "Region",
                        CASE 	
                            WHEN  "Birthday" != '' THEN  "Birthday"::date
                            ELSE NULL
                        END,
                        CASE 	
                            WHEN "Age" IS NOT NULL THEN "Age"::integer
                            ELSE NULL
                        END,
                        "Email",
                        "Password",
                        "Phone_Number",
                        "Games_Owned",
                        "Last_Update"::timestamp,
                        '0'
        FROM project_src_data."Users"
        WHERE "Last_Update"::timestamp > last_update_dt; 
        INSERT INTO project_stg.client
                        (user_id,
                        full_name,
                        nickname,
                        gender,
                        region,
                        birthday,
                        age,
                        email,
                        password,
                        phone_number,
                        games_owned,
                        last_update,
                        deleted_flg
                        )
        SELECT  
                        c.user_id,
                        c.full_name,
                        c.nickname,
                        c.gender,
                        c.region,
                        c.birthday,
                        c.age,
                        c.email,
                        c.password,
                        c.phone_number,
                        c.games_owned,
                        now(),
                        '1' 
        FROM project_ods.client c
        LEFT JOIN project_src_data."Users" s ON c.user_id = s."ID"
        WHERE s."ID" IS NULL;
        INSERT INTO tech.last_update
                (table_name,
                update_dt)
            VALUES 
                ('project_stg.client',
                now());
    END;
    $$ LANGUAGE plpgsql;

    DROP PROCEDURE IF EXISTS project_stg.wishlist_load();
    CREATE OR REPLACE PROCEDURE project_stg.wishlist_load()
    AS $$
    DECLARE 
            last_update_dt timestamp;
    BEGIN
            last_update_dt = COALESCE(
            (SELECT max(update_dt)
                FROM tech.last_update
                WHERE table_name = 'project_stg.wishlist'),
            '1900-01-01'::date);
        DELETE FROM project_stg.wishlist;
        INSERT INTO project_stg.wishlist
                        (user_id,
                        game_id,
                        date_added,
                        deleted_flg
                        )
        SELECT  
                        "User_ID",
                        "Game_ID",
                        "Date_Added"::timestamp,
                        '0'
        FROM project_src_data."Wishlists"
        WHERE "Date_Added"::timestamp > last_update_dt; 
        INSERT INTO project_stg.wishlist
                        (user_id,
                        game_id,
                        date_added,
                        deleted_flg
                        )
        SELECT          w.user_id,
                        w.game_id,
                        now(),
                        '1'
        FROM project_ods.wishlist w
        LEFT JOIN project_src_data."Wishlists" s ON w.user_id = s."User_ID" AND w.game_id = s."Game_ID"
        WHERE  s."User_ID" IS NULL AND s."Game_ID" IS NULL;			
        INSERT INTO tech.last_update
                (table_name,
                update_dt)
            VALUES 
                ('project_stg.wishlist',
                now());
    END;
    $$ LANGUAGE plpgsql;

    DROP SEQUENCE IF EXISTS project_dds.credentials_load_id;
    CREATE SEQUENCE project_dds.credentials_load_id;
    DROP FUNCTION IF EXISTS project_dds.insert_credentials ();
    CREATE OR REPLACE FUNCTION project_dds.insert_credentials ()
        RETURNS INTEGER
        LANGUAGE plpgsql
    AS $function$
        BEGIN
            PERFORM
                NEXTVAL('project_dds.credentials_load_id');
            INSERT INTO project_dds.credentials
            (
                login_id,
                nickname,
                email_address,
                hashed_password,
                mobile_num,
                is_deleted,
                load_id
            )
            SELECT
                user_id,
                nickname,
                email,
                password,
                phone_number,
                CASE 
                    WHEN deleted_flg::integer = 0 then FALSE 
                    ELSE TRUE
                END AS is_deleted,
                CURRVAL('project_dds.credentials_load_id') AS load_id
            FROM 
                project_ods.client;
            RETURN 0;
        END;
    $function$;

    DROP SEQUENCE IF EXISTS project_dds.region_load_id;
    CREATE SEQUENCE project_dds.region_load_id;
    DROP FUNCTION IF EXISTS project_dds.insert_region ();
    CREATE OR REPLACE FUNCTION project_dds.insert_region ()
        RETURNS INTEGER
        LANGUAGE plpgsql
    AS $function$
        BEGIN
            PERFORM
                NEXTVAL('project_dds.region_load_id');
            INSERT INTO project_dds.region
            (
                region_name, 
                load_id
            )
            SELECT
                DISTINCT region,
                CURRVAL('project_dds.region_load_id') AS load_id
            FROM 
                project_ods.client
            WHERE region != '';
            RETURN 0;
        END;
    $function$;

    DROP SEQUENCE IF EXISTS project_dds.client_load_id;
    CREATE SEQUENCE project_dds.client_load_id;
    DROP FUNCTION IF EXISTS project_dds.insert_client ();
    CREATE OR REPLACE FUNCTION project_dds.insert_client ()
        RETURNS INTEGER
        LANGUAGE plpgsql
    AS $function$
        BEGIN
            PERFORM
                NEXTVAL('project_dds.client_load_id');
            INSERT INTO project_dds.client 
            (
                user_id,
                full_name,
                gender,
                region_id,
                birthday,
                age,
                login_id,
                is_deleted,
                load_id
            )
            SELECT
                cl.user_id,
                cl.full_name,
                cl.gender,
                r.region_id,
                cl.birthday,
                CASE 
                    WHEN cl.birthday IS NOT NULL THEN EXTRACT(years FROM age(now(), cl.birthday::timestamp))
                    ELSE NULL
                END as age,
                cr.login_id,
                CASE 
                    WHEN cl.deleted_flg::integer = 0 then FALSE 
                    ELSE TRUE
                END as is_deleted,
                CURRVAL('project_dds.client_load_id') AS load_id
            FROM
                project_ods.client cl 
                JOIN project_dds.credentials cr ON cl.nickname = cr.nickname
                LEFT JOIN project_dds.region r on cl.region = r.region_name;
            RETURN 0;
        END;
    $function$;

    DROP SEQUENCE IF EXISTS project_dds.publisher_load_id;
    CREATE SEQUENCE project_dds.publisher_load_id;
    DROP FUNCTION IF EXISTS project_dds.insert_publisher ();
    CREATE OR REPLACE FUNCTION project_dds.insert_publisher ()
        RETURNS INTEGER
        LANGUAGE plpgsql
    AS $function$
        BEGIN
            PERFORM
                NEXTVAL('project_dds.publisher_load_id');
            INSERT INTO project_dds.publisher
            (
                publisher_name,
                is_deleted,
                load_id
            )
            SELECT
                DISTINCT g.publisher,
                CASE 
                    WHEN (SELECT count(*) FROM project_ods.game WHERE publisher = g.publisher) = 0 THEN TRUE
                    ELSE FALSE
                END AS is_deleted,
                CURRVAL('project_dds.publisher_load_id') AS load_id
            FROM 
                project_ods.game g;
            RETURN 0;
        END;
    $function$;

    DROP SEQUENCE IF EXISTS project_dds.developer_load_id;
    CREATE SEQUENCE project_dds.developer_load_id;
    DROP FUNCTION IF EXISTS project_dds.insert_developer ();
    CREATE OR REPLACE FUNCTION project_dds.insert_developer ()
        RETURNS INTEGER
        LANGUAGE plpgsql
    AS $function$
        BEGIN
            PERFORM
                NEXTVAL('project_dds.developer_load_id');
            INSERT INTO project_dds.developer
            (
                developer_name,
                is_deleted,
                load_id
            )
            SELECT
                DISTINCT g.developer,
                CASE 
                    WHEN (SELECT count(*) FROM project_ods.game WHERE developer = g.developer) = 0 THEN TRUE
                    ELSE FALSE
                END AS is_deleted,
                CURRVAL('project_dds.developer_load_id') AS load_id
            FROM 
                project_ods.game g;
            RETURN 0;
        END;
    $function$;

    DROP SEQUENCE IF EXISTS project_dds.genre_load_id;
    CREATE SEQUENCE project_dds.genre_load_id;
    DROP FUNCTION IF EXISTS project_dds.insert_genre ();
    CREATE OR REPLACE FUNCTION project_dds.insert_genre ()
        RETURNS INTEGER
        LANGUAGE plpgsql
    AS $function$
        BEGIN
            PERFORM
                NEXTVAL('project_dds.genre_load_id');
            INSERT INTO project_dds.genre
            (
                genre_name,
                load_id
            )
            SELECT
                DISTINCT genre_name,
                CURRVAL('project_dds.genre_load_id') AS load_id
            FROM 
                project_ods.game;
            RETURN 0;
        END;
    $function$;

    DROP SEQUENCE IF EXISTS project_dds.reviews_load_id;
    CREATE SEQUENCE project_dds.reviews_load_id;
    DROP FUNCTION IF EXISTS project_dds.insert_reviews ();
    CREATE OR REPLACE FUNCTION project_dds.insert_reviews ()
        RETURNS INTEGER
        LANGUAGE plpgsql
    AS $function$
        BEGIN
            PERFORM
                NEXTVAL('project_dds.reviews_load_id');
            INSERT INTO project_dds.reviews
            (
                reviews_detail,
                load_id
            )
            SELECT
                DISTINCT reviews,
                CURRVAL('project_dds.reviews_load_id') AS load_id
            FROM 
                project_ods.game;
            RETURN 0;
        END;
    $function$;

    DROP SEQUENCE IF EXISTS project_dds.game_load_id;
    CREATE SEQUENCE project_dds.game_load_id;
    DROP FUNCTION IF EXISTS project_dds.insert_game ();
    CREATE OR REPLACE FUNCTION project_dds.insert_game ()
        RETURNS INTEGER
        LANGUAGE plpgsql
    AS $function$
        BEGIN
            PERFORM
                NEXTVAL('project_dds.game_load_id');
            INSERT INTO project_dds.game
            (
                game_id,
                title,
                description,
                genre_id,
                release_date,
                price,
                developer_id,
                publisher_id,
                reviews_id,
                is_deleted,
                load_id
            )
            SELECT
                g.game_id,
                g.title,
                g.game_description,
                gn.genre_id,
                g.release_date,
                g.price,
                d.developer_id,
                p.publisher_id,
                r.reviews_id,
                CASE 
                    WHEN g.deleted_flg::integer = 0 then FALSE 
                    ELSE TRUE
                END as is_deleted,
                CURRVAL('project_dds.game_load_id') AS load_id
            FROM
                project_ods.game g 
                JOIN project_dds.publisher p ON g.publisher = p.publisher_name
                JOIN project_dds.developer d on g.developer = d.developer_name
                JOIN project_dds.genre gn on g.genre_name = gn.genre_name
                JOIN project_dds.reviews r on g.reviews = r.reviews_detail;
            RETURN 0;
        END;
    $function$;

    DROP SEQUENCE IF EXISTS project_dds.payment_method_load_id;
    CREATE SEQUENCE project_dds.payment_method_load_id;
    DROP FUNCTION IF EXISTS project_dds.insert_payment_method ();
    CREATE OR REPLACE FUNCTION project_dds.insert_payment_method ()
        RETURNS INTEGER
        LANGUAGE plpgsql
    AS $function$
        BEGIN
            PERFORM
                NEXTVAL('project_dds.payment_method_load_id');
            INSERT INTO project_dds.payment_method
            (
                method_details,
                load_id
            )
            SELECT
                DISTINCT payment_method,
                CURRVAL('project_dds.payment_method_load_id') AS load_id
            FROM 
                project_ods.sales;
            RETURN 0;
        END;
    $function$;

    DROP SEQUENCE IF EXISTS project_dds.sales_load_id;
    CREATE SEQUENCE project_dds.sales_load_id;
    DROP FUNCTION IF EXISTS project_dds.insert_sales ();
    CREATE OR REPLACE FUNCTION project_dds.insert_sales ()
        RETURNS INTEGER
        LANGUAGE plpgsql
    AS $function$
        BEGIN
            PERFORM
                NEXTVAL('project_dds.sales_load_id');
            INSERT INTO project_dds.sales
            (
                user_id,
                game_id,
                date_bought,
                pm_id,
                load_id
            )
            SELECT
                c.user_id,
                g.game_id,
                s.date_bought,
                p.pm_id,
                CURRVAL('project_dds.sales_load_id') AS load_id
            FROM
                project_ods.sales s 
                JOIN project_dds.client c ON s.user_id = c.user_id
                JOIN project_dds.game g on s.game_id = g.game_id
                JOIN project_dds.payment_method p on s.payment_method = p.method_details
                WHERE refunded = FALSE;
            RETURN 0;
        END;
    $function$;

    DROP SEQUENCE IF EXISTS project_dds.wishlist_load_id;
    CREATE SEQUENCE project_dds.wishlist_load_id;
    DROP FUNCTION IF EXISTS project_dds.insert_wishlist ();
    CREATE OR REPLACE FUNCTION project_dds.insert_wishlist ()
        RETURNS INTEGER
        LANGUAGE plpgsql
    AS $function$
        BEGIN
            PERFORM
                NEXTVAL('project_dds.wishlist_load_id');
            INSERT INTO project_dds.wishlist
            (
                user_id,
                game_id,
                date_added,
                is_deleted,
                load_id
            )
            SELECT
                c.user_id,
                g.game_id,
                w.date_added,
                CASE 
                    WHEN w.deleted_flg::integer = 0 then FALSE 
                    ELSE TRUE
                END as is_deleted,
                CURRVAL('project_dds.wishlist_load_id') AS load_id
            FROM
                project_ods.wishlist w
                JOIN project_dds.client c ON w.user_id = c.user_id
                JOIN project_dds.game g on w.game_id = g.game_id;
            RETURN 0;
        END;
    $function$;

    DROP SEQUENCE IF EXISTS project_dds.refunds_load_id;
    CREATE SEQUENCE project_dds.refunds_load_id;
    DROP FUNCTION IF EXISTS project_dds.insert_refunds ();
    CREATE OR REPLACE FUNCTION project_dds.insert_refunds ()
        RETURNS INTEGER
        LANGUAGE plpgsql
    AS $function$
        BEGIN
            PERFORM
                NEXTVAL('project_dds.refunds_load_id');
            INSERT INTO project_dds.refunds
            (
                user_id,
                game_id,
                date_bought,
                date_refunded,
                load_id
            )
            SELECT
                c.user_id,
                g.game_id,
                s.date_bought,
                s.date_refunded,
                CURRVAL('project_dds.refunds_load_id') AS load_id
            FROM
                project_ods.sales s 
                JOIN project_dds.client c ON s.user_id = c.user_id
                JOIN project_dds.game g on s.game_id = g.game_id
                WHERE refunded = TRUE;
            RETURN 0;
        END;
    $function$;

    DROP SEQUENCE IF EXISTS project_dds.user_x_game_load_id;
    CREATE SEQUENCE project_dds.user_x_game_load_id;
    DROP FUNCTION IF EXISTS project_dds.insert_user_x_game ();
    CREATE OR REPLACE FUNCTION project_dds.insert_user_x_game ()
        RETURNS INTEGER
        LANGUAGE plpgsql
    AS $function$
        BEGIN
            PERFORM
                NEXTVAL('project_dds.user_x_game_load_id');
            INSERT INTO project_dds.user_x_game
            (
                user_id,
                game_id,
                started_once,
                played_hours,
                is_deleted,
                load_id
            )
            SELECT
                c.user_id,
                g.game_id,
                s.started_once,
                s.played_hours,
                CASE
                    WHEN (SELECT count(*) FROM project_dds.refunds WHERE user_id = c.user_id AND game_id = g.game_id) = 0 THEN FALSE
                    ELSE TRUE
                END AS is_deleted,
                CURRVAL('project_dds.user_x_game_load_id') AS load_id
            FROM
                project_ods.sales s 
                JOIN project_dds.client c ON s.user_id = c.user_id
                JOIN project_dds.game g ON s.game_id = g.game_id;
            RETURN 0;
        END;
    $function$;

    CREATE OR REPLACE FUNCTION project_dm.insert_user_g_data()
        RETURNS integer
        LANGUAGE plpgsql
    AS $function$
        BEGIN 
            CREATE OR REPLACE VIEW project_dm.user_g_data AS 
                WITH user_game_and_nulls as(
                    SELECT 
                        uxg.user_id, 
                        uxg.game_id,
                        uxg.played_hours
                    FROM project_dds.client c
                    LEFT JOIN project_dds.user_x_game uxg ON c.user_id = uxg.user_id
                    WHERE uxg.is_deleted = FALSE
                    UNION
                    SELECT 
                        c.user_id, 
                        uxg.game_id,
                        uxg.played_hours 
                    FROM project_dds.client c
                    LEFT JOIN project_dds.user_x_game uxg ON uxg.user_id = c.user_id
                    WHERE uxg.user_id IS NULL
                ),
                ranked_games AS (
                    SELECT 
                        usg.user_id,
                        usg.game_id,
                        g.title,
                        usg.played_hours,
                        ROW_NUMBER() OVER (PARTITION BY usg.user_id ORDER BY usg.played_hours DESC) rn
                    FROM user_game_and_nulls usg
                    LEFT JOIN project_dds.game g ON usg.game_id = g.game_id
                ),
                games_owned AS (
                    SELECT
                        c.user_id,
                        count(s.user_id) AS gs_owned
                    FROM project_dds.sales s
                    RIGHT JOIN project_dds.client c ON c.user_id = s.user_id
                    GROUP BY 1
                ),
                games_wishlist AS (
                    SELECT
                        c.user_id,
                        count(w.user_id) AS gs_in_w
                    FROM project_dds.wishlist w
                    RIGHT JOIN project_dds.client c ON c.user_id = w.user_id
                    GROUP BY 1
                ),
                games_refunded AS (
                    SELECT
                        c.user_id,
                        count(r.user_id) AS gs_refunded
                    FROM project_dds.refunds r 
                    RIGHT JOIN project_dds.client c ON c.user_id = r.user_id
                    GROUP BY 1
                )
                SELECT 
                    c.full_name AS "Full Name", 
                    cr.nickname AS "Nickname", 
                    CASE 
                        WHEN c.gender = 'M' THEN 'Male'
                        ELSE 'Female'
                    END AS "Gender", 
                    CASE 
                        WHEN c.birthday IS NOT NULL THEN to_char(c.birthday, 'DD-MM-YYYY')
                        ELSE 'N/A'
                    END AS "Birthday",
                    CASE 
                        WHEN c.age IS NOT NULL THEN c.age::text
                        ELSE 'N/A'
                    END AS "Age",
                    CASE 
                        WHEN c.region_id IS NOT NULL THEN r.region_name
                        ELSE 'N/A'
                    END AS "Region",
                    cr.email_address AS "Email Address",
                    CASE 
                        WHEN COALESCE(TRIM(cr.mobile_num),NULL,'') != '' THEN cr.mobile_num 
                        ELSE 'N/A'
                    END AS "Phone Number",
                    CASE 
                        WHEN COALESCE(TRIM(rg.title),NULL,'') != '' AND rg.played_hours > 0 THEN rg.title
                        ELSE 'N/A'
                    END AS "Most Played Game",
                    gso.gs_owned AS "N. Games Owned",
                    gw.gs_in_w AS "N. in Wishlist",
                    rfn.gs_refunded AS "N. Games Refunded",
                    CASE 
                        WHEN sum(rg.played_hours) IS NOT NULL THEN sum(rg.played_hours)
                        ELSE 0
                    END AS "Total Playtime",
                    CAST(now() AS timestamp) AS "DM Creation"
                FROM project_dds.client c 
                JOIN project_dds.credentials cr ON c.login_id = cr.login_id 
                LEFT JOIN project_dds.region r ON r.region_id = c.region_id
                JOIN ranked_games rg ON rg.user_id = c.user_id 
                JOIN games_owned gso ON gso.user_id = c.user_id
                JOIN games_refunded rfn ON rfn.user_id = c.user_id
                JOIN games_wishlist gw ON gw.user_id = c.user_id
                WHERE rg.rn = 1
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
                ORDER BY c.full_name ASC;
            RETURN 0;
        END;
    $function$;

    CREATE OR REPLACE FUNCTION project_dm.insert_g_owned_data()
        RETURNS integer
        LANGUAGE plpgsql
    AS $function$
        BEGIN 
            DROP VIEW IF EXISTS project_dm.g_owned_data; 
            CREATE OR REPLACE VIEW project_dm.g_owned_data AS 
                WITH user_info AS (
                    SELECT c.user_id, cr.nickname, s.game_id, s.date_bought
                    FROM project_dds.client c
                    JOIN project_dds.sales s ON s.user_id = c.user_id
                    JOIN project_dds.credentials cr ON c.login_id = cr.login_id
                )
                SELECT 
                    g.title AS "Game",  
                    gn.genre_name AS "Genre",
                    g.release_date AS "Release Date", 
                    d.developer_name AS "Developer",
                    p.publisher_name AS "Publisher", 
                    g.price AS "Price",
                    u.nickname AS "Buyers",
                    CAST(u.date_bought AS date)  AS "Date Bought",
                    CAST(now() AS timestamp) AS "DM Creation"
                FROM project_dds.game g 
                JOIN project_dds.genre gn ON gn.genre_id = g.genre_id 
                JOIN project_dds.developer d ON d.developer_id = g.developer_id 
                JOIN project_dds.publisher p ON p.publisher_id = g.publisher_id
                JOIN user_info u ON u.game_id = g.game_id
                ORDER BY g.title ASC;
            RETURN 0;
        END;
    $function$;
    """
    )
    
  create_schemas_task >> create_tables_task >> create_funcs_task
    
init_dag = deploy_database()