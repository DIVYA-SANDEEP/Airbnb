import pandas as pd
import pymongo
import streamlit as st
import plotly.express as px
from streamlit_option_menu import option_menu
from PIL import Image
from bson.decimal128 import Decimal128


# Creating Connection with MongoDB and retrieving data
from pymongo.mongo_client import MongoClient
client = MongoClient("mongodb+srv://divya:1234@cluster0.afrzrns.mongodb.net/?retryWrites=true&w=majority")
database = client.Airbnb
collection = database.Airbnb_Data

# Reading the Cleaned DataFrame
df =pd.read_csv("airbnb_data.csv")

def extract_countries():
    pipeline = [
        {"$group": {"_id": "$address.country"}},
        {"$sort": {"_id": 1}}
    ]
    countries = [doc['_id'] for doc in collection.aggregate(pipeline)]
    return countries

def list_property():
    pipeline = [
        {"$group": {"_id": "$property_type"}},
        {"$sort": {"_id": 1}}
    ]
    properties = [doc['_id'] for doc in collection.aggregate(pipeline)]
    return properties

def amenities():
    pipeline = [
        {"$unwind": "$amenities"},
        {"$group": {"_id": "$amenities"}},
        {"$sort": {"_id": 1}}
    ]
    result = [doc['_id'] for doc in collection.aggregate(pipeline)]
    return result

def max_nights(days, col, country, pt):
    pipeline = [
        {"$match": {"minimum_nights": str(days), "address.country": country, "property_type": pt}},
        {"$project": {
            "_id": 0,
            "name": 1,
            "property_type": 1,
            "room_type": 1,
            "price": 1,
            "country": "$address.country",
            "review_scores_value": {"$ifNull": ["$review_scores.review_scores_value", "No Rating"]},
            "amenities": 1
        }}
    ]
    result = list(col.aggregate(pipeline))
    return result

def amen_based(col, selected_amenity, country, pt):
    pipeline = [
        {"$match": {"amenities": selected_amenity, "address.country": country, "property_type": pt}},
        {"$project": {
            "_id": 0,
            "name": 1,
            "property_type": 1,
            "room_type": 1,
            "price": 1,
            "country": "$address.country",
            "review_scores_value": {"$ifNull": ["$review_scores.review_scores_value", "No Rating"]},
            "amenities": 1
        }}
    ]
    result = list(col.aggregate(pipeline))
    return result

def room_list(country, collection):
    pipeline = [
        {"$match": {"address.country": country}},
        {"$group": {"_id": "$name"}},
        {"$sort": {"_id": 1}}
    ]
    rooms = [doc['_id'] for doc in collection.aggregate(pipeline)]
    return rooms

def room_info(col, selected_room, country):
    pipeline = [
        {"$match": {"name": selected_room, "address.country": country}},
        {"$project": {
            "_id": 0,
            "name": 1,
            "property_type": 1,
            "room_type": 1,
            "price": 1,
            "bedrooms": 1,
            "beds": 1,
            "bed_type": 1,
            "extra_people": 1,
            "guests_included": 1
        }}
    ]
    result = list(col.aggregate(pipeline))
    return result

def days(user_data, col, country, pt):
    pipeline = [
        {"$match": {
            "$or": [
                {"availability.availability_30": user_data},
                {"availability.availability_60": user_data},
                {"availability.availability_90": user_data},
                {"availability.availability_365": user_data}
            ],
            "address.country": country,
            "property_type": pt
        }},
        {"$project": {
            "_id": 0,
            "name": 1,
            "property_type": 1,
            "room_type": 1,
            "price": 1,
            "country": "$address.country",
            "review_scores_value": {"$ifNull": ["$review_scores.review_scores_value", "No Rating"]},
            "amenities": 1
        }}
    ]
    result = list(col.aggregate(pipeline))
    return result

def room_availability_by_country():
    pipeline = [
        {"$group": {
            "_id": {
                "country": "$address.country",
                "room_type": "$room_type"
            },
            "total_rooms": {"$sum": 1}
        }}
    ]
    availability_data = [doc for doc in collection.aggregate(pipeline)]
    return availability_data

def location(country, collection):
    pipeline = [
        {"$match": {"address.country": country}},
        {"$group": {
            "_id": "$property_type",
            "count": {"$sum": 1},
            "avg_price": {"$avg": "$price"},
            "max_price": {"$max": "$price"},
            "min_price": {"$min": "$price"},
            "avg_review_score": {"$avg": {"$ifNull": ["$review_scores.review_scores_value", 0]}}
        }},
        {"$project": {
            "_id": 0,
            "property_type": "$_id",
            "count": 1,
            "avg_price": 1,
            "max_price": 1,
            "min_price": 1,
            "avg_review_score": {"$round": ["$avg_review_score", 2]},
            "country": 1
        }}
    ]
    result = list(collection.aggregate(pipeline))
    return result

def group_property_types(country):
    pipeline = [
        {"$match": {"address.country": country}},  
        {"$group": {"_id": "$property_type", "count": {"$sum": 1}}}]
    result = list(collection.aggregate(pipeline))   
    return result

def top_10_prop(country):
    pipeline = [
    {"$match": {"address.country": country}},  
    {"$group": {"_id": "$property_type", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 5}]  
    result = list(collection.aggregate(pipeline))
    return result

def price(country):
    pipeline=[
    {"$match":{"address.country": country}},
    {"$project":{"_id":0,"name":1,"room_type":1,"property_type":1,"price":1,"cleaning_fee":1,"security_deposit":1}},
    {"$addFields": {"Total": {"$sum": ["$price", "$cleaning_fee", "$security_deposit"]}}}]
    result = list(collection.aggregate(pipeline))
    return result


def top_host(country):
    pipeline = [
        {"$match": {"address.country": country}},
        {"$group": {"_id": "$host.host_name", "host_listings_count": {"$sum": "$host.host_total_listings_count"}}},
        {"$sort": {"host_listings_count": -1}},
        {"$limit": 10}]
    result = list(collection.aggregate(pipeline))
    return result

icon = Image.open("Home1.png")
st.set_page_config(page_title= "Airbnb Data Visualization",
                   page_icon= icon,
                   layout= "wide",
                   initial_sidebar_state= "expanded",
                   )



col1, col2, col3 = st.columns([1,1,1])
col2.image("Home.png", width=150,use_column_width='always')

choropleth_data = df[['Country', 'Latitude', 'Longitude']]
choropleth_data.dropna(subset=['Country', 'Latitude', 'Longitude'], inplace=True)
    
selected = option_menu(None, ["Home","Properties","Analysis"],
                       icons=["house","building","search"],
                       default_index=0,
                       orientation="horizontal",
                       styles={"nav-link": {"font-size": "15px", "text-align": "centre", "margin": "-2px", "--hover-color": "Red"},
                               "icon": {"font-size": "35px"},
                               "container" : {"max-width": "5000px"},
                               "nav-link-selected": {"background-color": "Red"}})
if selected == "Home": 
    st.subheader(":red[***About Project:***]",divider="red")
    st.write("#### 🔹 This project focuses on analyzing Airbnb data retrieved from MongoDB Atlas, emphasizing data cleaning, exploratory analysis, and geospatial visualization to provide insights into listing distribution and pricing trends across various locations.") 
    st.write("#### 🔹 Through MongoDB integration and Streamlit web application development, the project aims to deliver an interactive platform for users to explore and understand the Airbnb dataset comprehensively.")

    st.subheader(" :red[***Technologies Used :***]",divider="red")
    st.write("#### 🔹Python")
    st.write("#### 🔹Pandas")
    st.write("#### 🔹MongoDB")
    st.write("#### 🔹Plotly")
    st.write("#### 🔹Streamlit")

    st.subheader(" :red[***Dashboard Link:***]",divider="red")
    st.link_button("PowerBI Dashboard","https://app.powerbi.com/groups/me/reports/9af32082-f478-4c8c-901c-acb476c1609f?experience=power-bi?publish=yes")

if selected == "Properties":   
    
    tab1, tab2 = st.tabs(["***Property Type and count***", "***Property Detail***"])   
    
    with tab1:
        col1,col2,col3=st.columns(3)
    with col1:
        countries = extract_countries()
        selected_country = st.selectbox("Select a country", countries, key='country_selectbox')
        property_type=list_property()
        property_types = group_property_types(selected_country)
    
        if property_types:
             df = pd.DataFrame(property_types)
             df.index=df.index+1
             df.columns = ["Property Type", "Count"] 
             st.dataframe(df)

    with tab2:
        col1,col2,col3,col4=st.columns(4)
        with col1:
            list1=st.selectbox("Key Features",["Number of Nights","Availability of Days"])
        
        if list1=="Number of Nights":    
            with col1:
                num_nights=st.slider("Number Of Nights",min_value=1,max_value=50)
                
            with col2:
                countries_list = extract_countries()
                selected_country = st.selectbox("Select a country", countries_list)
            with col3:
                property_list=list_property()
                selected_property=st.selectbox("Select a Property Type",property_list)
                    
            night_data = max_nights(num_nights, collection,selected_country,selected_property)
            nightdf = pd.DataFrame(night_data)
            nightdf.index=nightdf.index+1  
            st.dataframe(nightdf)
        if list1=="Availability of Days":
            with col1:
                day_count=st.selectbox("Number of days",["30","60","90","365"])
                
            with col2:
                countries_list = extract_countries()
                selected_country = st.selectbox("Select a country", countries_list)
            with col3:
                property_list=list_property()
                selected_property=st.selectbox("Select a Property Type",property_list)
           
            day_count_int=int(day_count)
            days_data = days(day_count_int, collection,selected_country,selected_property)
            daysdf = pd.DataFrame(days_data)
            daysdf.index=daysdf.index+1
            if not daysdf.empty:
                st.dataframe(daysdf)
            else:
                st.error("Unable to find a match")
                st.warning("Please try with a different property type or country")

if selected == "Analysis":
    list1=st.selectbox("Key Features",["Price Analysis","Top 5 Properties","Availability Analysis"])
    if list1 =="Top 5 Properties":
        col1,col2,col3=st.columns(3)
        with col1:
            countries_list = extract_countries()
            selected_country = st.selectbox("Select a country", countries_list)
            property_type=list_property()
    
        property_types = top_10_prop(selected_country)
        if property_types:
            df = pd.DataFrame(property_types)
            df.index=df.index+1
            df.columns = ["Property Type", "Count"]  

            fig = px.bar(df,title="Top 5 properties in each Countries", x="Property Type", y="Count",color="Property Type")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No data available for the selected country.") 
        
    if list1=="Price Analysis":
            countries_list = extract_countries()
            selected_country = st.selectbox("Select a country", countries_list)
            if selected_country:  
                price_analysis=price(selected_country)
                price_analysis = [{k: float(str(v)) if isinstance(v, Decimal128) else v for k, v in row.items()} for row in price_analysis]
                df=pd.DataFrame(price_analysis)
                df = df.dropna()
                df.columns=["Name","Room_Type","Property_Type","Price","Security Deposit","Total","Cleaning Fees"]
                st.dataframe(df)

    if list1=="Availability Analysis":
       availability_data = room_availability_by_country()

       df1 = pd.DataFrame(availability_data)

       selected_country = st.selectbox("Select a country", sorted(df['Country'].unique()))
       filtered_df = df[df['Country'] == selected_country]
       selected_room_type = st.multiselect("Select room types", filtered_df['Room_type'].unique())
       filtered_df = filtered_df[filtered_df['Room_type'].isin(selected_room_type)]

       columns_to_display = st.multiselect("Select columns to display", filtered_df.columns)
       if not columns_to_display:
         st.warning("Please select columns to display.")
       else:
         st.write("Room Availability by Country and Room Type:")
         st.dataframe(filtered_df[columns_to_display])
        
